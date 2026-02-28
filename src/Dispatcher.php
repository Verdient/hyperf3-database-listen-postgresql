<?php

declare(strict_types=1);

namespace Verdient\Hyperf3\Database\Listen\PostgreSQL;

use Generator;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\ProcessInterface;
use Override;
use RuntimeException;
use Swoole\Coroutine\Channel;
use Swoole\Event as SwooleEvent;
use Swoole\Process;
use Swoole\Timer;
use Verdient\Hyperf3\Database\Listen\AbstractDatabaseEventDispatcher;
use Verdient\Hyperf3\Database\Listen\Event;
use Verdient\Hyperf3\Database\Listen\EventDispatcher;
use Verdient\Hyperf3\Database\Listen\EventModels;
use Verdient\Hyperf3\Database\Model\Utils;
use Verdient\Hyperf3\Logger\StdoutLogger;

use function Hyperf\Config\config;
use function Hyperf\Coroutine\run;

/**
 * PostgreSQL数据库事件调度器
 *
 * @author Verdient。
 */
class Dispatcher extends AbstractDatabaseEventDispatcher
{
    /**
     * @author Verdient。
     */
    public bool $enableCoroutine = false;

    /**
     * 事件调度器
     *
     * @author Verdient。
     */
    protected ?EventDispatcher $eventDispatcher = null;

    /**
     * 缓冲文件夹路径
     *
     * @author Verdient。
     */
    protected ?string $bufferDir = null;

    /**
     * @var array<int,Buffer> 缓冲区
     *
     * @author Verdient。
     */
    protected array $buffers = [];

    /**
     * @author Verdient。
     */
    #[Override]
    public function handle(): void
    {
        $this->bufferDir = implode(DIRECTORY_SEPARATOR, [constant('BASE_PATH'), 'runtime', 'tmp', 'database', 'listen', 'postgresql']);

        if (!is_dir($this->bufferDir)) {
            mkdir($this->bufferDir, 0755, true);
        }

        $process = $this->createProcess();

        $this->registerSignals($process);

        Timer::tick(1000, function () use ($process) {
            if (!Process::kill($process->pid, 0)) {
                $this->logger()->error('The PostgreSQL event dispatcher child process exited abnormally.');
                Process::kill(getmypid(), SIGTERM);
            }
        });

        $this->eventDispatcher = new EventDispatcher();

        $channel = new Channel(10240);

        SwooleEvent::add($process->pipe, function () use ($channel, $process) {
            $content = $process->read();

            if ($content === false) {
                return;
            }

            $channel->push($content);
        });

        $process->start();

        $this->logger()->info('PostgreSQL Event Dispatcher started.');

        $buffer = '';

        run(function () use (&$buffer, $channel) {

            while (true) {
                $content = $channel->pop();

                if ($content === false) {
                    continue;
                }

                $buffer .= $content;

                while (true) {
                    $pos = strpos($buffer, "\n");

                    if ($pos === false) {
                        break;
                    }

                    if ($pos === 0) {
                        $buffer = substr($buffer, 1);
                    } else {
                        $line = substr($buffer, 0, $pos);
                        $buffer = substr($buffer, $pos + 1);
                        $this->consume($line);
                    }
                }
            }
        });
    }

    /**
     * 注册信号
     *
     * @param Process $process 进程对象
     *
     * @author Verdient。
     */
    protected function registerSignals(Process $process): void
    {
        Process::signal(SIGCHLD, function () {
            while (Process::wait(false)) {
            }
        });

        pcntl_async_signals(true);

        foreach ([SIGINT, SIGTERM] as $signal) {
            $handler = pcntl_signal_get_handler($signal);

            pcntl_signal($signal, function () use ($signal, $handler, $process) {
                pcntl_signal($signal, $handler);
                pcntl_signal(SIGCHLD, SIG_IGN);
                $this->logger()->info('PostgreSQL Event Dispatcher stoped.');
                posix_kill($process->pid, $signal);
                posix_kill(getmypid(), $signal);
            });
        }
    }

    /**
     * 创建进程
     *
     * @author Verdient。
     */
    protected function createProcess(): Process
    {
        /** @var ConfigInterface */
        $config = $this->container->get(ConfigInterface::class);

        $slotName = preg_replace(
            '/[^a-z0-9_]/',
            '_',
            strtolower($config->get('app_name') . '_' . $this->connectionName)
        );

        $databaseConfig = $config->get('databases.' . $this->connectionName);

        $dsn = '';

        foreach (
            [
                'host' => $databaseConfig['host'],
                'port' => $databaseConfig['port'],
                'user' => $databaseConfig['username'],
                'password' => $databaseConfig['password'],
                'dbname' => $databaseConfig['database'],
                'replication' => 'database'
            ] as $name => $value
        ) {
            $dsn .= $name . '=' . $value . ' ';
        }

        $processName = config('app_name') . '.PostgreSQL-Replication-' . $this->connectionName;

        $arch = php_uname('m');

        $path = __DIR__ . '/go/pg-replication-' . $arch;

        if (!file_exists($path)) {
            throw new RuntimeException('Unsupported platform for PostgreSQL replication ' . $arch . '.');
        }

        chmod($path, 0755);

        $masterPid = getmypid();

        $process = new Process(function (Process $worker) use ($dsn, $slotName, $processName, $path, $masterPid) {

            putenv('PG_DSN=' . substr($dsn, 0, -1));
            putenv('PG_SLOT=' . $slotName);
            putenv('PG_PROCESS_NAME=' . $processName);
            putenv('PG_MASTER_PID=' . $masterPid);

            $worker->exec($path, []);
        }, true);

        $process->setBlocking(false);

        return $process;
    }

    /**
     * 解析数据
     *
     * @param string $content 数据
     *
     * @author Verdient。
     */
    protected function parse(string $content): array|false
    {
        $data = json_decode($content, true);

        if (!is_array($data)) {
            return false;
        }

        if (
            !isset($data['action'])
            || !is_string($data['action'])
            || !isset($data['xid'])
            || !is_int($data['xid'])
        ) {
            return false;
        }

        return $data;
    }

    /**
     * 消费数据
     *
     * @param string $content 数据
     *
     * @author Verdient。
     */
    protected function consume(string $content): void
    {
        $logger = new StdoutLogger();

        if (!$data = $this->parse($content)) {
            $this->logger()->error($content);
            $logger->error($content);
            return;
        }

        $xid = $data['xid'];

        if ($data['action'] === 'B') {
            $this->buffers[$xid] = new Buffer($this->connectionName . '.' . $xid, $this->bufferDir);
            return;
        }

        if ($data['action'] === 'C') {
            foreach ($this->eventModels($this->buffers[$xid]) as $eventModels) {
                $this->eventDispatcher->dispatch($eventModels);
            }
            unset($this->buffers[$xid]);
            return;
        }

        $identity = [];

        if (isset($data['identity'])) {
            foreach ($data['identity'] as $row) {
                $identity[$row['name']] = $row['value'];
            }
        }

        $columns = [];

        if (isset($data['columns'])) {
            foreach ($data['columns'] as $row) {
                $columns[$row['name']] = $row['value'];
            }
        }

        $this->buffers[$xid]->push([
            $data['action'],
            $data['table'],
            $identity,
            $columns,
        ]);
    }

    /**
     * 获取事件模型集合
     *
     * @param Buffer $buffer 缓冲区
     *
     * @return Generator<int,EventModels>
     * @author Verdient。
     */
    protected function eventModels(Buffer $buffer): Generator
    {
        foreach ($buffer->batch() as $batch) {
            $events = [];

            foreach ($batch as $data) {

                $event = match ($data[0]) {
                    'I' => Event::INSERT,
                    'U' => Event::UPDATE,
                    'D' => Event::DELETE,
                    default => null
                };

                if ($event === null) {
                    continue;
                }

                if (!$modelClass = $this->modelClassResolver->resolve($data[1])) {
                    continue;
                }

                if (!isset($events[$event->name])) {
                    $events[$event->name] = [];
                }

                if (!isset($events[$event->name][$modelClass])) {
                    $events[$event->name][$modelClass] = new EventModels($event, $modelClass);
                }

                $before = [];

                $after = [];

                if ($event === Event::INSERT) {
                    $before = $after = $data[3];
                } else if ($event === Event::UPDATE) {
                    $before = $data[2];

                    foreach ($data[3] as $name => $value) {
                        if (!array_key_exists($name, $before)) {
                            $before[$name] = $value;
                        }
                        $after[$name] = $value;
                    }
                } else if ($event === Event::DELETE) {
                    $before = $after = $data[2];
                }

                $model = Utils::createModelWithOriginals($modelClass, $before);

                foreach (Utils::deserialize($modelClass, $after) as $key => $value) {
                    $model->setAttribute($key, $value);
                }

                $events[$event->name][$modelClass]->add($model);
            }

            foreach ($events as $groupedEventModels) {
                foreach ($groupedEventModels as $eventModels) {
                    yield $eventModels;
                }
            }
        }
    }

    /**
     * 创建默认的记录器的组名集合
     *
     * @return array<int|string,string>
     * @author Verdient。
     */
    protected function groupsForCreateDefaultLogger(): array
    {
        return [static::class => ProcessInterface::class];
    }
}
