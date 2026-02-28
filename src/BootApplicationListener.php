<?php

declare(strict_types=1);

namespace Verdient\Hyperf3\Database\Listen\PostgreSQL;

use Hyperf\Event\Contract\ListenerInterface;
use Hyperf\Framework\Event\BootApplication;
use Override;
use Verdient\Hyperf3\Database\Listen\DispatcherManager;

/**
 * 启动程序监听器
 *
 * @author Verdient。
 */
class BootApplicationListener implements ListenerInterface
{
    /**
     * @author Verdient。
     */
    #[Override]
    public function listen(): array
    {
        return [
            BootApplication::class
        ];
    }

    /**
     * @param BootApplication $event
     *
     * @author Verdient。
     */
    #[Override]
    public function process(object $event): void
    {
        DispatcherManager::register('pgsql', Dispatcher::class);
    }
}
