<?php

declare(strict_types=1);

namespace Verdient\Hyperf3\Database\Listen\PostgreSQL;

use Generator;
use SplFileObject;

/**
 * 缓冲区
 *
 * @author Verdient。
 */
class Buffer
{
    /**
     * 缓冲区
     *
     * @author Verdient。
     */
    protected array $buffers = [];

    /**
     * 路径
     *
     * @author Verdient。
     */
    protected string $path;

    /**
     * @param string $identifier 标识符
     * @param string $dir 文件夹路径
     *
     * @author Verdient。
     */
    public function __construct(
        protected string $identifier,
        protected string $dir
    ) {
        $this->path = $dir . DIRECTORY_SEPARATOR . $identifier;
    }

    /**
     * 压入缓冲区
     *
     * @param array $value 缓冲的内容
     *
     * @author Verdient。
     */
    public function push(array $value): void
    {
        $this->buffers[] = $value;

        if (count($this->buffers) >= 5000) {
            file_put_contents($this->path, implode("\n", array_map(fn($value) => $this->seralize($value), $this->buffers)) . "\n", FILE_APPEND);
            $this->buffers = [];
        }
    }

    /**
     * 序列化数据
     *
     * @param string $data 数据
     *
     * @author Verdient。
     */
    protected function seralize(array $data): string
    {
        return json_encode($data);
    }

    /**
     * 反序列化数据
     *
     * @param string $data 数据
     *
     * @author Verdient。
     */
    protected function unseralize(string $data): array
    {
        return json_decode($data, true);
    }

    /**
     * 逐个获取缓冲区数据
     *
     * @param int $size 获取的数量
     *
     * @return Generator<int,array>
     * @author Verdient。
     */
    public function each(): Generator
    {
        if (file_exists($this->path)) {

            $file = new SplFileObject($this->path);

            foreach ($file as $line) {
                $line = substr($line, 0, -1);

                if ($line === '') {
                    continue;
                }

                yield $this->unseralize($line);
            }
        }

        foreach ($this->buffers as $value) {
            yield $value;
        }
    }

    /**
     * 批量获取缓冲区数据
     *
     * @param int $size 获取的数量
     *
     * @return Generator<int,array<int,array>>
     * @author Verdient。
     */
    public function batch(int $size = 1000): Generator
    {
        $result = [];

        foreach ($this->each() as $value) {
            $result[] = $value;

            if (count($result) === $size) {
                yield $result;
                $result = [];
            }
        }

        if (!empty($result)) {
            yield $result;
        }
    }

    /**
     * @author Verdient。
     */
    public function __destruct()
    {
        if (file_exists($this->path)) {
            @unlink($this->path);
        }
    }
}
