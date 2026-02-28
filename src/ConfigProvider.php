<?php

declare(strict_types=1);

namespace Verdient\Hyperf3\Database\Listen\PostgreSQL;

class ConfigProvider
{
    public function __invoke(): array
    {
        return [
            'listeners' => [
                BootApplicationListener::class
            ]
        ];
    }
}
