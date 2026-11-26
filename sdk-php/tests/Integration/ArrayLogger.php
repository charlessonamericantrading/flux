<?php

declare(strict_types=1);

namespace Flux\Tests\Integration;

use Psr\Log\AbstractLogger;

/** Un `LoggerInterface` que guarda lo que se le escribe: lo que dice el SDK de una denegación SOLO sale por aquí. */
final class ArrayLogger extends AbstractLogger
{
    /** @var list<array{level:string,message:string}> */
    public array $records = [];

    public function log($level, string|\Stringable $message, array $context = []): void
    {
        $this->records[] = ['level' => (string) $level, 'message' => (string) $message];
    }

    /** @return list<string> */
    public function messages(string $level = null): array
    {
        return array_values(array_map(
            static fn (array $r): string => $r['message'],
            array_filter($this->records, static fn (array $r): bool => $level === null || $r['level'] === $level),
        ));
    }

    public function contains(string $needle, ?string $level = null): bool
    {
        foreach ($this->messages($level) as $m) {
            if (str_contains($m, $needle)) {
                return true;
            }
        }

        return false;
    }
}
