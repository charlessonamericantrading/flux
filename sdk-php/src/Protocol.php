<?php

declare(strict_types=1);

namespace Flux;

/**
 * Constantes y derivaciones del protocolo.
 * Contrato normativo: specification/02-naming.md, specification/03-delivery.md
 *
 * Todo lo de aquí sale de `protocol.json`. Si divergen, `protocol.json` manda — es lo
 * que consumen los otros cinco SDKs y los agentes. Los tests leen el fichero y comparan,
 * así que una divergencia falla en CI en vez de en producción.
 */
final class Protocol
{
    public const PROTOCOL_VERSION = '1.0.0';
    public const SPEC_VERSION     = '1.0';

    /** 1 MiB. Ver 01-envelope.md §7. */
    public const MAX_MESSAGE_BYTES = 1_048_576;

    // ─── Configuración canónica de consumidor — 03-delivery.md §2 ─────────────

    public const ACK_POLICY = 'explicit';

    /**
     * ⚠️ `ACK_WAIT_MS` DEBE ser igual a `BACKOFF_MS[0]`.
     *
     * JetStream **sobrescribe** `ack_wait` con `backoff[0]` sin devolver error y sin
     * avisar (verificado contra NATS 2.14.5, ver 03-delivery.md §2.1). Es decir:
     * `backoff[0]` ES el presupuesto de duración del handler. Un `backoff[0]` de 1 s
     * daría un `ack_wait` efectivo de 1 s y cualquier handler que toque una base de
     * datos se ejecutaría en concurrencia consigo mismo, en cada mensaje, bajo carga.
     *
     * `ProtocolTest::testAckWaitEsBackoffCero()` fija esta invariante.
     */
    public const ACK_WAIT_MS = 30_000;

    /** 1 entrega inicial + 5 reintentos, uno por entrada de `BACKOFF_MS`. */
    public const MAX_DELIVER = 6;

    /**
     * Backoff canónico. `sum(BACKOFF_MS)` = 3090 s ≈ 51 min 30 s hasta la DLQ, que es
     * el límite INFERIOR: si el handler agota el plazo en vez de fallar, cada intento
     * añade hasta un `ack_wait` más (~54 min). Ver 03-delivery.md §2.
     */
    public const BACKOFF_MS = [30_000, 60_000, 300_000, 900_000, 1_800_000];

    public const MAX_ACK_PENDING = 256;
    public const DELIVER_POLICY  = 'all';
    public const REPLAY_POLICY   = 'instant';

    // ─── Streams — 02-naming.md §3.3 ──────────────────────────────────────────

    public const STREAM_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000;

    /** Más larga que la del stream principal: la DLQ es material forense. */
    public const DLQ_STREAM_MAX_AGE_MS = 90 * 24 * 60 * 60 * 1000;

    /**
     * ⚠️ Deduplica PUBLICACIONES con el mismo `Nats-Msg-Id` dentro de la ventana.
     * NO deduplica reentregas de consumo — 03-delivery.md §3. Nunca sustituye a la
     * idempotencia del consumidor.
     */
    public const DUPLICATE_WINDOW_MS = 120_000;

    /**
     * Con 3 réplicas un stream no arranca contra un NATS de un solo nodo, así que la
     * creación de comodidad del SDK usa 1. En producción los streams los provisiona
     * IaC — 02-naming.md §3.2.
     */
    public const STREAM_REPLICAS_SDK_DEFAULT = 1;

    // ─── Naming — 02-naming.md §1 ─────────────────────────────────────────────

    /**
     * `<dominio>.<agregado>.v<major>.<evento>`, sin delimitadores de PCRE para poder
     * compararlo carácter a carácter con `protocol.json` en los tests.
     */
    public const SUBJECT_PATTERN = '^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]*\.[a-z0-9]+(-[a-z0-9]+)*$';

    /**
     * Nombre de servicio. NATS aceptaría un durable `FacturacionAPI__pedidos_…` sin
     * error, así que si el SDK no valida esto el incumplimiento del patrón de
     * `durableConsumer` solo se descubre al parsear nombres en una herramienta.
     */
    public const SERVICE_PATTERN = '^[a-z0-9]+(-[a-z0-9]+)*$';

    /** RFC 3339 UTC con EXACTAMENTE 3 decimales y sufijo `Z` — 01-envelope.md §2.2. */
    public const TIME_PATTERN = '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$';

    /** `format()` de PHP. `v` = milisegundos con 3 dígitos; `DateTime::ATOM` no vale. */
    public const TIME_FORMAT = 'Y-m-d\TH:i:s.v\Z';

    /** Cabecera obligatoria al publicar — 03-delivery.md §3. */
    public const MSG_ID_HEADER = 'Nats-Msg-Id';

    private function __construct()
    {
    }

    // ─── Subjects ─────────────────────────────────────────────────────────────

    /**
     * Valida y descompone un subject de NATS.
     *
     * @throws InvalidSubjectException
     */
    public static function parseSubject(string $subject): ParsedSubject
    {
        if ($subject !== strtolower($subject)) {
            // Se comprueba ANTES que el patrón para poder dar este mensaje concreto: los
            // subjects de NATS son case-sensitive y `Pedidos.` ≠ `pedidos.`. Un publish
            // por core NATS a un subject en mayúsculas no da NINGÚN error y el evento se
            // evapora — 02-naming.md §1.1.
            throw new InvalidSubjectException(
                $subject,
                'debe ir todo en minúsculas — NATS es case-sensitive y una mayúscula crea '
                . 'un subject fantasma al que nadie está suscrito, sin producir error'
            );
        }

        if (preg_match('/' . self::SUBJECT_PATTERN . '/', $subject) !== 1) {
            $tokens = count(explode('.', $subject));
            throw new InvalidSubjectException(
                $subject,
                $tokens !== 4
                    ? "debe tener exactamente 4 tokens (<dominio>.<agregado>.v<major>.<evento>), tiene {$tokens}"
                    : 'formato esperado <dominio>.<agregado>.v<major>.<evento> en kebab-case'
            );
        }

        [$domain, $aggregate, $version, $event] = explode('.', $subject);

        return new ParsedSubject($domain, $aggregate, (int) substr($version, 1), $event);
    }

    public static function isValidSubject(string $subject): bool
    {
        try {
            self::parseSubject($subject);

            return true;
        } catch (InvalidSubjectException) {
            return false;
        }
    }

    /** `pedidos.pedido.v1.creado` → `com.flux.pedidos.pedido.creado.v1` — 02-naming.md §2. */
    public static function subjectToType(string $subject): string
    {
        $p = self::parseSubject($subject);

        return "com.flux.{$p->domain}.{$p->aggregate}.{$p->event}.v{$p->major}";
    }

    /** `EVT_PEDIDOS`. NATS no admite `.` en nombres de stream — 02-naming.md §3. */
    public static function streamName(string $domain): string
    {
        return 'EVT_' . strtoupper(str_replace('-', '_', $domain));
    }

    public static function dlqStreamName(string $domain): string
    {
        return 'DLQ_' . strtoupper(str_replace('-', '_', $domain));
    }

    /**
     * `facturacion-api__pedidos_pedido_v1_creado` — 02-naming.md §4.
     *
     * NATS rechaza `.`, `*` y `>` en nombres de durable. Separar el servicio con `__`
     * mantiene la reversibilidad: partiendo por `__` se recuperan servicio y subject
     * exactos, que es lo que hace útil un `nats consumer ls` a las 3 de la mañana.
     *
     * Valida las dos mitades: el subject y **también el nombre de servicio**, porque
     * NATS aceptaría `FacturacionAPI__…` sin error y el incumplimiento quedaría latente.
     *
     * @throws InvalidSubjectException
     * @throws \InvalidArgumentException si el nombre de servicio no es kebab-case
     */
    public static function durableName(string $service, string $subject): string
    {
        self::assertServiceName($service);
        self::parseSubject($subject);

        return $service . '__' . str_replace(['.', '-'], '_', $subject);
    }

    /** @throws \InvalidArgumentException */
    public static function assertServiceName(string $service): void
    {
        if (preg_match('/' . self::SERVICE_PATTERN . '/', $service) !== 1) {
            throw new \InvalidArgumentException(
                "nombre de servicio inválido \"{$service}\": debe ser kebab-case en minúsculas "
                . '(^[a-z0-9]+(-[a-z0-9]+)*$). NATS aceptaría el durable resultante sin error, '
                . 'así que el incumplimiento solo se descubriría al parsear nombres en una '
                . 'herramienta (protocol.json → naming.service).'
            );
        }
    }

    /**
     * `dlq.pedidos.pedido.v1.creado`.
     *
     * PREFIJO, no sufijo: un sufijo encajaría con `<dominio>.>` y el stream principal
     * capturaría sus propios muertos — 02-naming.md §3.1.
     */
    public static function dlqSubject(string $subject): string
    {
        return 'dlq.' . $subject;
    }

    public static function isDlqSubject(string $subject): bool
    {
        return str_starts_with($subject, 'dlq.');
    }

    /** `/produccion/pedidos-api` — 01-envelope.md §2. */
    public static function sourceUri(string $environment, string $service): string
    {
        return '/' . $environment . '/' . $service;
    }

    // ─── Unidades ─────────────────────────────────────────────────────────────

    /**
     * Milisegundos → nanosegundos.
     *
     * Diferencia con el SDK de Python, que entrega **segundos**: `nats-py` convierte a
     * nanosegundos él mismo dentro de `ConsumerConfig.as_dict()`. Aquí el transporte
     * habla directamente con la API `$JS.API.*`, cuyo JSON usa nanosegundos para
     * `ack_wait` y `backoff`. Equivocar la unidad no da error: da un `ack_wait` de
     * 950 años o de 30 µs, en silencio.
     */
    public static function nanos(int $ms): int
    {
        return $ms * 1_000_000;
    }

    // ─── Identidad ────────────────────────────────────────────────────────────

    private static int $uuid7LastMs = -1;
    private static int $uuid7Counter = 0;

    /**
     * UUIDv7 canónico (RFC 9562) — 01-envelope.md §2.
     *
     * PHP no lo trae de serie (no hay `uuid_create` sin `ext-uuid`, y `ext-uuid` no
     * genera v7), así que se implementa aquí en lugar de añadir `ramsey/uuid` por 20
     * líneas — un SDK de protocolo no debería imponer dependencias evitables.
     *
     * Los 12 bits de `rand_a` se usan como contador dentro del mismo milisegundo, igual
     * que en los SDKs de Node y Python. No es decorativo: 01-envelope.md §2.6 se apoya
     * en que ordenar por `id` dentro de un mismo `source` equivale a ordenar por
     * instante de generación —útil al reconstruir historiales desde la DLQ— y sin
     * contador dos eventos del mismo milisegundo quedan en orden aleatorio.
     */
    public static function uuid7(): string
    {
        $ms = (int) floor(microtime(true) * 1000);

        if ($ms > self::$uuid7LastMs) {
            self::$uuid7LastMs = $ms;
            // Arranque aleatorio en la mitad baja del contador: deja ~2048 ids
            // monotónicos por milisegundo antes de tener que avanzar el reloj.
            self::$uuid7Counter = random_int(0, 0x7FF);
        } else {
            // Mismo milisegundo, o el reloj del sistema ha ido hacia atrás (NTP).
            // Se sigue incrementando para no romper la monotonía.
            self::$uuid7Counter++;
            if (self::$uuid7Counter > 0xFFF) {
                self::$uuid7LastMs++;
                self::$uuid7Counter = random_int(0, 0x7FF);
            }
            $ms = self::$uuid7LastMs;
        }

        $counter = self::$uuid7Counter;

        // Se construye por bytes y no con aritmética de 128 bits porque PHP no tiene
        // enteros de 128 bits: `1 << 80` desbordaría a float y perdería precisión.
        $bytes = pack('n', ($ms >> 32) & 0xFFFF)          // unix_ts_ms, 16 bits altos
            . pack('N', $ms & 0xFFFF_FFFF)                // unix_ts_ms, 32 bits bajos
            . chr(0x70 | (($counter >> 8) & 0x0F))        // versión 7 + rand_a alto
            . chr($counter & 0xFF);                       // rand_a bajo

        $random = random_bytes(8);
        $random[0] = chr((ord($random[0]) & 0x3F) | 0x80); // variante RFC 4122 (10xx)

        $hex = bin2hex($bytes . $random);

        return substr($hex, 0, 8) . '-' . substr($hex, 8, 4) . '-' . substr($hex, 12, 4)
            . '-' . substr($hex, 16, 4) . '-' . substr($hex, 20, 12);
    }
}
