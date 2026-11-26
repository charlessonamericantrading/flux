<?php

declare(strict_types=1);

namespace Flux;

/**
 * Envelope CloudEvents 1.0 — construcción, serialización y parseo.
 * Contrato normativo: specification/01-envelope.md
 */
final class Envelope
{
    /** Atributos raíz permitidos. Todo lo demás va dentro de `data` — 01-envelope.md §3.4. */
    public const ALLOWED_ROOT_ATTRIBUTES = [
        'specversion', 'id', 'source', 'type', 'time', 'datacontenttype', 'dataschema',
        'subject', 'correlationid', 'tenantid', 'producerversion', 'dataclassification',
        'causationid', 'partitionkey', 'traceparent', 'tracestate',
        // Extensión OPCIONAL de la firma — 07-signing.md §4. Se aceptan SIEMPRE, aunque el
        // SDK no verifique: un evento firmado tiene que poder consumirse en modo `off`, o
        // la adopción gradual de §7 sería imposible (el primer productor que firmara
        // dejaría a todos los consumidores emitiendo UNKNOWN_ROOT_ATTRIBUTE).
        'signkeyid', 'signature',
        'dlqreason', 'dlqattempts', 'dlqconsumer', 'dlqerror', 'dlqtime', 'data',
    ];

    public const REQUIRED_ATTRIBUTES = [
        'specversion', 'id', 'source', 'type', 'time', 'datacontenttype', 'dataschema', 'data',
    ];

    /**
     * Extensiones del perfil flux cuya ausencia es POISON — 01-envelope.md §3.1.
     *
     * Se exigen de verdad, no solo en la prosa. Si no se exigieran no serían
     * obligatorias, serían recomendadas, y cada consumidor tendría que tratar el caso
     * ausente — que es exactamente lo que declararlas obligatorias pretendía evitar.
     *
     * Asumir un valor por defecto es peligroso en las cuatro: un `dataclassification`
     * ausente tomado como `internal` hace circular PII con 30 días de retención en vez
     * de 7, y un `tenantid` ausente tomado como `system` cruza fronteras de tenant.
     */
    public const REQUIRED_EXTENSIONS = [
        'correlationid', 'tenantid', 'producerversion', 'dataclassification',
    ];

    public const VALID_CLASSIFICATIONS = ['public', 'internal', 'confidential', 'restricted'];

    /**
     * Atributos que DEBEN llegar como cadena JSON — 01-envelope.md §2.4.
     *
     * En PHP hay que comprobarlo con `is_string()` explícito: el lenguaje coacciona con
     * entusiasmo (`"42" == 42` es `true`, y `(string) 42` no se queja nunca), así que sin
     * esta lista un `{"tenantid": 42}` se colaría hasta el handler como entero y
     * reventaría —o peor, NO reventaría— en la primera comparación laxa.
     */
    public const STRING_ATTRIBUTES = [
        'id', 'source', 'type', 'time', 'correlationid', 'tenantid', 'producerversion',
    ];

    /**
     * Las banderas de la forma canónica — ver `Json` para el porqué de cada una (UTF-8 literal, barras sin
     * escapar, U+2028 literal, sin `.0` en los enteros, `INF`/`NAN` fallan en vez de producir `0`).
     */
    public const JSON_FLAGS = Json::FLAGS;

    /** Un stacktrace entero cabría, pero se comería el presupuesto de 1 MiB del evento. */
    private const MAX_DLQ_ERROR_BYTES = 1024;

    private function __construct()
    {
    }

    // ─── Tiempo ───────────────────────────────────────────────────────────────

    /**
     * RFC 3339 en UTC con **exactamente 3 decimales y sufijo `Z`** — 01-envelope.md §2.2.
     *
     * ⚠️ Ni `DateTimeInterface::ATOM` ni `format('c')` valen: no emiten milisegundos y ponen `+00:00` en
     * lugar de `Z`. "Precisión de milisegundos" tampoco basta como regla, porque los formateadores por
     * defecto de cada lenguaje producen cosas distintas y todas válidas según RFC 3339 (Go recorta ceros,
     * Python da microsegundos y offset). El resultado sería que dos servicios del mismo ecosistema emiten
     * `time` distintos para el mismo instante.
     *
     * `v` en `format()` son los milisegundos con tres dígitos, ceros incluidos.
     */
    public static function formatTime(\DateTimeInterface $when): string
    {
        return \DateTimeImmutable::createFromInterface($when)
            ->setTimezone(new \DateTimeZone('UTC'))
            ->format(Protocol::TIME_FORMAT);
    }

    public static function now(): string
    {
        // `new DateTimeImmutable('now')` incluye microsegundos desde PHP 7.1; `time()` no los tendría y todos
        // los eventos saldrían con `.000Z`.
        return self::formatTime(new \DateTimeImmutable('now', new \DateTimeZone('UTC')));
    }

    /**
     * Acepta un `DateTimeInterface` o un texto RFC 3339, y devuelve la **forma canónica** (3 decimales y
     * `Z`, en UTC) — 01-envelope.md §2.2.
     *
     * Un constructor **normaliza** lo que le dan o lo **rechaza**: copiarlo tal cual dejaría pasar
     * `…39Z` o `…+02:00` —otros bytes— o un valor que ni siquiera es un instante. Los decimales de más
     * de tres se truncan; un segundo 60, un año fuera de 0000–9999 y un desfase de 24 horas no se admiten.
     * Un `time` rechazado es `WRONG_ATTRIBUTE_TYPE` (CloudEvents lo tipa `Timestamp`).
     *
     * @throws InvalidEnvelopeInputException
     */
    public static function coerceTime(\DateTimeInterface|string $when): string
    {
        if ($when instanceof \DateTimeInterface) {
            return self::formatTime($when);
        }

        $canonical = Time::canonical($when);
        if ($canonical === null) {
            throw new InvalidEnvelopeInputException(
                '`time` debe ser un instante RFC 3339 (`2026-08-20T10:41:07.318Z`; se normaliza a 3 decimales y `Z`) y llegó un texto '
                . 'que no lo es (01-envelope.md §2.2). Pasa un DateTimeInterface y el SDK lo formatea.',
                'WRONG_ATTRIBUTE_TYPE',
            );
        }

        return $canonical;
    }

    /**
     * Los atributos que el SDK rellena NO pueden llegar vacíos a un constructor: una cadena vacía y una
     * ausente son lo mismo (01-envelope.md §3.1, §3.3), y un evento que su propio parser rechazaría no se
     * construye. `$rejects` decide el error: el de eventos y estado lleva el código del parser; el de
     * petición/respuesta, `INVALID_RPC_CALL` (su conjunto de códigos es cerrado).
     *
     * @param list<array{0:string,1:mixed,2?:bool}> $fields `[nombre, valor, esExtensión]`
     * @param (callable(string,string):\Throwable)|null $rejects `(mensaje, código) → excepción`
     *
     * @throws \Throwable
     */
    public static function requireAttributes(array $fields, ?callable $rejects = null): void
    {
        foreach ($fields as $field) {
            [$name, $value] = $field;
            if (is_string($value) && $value !== '') {
                continue;
            }
            $code = ($field[2] ?? false) ? 'MISSING_REQUIRED_EXTENSION' : 'MISSING_REQUIRED_ATTRIBUTE';
            $message = "`{$name}` es obligatorio y llegó "
                . ($value === null ? 'ausente' : (is_string($value) ? 'vacío' : 'como ' . get_debug_type($value)))
                . '. Lo rellena el SDK a partir de connect() y del evento en curso; si estás llamando a un constructor a mano, rellénalo '
                . 'con una cadena no vacía (01-envelope.md §5).';
            throw $rejects !== null ? $rejects($message, $code) : new InvalidEnvelopeInputException($message, $code);
        }
    }

    // ─── Construcción ─────────────────────────────────────────────────────────

    /**
     * Monta el CloudEvent.
     *
     * Todos los parámetros se pasan con nombre a propósito (`FluxBus` lo hace así): `source`, `id` y
     * `correlationid` son tres cadenas seguidas y posicionalmente son indistinguibles.
     *
     * Valida en este orden —el primero que falla gana—: `data` objeto, el subject (`RESERVED_DOMAIN`, y
     * después su forma), la clasificación, que los atributos que el SDK rellena no lleguen vacíos y que
     * `time` sea un instante.
     *
     * @param array<string,mixed> $data
     *
     * @throws EnvelopeException|InvalidSubjectException|ReservedDomainException|InvalidDataClassificationException
     */
    public static function build(
        string $subject,
        array $data,
        string $id,
        string $source,
        string $producerversion,
        string $tenantid,
        string $dataclassification,
        string $dataschema,
        string $correlationid,
        \DateTimeInterface|string|null $time = null,
        ?string $aggregateId = null,
        ?string $causationid = null,
        ?string $partitionkey = null,
        ?string $traceparent = null,
        ?string $tracestate = null,
    ): FluxEvent {
        // Un array o escalar en la raíz impide añadir campos después sin romper el esquema —
        // 01-envelope.md §4. `[]` se acepta porque en PHP es a la vez la lista vacía y el objeto vacío, y el
        // objeto vacío sí es legal (ver `serialize`).
        if ($data !== [] && array_is_list($data)) {
            throw new EnvelopeException(
                '`data` debe ser un objeto JSON en la raíz (ni array ni escalar), para poder añadir campos '
                . 'sin romper el contrato — 01-envelope.md §4'
            );
        }

        $type = Protocol::subjectToType($subject);

        if (!in_array($dataclassification, self::VALID_CLASSIFICATIONS, true)) {
            // Fallar aquí y no en el consumidor: publicar con una clasificación inventada produciría un
            // POISON en cada consumidor del subject, y el productor no se enteraría nunca.
            throw new InvalidDataClassificationException($dataclassification);
        }

        self::requireAttributes([
            ['id', $id],
            ['source', $source],
            ['dataschema', $dataschema],
            ['correlationid', $correlationid, true],
            ['tenantid', $tenantid, true],
            ['producerversion', $producerversion, true],
        ]);

        return new FluxEvent(
            id: $id,
            source: $source,
            type: $type,
            time: $time !== null ? self::coerceTime($time) : self::now(),
            dataschema: $dataschema,
            correlationid: $correlationid,
            tenantid: $tenantid,
            producerversion: $producerversion,
            dataclassification: $dataclassification,
            data: $data,
            // ⚠️ El `subject` del envelope es el ID DEL AGREGADO — 01-envelope.md §2.1.
            subject: $aggregateId,
            causationid: $causationid,
            // Por convención, la clave de partición es el id del agregado — §3.2.
            partitionkey: $partitionkey ?? $aggregateId,
            traceparent: $traceparent,
            tracestate: $tracestate,
        );
    }

    /**
     * Los bytes del evento en su forma canónica, **sin** límite de tamaño: lo pone quien envía. Es lo que usa
     * la petición/respuesta, que comprueba el suyo contra el `max_payload` del broker.
     *
     * @throws EnvelopeException
     */
    public static function encode(FluxEvent $event): string
    {
        $payload = $event->toArray();

        // `data` es un objeto JSON en la raíz, pero en PHP `[]` es indistinguible de la lista vacía y
        // `json_encode([])` daría `[]`. En la raíz de `data` la ambigüedad se resuelve por contrato (§4:
        // DEBE ser objeto), así que se fuerza `{}`. Los objetos vacíos ANIDADOS que vienen de un mensaje
        // recibido se conservan (`Json::fromObjectTree`); en uno construido a mano usa `new \stdClass`.
        if ($payload['data'] === []) {
            $payload['data'] = new \stdClass();
        }

        try {
            return Json::encode($payload);
        } catch (\JsonException $e) {
            throw new EnvelopeException(
                'el evento no es serializable a JSON: ' . $e->getMessage()
                . ' (¿INF, NAN o una cadena que no es UTF-8 válido en `data`?)',
                previous: $e
            );
        }
    }

    /**
     * Serializa el evento y aplica el límite de 1 MiB — 01-envelope.md §7.
     *
     * @throws EnvelopeException
     */
    public static function serialize(FluxEvent $event): string
    {
        $json = self::encode($event);

        $bytes = strlen($json);
        if ($bytes > Protocol::MAX_MESSAGE_BYTES) {
            // Fallar aquí y no en el broker: el mensaje del broker no dice qué hacer.
            throw new EnvelopeException(
                "evento de {$bytes} bytes supera el límite de " . Protocol::MAX_MESSAGE_BYTES
                . ' (1 MiB). Aplica claim-check: sube el contenido a almacenamiento de objetos '
                . 'y publica { uri, sha256, bytes } en su lugar (01-envelope.md §7).'
            );
        }

        return $json;
    }

    // ─── Parseo ───────────────────────────────────────────────────────────────

    /**
     * Un fallo aquí es POISON: el mensaje ni siquiera es interpretable, así que nunca llega al handler —
     * 04-errors.md §1.3.
     *
     * Se decodifica en **modo objeto**: `{}` y `[]` siguen siendo distintos (con `assoc = true` PHP los
     * colapsa en el mismo `array` vacío, y un `{"a":{}}` recibido saldría de la DLQ como `{"a":[]}`: otros
     * bytes). Un cuerpo que no es UTF-8, o que empieza por una marca de orden de bytes, no es JSON (RFC 8259
     * §8.1): `MALFORMED_JSON`.
     *
     * Sobre fidelidad numérica (01-envelope.md §2.5): `json_decode` convierte los números de `data` a
     * `int`/`float` de PHP, así que un `4995.00` vuelve a salir como `4995` —igual que en Node, la referencia—,
     * a diferencia de Rust, Go y Java, que conservan los bytes crudos. La forma de no tener el problema es la
     * que recomienda la propia spec: no metas decimales en `data`; los importes van como entero en la unidad
     * mínima.
     *
     * @throws PoisonError
     */
    public static function parse(string $payload): FluxEvent
    {
        try {
            /** @var mixed $tree */
            $tree = Json::decodeObjects($payload);
        } catch (\JsonException $e) {
            throw new PoisonError(
                'el cuerpo del mensaje no es JSON válido',
                'MALFORMED_JSON',
                $e
            );
        }

        if (!$tree instanceof \stdClass) {
            throw new PoisonError(
                'el cuerpo del mensaje no es un objeto JSON',
                'NOT_AN_OBJECT'
            );
        }

        /** @var array<string,mixed> $raw */
        $raw = get_object_vars($tree);

        if (($raw['specversion'] ?? null) !== Protocol::SPEC_VERSION) {
            throw new PoisonError(
                'specversion no soportada: ' . self::describe($raw['specversion'] ?? null)
                . ' (se esperaba "' . Protocol::SPEC_VERSION . '")',
                'UNSUPPORTED_SPECVERSION'
            );
        }

        // `null` cuenta como ausente: 01-envelope.md §3.3 prohíbe darle significado propio a un valor vacío,
        // así que un atributo obligatorio a `null` está igual de mal que no estar.
        $missing = array_values(array_filter(
            self::REQUIRED_ATTRIBUTES,
            static fn (string $a): bool => ($raw[$a] ?? null) === null
        ));
        if ($missing !== []) {
            throw new PoisonError(
                'faltan atributos obligatorios de CloudEvents: ' . implode(', ', $missing),
                'MISSING_REQUIRED_ATTRIBUTE'
            );
        }

        // Ausente, `null` y `""` son lo mismo aquí, y se evalúa ANTES que el enum: la tabla de
        // 01-envelope.md §3.1 fija que `"dataclassification": ""` es MISSING_REQUIRED_EXTENSION y no
        // INVALID_DATACLASSIFICATION. Si dos SDKs emiten códigos distintos ante la misma entrada, agrupar por
        // causa deja de funcionar.
        $missingExt = array_values(array_filter(
            self::REQUIRED_EXTENSIONS,
            static fn (string $a): bool => ($raw[$a] ?? null) === null || $raw[$a] === ''
        ));
        if ($missingExt !== []) {
            throw new PoisonError(
                'faltan extensiones obligatorias del perfil flux: ' . implode(', ', $missingExt)
                . '. Su ausencia no es recuperable asumiendo un valor: un dataclassification '
                . 'ausente tomado como "internal" haría circular PII con la retención larga '
                . '(01-envelope.md §3.1)',
                'MISSING_REQUIRED_EXTENSION'
            );
        }

        $classification = $raw['dataclassification'];
        // El `is_string` va delante y no sobra: evita que un array acabe interpolado en el mensaje.
        if (!is_string($classification) || !in_array($classification, self::VALID_CLASSIFICATIONS, true)) {
            // Un valor fuera del enum no se puede degradar a algo seguro: la retención y las ACLs de
            // 06-security.md §5 se derivan de él, así que inventarle un sentido es peor que rechazar el mensaje.
            throw new PoisonError(
                'dataclassification inválido: ' . self::describe($classification)
                . '. Valores permitidos: ' . implode(', ', self::VALID_CLASSIFICATIONS),
                'INVALID_DATACLASSIFICATION'
            );
        }

        // Los tipos son exactos: `{"tenantid": 42}` es POISON, no el tenant "42". En PHP hay que comprobarlo a
        // mano — el lenguaje coacciona con entusiasmo y `$evento->tenantid == 42` sería `true` para el tenant
        // "42" y también para el entero 42. Jackson coacciona, Go rechaza, Node lo propagaba como número: el
        // mismo mensaje, tres comportamientos, y el que "funciona" es el peor — 01-envelope.md §2.4.
        foreach (self::STRING_ATTRIBUTES as $attribute) {
            if (!is_string($raw[$attribute])) {
                throw new PoisonError(
                    "{$attribute} debe ser una cadena, llegó " . get_debug_type($raw[$attribute])
                    . ' (' . self::describe($raw[$attribute]) . ')',
                    'WRONG_ATTRIBUTE_TYPE'
                );
            }
        }

        if (($raw['datacontenttype'] ?? null) !== 'application/json') {
            throw new PoisonError(
                'datacontenttype no soportado: ' . self::describe($raw['datacontenttype'] ?? null),
                'UNSUPPORTED_CONTENT_TYPE'
            );
        }

        $unknown = array_values(array_diff(array_keys($raw), self::ALLOWED_ROOT_ATTRIBUTES));
        if ($unknown !== []) {
            // Estricto a propósito: permitir atributos raíz ad-hoc lleva a serializar JSON dentro de un string
            // y el envelope deja de ser interoperable. Las extensiones de CloudEvents solo admiten escalares —
            // 01-envelope.md §3.4.
            throw new PoisonError(
                'atributos raíz desconocidos: ' . implode(', ', array_map('strval', $unknown))
                . '. Todo metadato adicional va dentro de `data`',
                'UNKNOWN_ROOT_ATTRIBUTE'
            );
        }

        // Simétrico con `build()`: `data` DEBE ser un objeto, ni array ni escalar (01-envelope.md §4). En modo
        // objeto la diferencia es exacta: una lista JSON llega como `array` y un objeto como `stdClass`.
        if (!$raw['data'] instanceof \stdClass) {
            throw new PoisonError(
                '`data` debe ser un objeto JSON en la raíz (ni array ni escalar), llegó '
                . (is_array($raw['data']) ? 'un array JSON' : get_debug_type($raw['data'])),
                'WRONG_ATTRIBUTE_TYPE'
            );
        }

        // Los tipos son exactos TAMBIÉN en lo no obligatorio: `{"subject": 5}` o `{"dlqattempts": "seis"}` no
        // se propagan. Es el último código de `parse` (el orden es parte del contrato entre SDKs).
        $bad = self::incompatibleAttribute($raw);
        if ($bad !== null) {
            throw new PoisonError(
                "{$bad} tiene un tipo incompatible con el contrato (01-envelope.md §2.4): "
                . self::describe(is_object($raw[$bad]) || is_array($raw[$bad]) ? 'un objeto o un array' : $raw[$bad]),
                'INVALID_ATTRIBUTE_TYPE'
            );
        }

        $data = Json::fromObjectTree($raw['data']);
        // La raíz de `data` pasa a `array` (lo que los handlers esperan). Un objeto raíz cuyas claves serían
        // una lista (`{"0":…}`) no tiene forma común entre lenguajes (01-envelope.md §4.1: claves con letras).
        $raw['data'] = $data instanceof \stdClass ? get_object_vars($data) : $data;

        // Con los nombres del constructor idénticos a los del envelope no hay mapeo que desincronizar.
        return new FluxEvent(...$raw);
    }

    private const OPTIONAL_STRINGS = [
        'dataschema', 'subject', 'causationid', 'partitionkey', 'traceparent', 'tracestate',
        'dlqconsumer', 'dlqerror', 'dlqtime', 'signkeyid', 'signature',
    ];

    /**
     * El primer atributo conocido —ni de los siete cadena obligatorios— con un tipo incompatible, o `null`. `null`
     * es «ausente» en un opcional. `dlqattempts` es un entero de 0 a 2^32 − 1 (en PHP, `7.0` es un `float` y se
     * rechaza, como en Rust) y `dlqreason`, una de las tres clases.
     *
     * @param array<string,mixed> $raw
     */
    private static function incompatibleAttribute(array $raw): ?string
    {
        foreach (self::OPTIONAL_STRINGS as $a) {
            if (($raw[$a] ?? null) !== null && !is_string($raw[$a])) {
                return $a;
            }
        }
        $reason = $raw['dlqreason'] ?? null;
        if ($reason !== null && !in_array($reason, ['retryable', 'permanent', 'poison'], true)) {
            return 'dlqreason';
        }
        $attempts = $raw['dlqattempts'] ?? null;
        if ($attempts !== null && (!is_int($attempts) || $attempts < 0 || $attempts > 4_294_967_295)) {
            return 'dlqattempts';
        }

        return null;
    }

    // ─── DLQ ──────────────────────────────────────────────────────────────────

    /**
     * Prepara un evento para la DLQ: el CloudEvent original íntegro más las extensiones
     * `dlq*`, que van ANTES de `data`. Sin wrapper — así el replay es `del(.dlq*)` y
     * republicar en el subject original (04-errors.md §3).
     *
     * `$attempts` es el número de entrega en que el evento murió, NO una propiedad de su
     * clase: un PERMANENT lanzado en la 3ª entrega registra 3.
     */
    public static function toDlqEvent(
        FluxEvent $event,
        ErrorClass $reason,
        int $attempts,
        string $consumer,
        string $error,
        ?string $dlqtime = null,
    ): FluxEvent {
        return $event->withDlq(
            $reason->value,
            $attempts,
            $consumer,
            // 1024 BYTES UTF-8 sin partir un carácter (04-errors.md §3).
            Text::truncateUtf8Bytes($error, self::MAX_DLQ_ERROR_BYTES),
            // El instante de la muerte, si no es «ahora» (lo fija el arnés de conformidad): se normaliza a la
            // forma canónica o se rechaza.
            $dlqtime !== null ? self::coerceTime($dlqtime) : self::now(),
        );
    }

    /** Quita las extensiones `dlq*`. El `id` original se conserva — 04-errors.md §4.1. */
    public static function stripDlqExtensions(FluxEvent $event): FluxEvent
    {
        return $event->withoutDlq();
    }

    // ─── Firma ────────────────────────────────────────────────────────────────

    /**
     * Los bytes que se firman: el evento **sin `signature` y sin las extensiones `dlq*`**
     * — 07-signing.md §5.
     *
     * - `signkeyid` **SÍ** entra. Si quedara fuera, un atacante podría cambiarlo para que
     *   la verificación buscara otra clave.
     * - `signature` no puede firmarse a sí misma.
     * - Las `dlq*` se añaden **después** de firmar, así que quitarlas es exactamente lo que
     *   hace el replay — por eso un evento reproducido conserva su firma válida (§5.1).
     *
     * **No hay una canonicalización aparte para firmar: es el mismo `serialize()` que usa
     * el productor.** Funciona porque 01-envelope.md §1.1 (UTF-8 literal), §2.2 (`time`
     * con 3 decimales) y §6 (orden de claves, `data` al final) fijan una única
     * representación en bytes. Las tres se escribieron para otros problemas y juntas
     * resultaron ser justo lo que la firma necesitaba.
     *
     * @throws EnvelopeException
     */
    public static function signablePayload(FluxEvent $event): string
    {
        return self::serialize($event->withoutDlq()->withoutSignature());
    }

    // ─── Utilidades ───────────────────────────────────────────────────────────

    /** Representación segura de un valor cualquiera para un mensaje de error. */
    private static function describe(mixed $value): string
    {
        $json = json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

        return $json !== false ? $json : get_debug_type($value);
    }
}
