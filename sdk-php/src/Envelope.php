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
     * ⚠️ Las tres banderas son obligatorias, no cosméticas.
     *
     * - `JSON_UNESCAPED_UNICODE`: sin ella `json_encode` escapa TODO lo no-ASCII
     *   (`é` → `é`). 01-envelope.md §1.1 exige UTF-8 con los no-ASCII **literales**.
     *   Ambas formas son JSON válido y se parsean al mismo string, pero no son los mismos
     *   BYTES — y flux depende de los bytes para el replay verbatim, la firma futura, la
     *   deduplicación por hash y los fixtures compartidos entre SDKs.
     * - `JSON_UNESCAPED_SLASHES`: sin ella `/` → `\/`, así que un `dataschema` con URL
     *   (que los tiene todos) diferiría byte a byte de los otros cinco SDKs. Es el mismo
     *   problema que `JavaScriptEncoder.UnsafeRelaxedJsonEscaping` resuelve en .NET.
     * - `JSON_PRESERVE_ZERO_FRACTION`: sin ella un `float(4995.0)` sale como `4995` y un
     *   importe decimal cambiaría de tipo al reserializar. Con ella sale `4995.0`, que es
     *   lo que emite `json.dumps` de Python. Ver la nota sobre fidelidad numérica en
     *   `parse()`.
     * - `JSON_THROW_ON_ERROR`: `INF`/`NAN` fallan en vez de producir `0`, equivalente al
     *   `allow_nan=False` del SDK de Python.
     */
    public const JSON_FLAGS = JSON_UNESCAPED_UNICODE
        | JSON_UNESCAPED_SLASHES
        | JSON_PRESERVE_ZERO_FRACTION
        | JSON_THROW_ON_ERROR;

    /** Un stacktrace entero cabría, pero se comería el presupuesto de 1 MiB del evento. */
    private const MAX_DLQ_ERROR_BYTES = 1024;

    private function __construct()
    {
    }

    // ─── Tiempo ───────────────────────────────────────────────────────────────

    /**
     * RFC 3339 en UTC con **exactamente 3 decimales y sufijo `Z`** — 01-envelope.md §2.2.
     *
     * ⚠️ Ni `DateTimeInterface::ATOM` ni `format('c')` valen: no emiten milisegundos y
     * ponen `+00:00` en lugar de `Z`. "Precisión de milisegundos" tampoco basta como
     * regla, porque los formateadores por defecto de cada lenguaje producen cosas
     * distintas y todas válidas según RFC 3339 (Go recorta ceros, Python da
     * microsegundos y offset). El resultado sería que dos servicios del mismo ecosistema
     * emiten `time` distintos para el mismo instante.
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
        // `new DateTimeImmutable('now')` incluye microsegundos desde PHP 7.1; `time()` no
        // los tendría y todos los eventos saldrían con `.000Z`.
        return self::formatTime(new \DateTimeImmutable('now', new \DateTimeZone('UTC')));
    }

    /**
     * Acepta un `DateTimeInterface` o una cadena ya formateada, y **valida la cadena**.
     *
     * Diferencia deliberada con Node y Python, que dejan pasar cualquier string: aquí un
     * `time` con formato incorrecto se rechaza en `publish()`. Es más barato fallar en el
     * productor que descubrir en el replay que un servicio lleva meses emitiendo
     * `…39.41Z`.
     *
     * @throws EnvelopeException
     */
    public static function coerceTime(\DateTimeInterface|string $when): string
    {
        if ($when instanceof \DateTimeInterface) {
            return self::formatTime($when);
        }

        if (preg_match('/' . Protocol::TIME_PATTERN . '/', $when) !== 1) {
            throw new EnvelopeException(
                "`time` inválido: \"{$when}\". Debe ser RFC 3339 en UTC con exactamente 3 "
                . 'decimales y sufijo Z (2026-08-20T10:25:39.412Z) — 01-envelope.md §2.2. '
                . 'Pasa un DateTimeInterface y el SDK lo formatea.'
            );
        }

        return $when;
    }

    // ─── Construcción ─────────────────────────────────────────────────────────

    /**
     * Monta el CloudEvent.
     *
     * Todos los parámetros se pasan con nombre a propósito (`FluxBus` lo hace así):
     * `source`, `id` y `correlationid` son tres cadenas seguidas y posicionalmente son
     * indistinguibles.
     *
     * @param array<string,mixed> $data
     *
     * @throws EnvelopeException|InvalidSubjectException
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
        // Un array o escalar en la raíz impide añadir campos después sin romper el
        // esquema — 01-envelope.md §4. `[]` se acepta porque en PHP es a la vez la lista
        // vacía y el objeto vacío, y el objeto vacío sí es legal (ver `serialize`).
        if ($data !== [] && array_is_list($data)) {
            throw new EnvelopeException(
                '`data` debe ser un objeto JSON en la raíz (ni array ni escalar), para poder '
                . 'añadir campos sin romper el contrato — 01-envelope.md §4'
            );
        }

        if (!in_array($dataclassification, self::VALID_CLASSIFICATIONS, true)) {
            // Fallar aquí y no en el consumidor: publicar con una clasificación inventada
            // produciría un POISON en cada consumidor del subject, y el productor no se
            // enteraría nunca.
            throw new EnvelopeException(
                "dataclassification inválido: \"{$dataclassification}\". Valores permitidos: "
                . implode(', ', self::VALID_CLASSIFICATIONS)
            );
        }

        return new FluxEvent(
            id: $id,
            source: $source,
            type: Protocol::subjectToType($subject),
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
     * Serializa el evento y aplica el límite de 1 MiB — 01-envelope.md §7.
     *
     * @throws EnvelopeException
     */
    public static function serialize(FluxEvent $event): string
    {
        $payload = $event->toArray();

        // `data` es un objeto JSON en la raíz, pero en PHP `[]` es indistinguible de la
        // lista vacía y `json_encode([])` daría `[]`. En la raíz de `data` la ambigüedad
        // se resuelve por contrato (§4: DEBE ser objeto), así que se fuerza `{}`.
        // Los objetos vacíos ANIDADOS siguen siendo ambiguos: usa `new \stdClass` si
        // necesitas uno. Está documentado en el README.
        if ($payload['data'] === []) {
            $payload['data'] = new \stdClass();
        }

        try {
            $json = json_encode($payload, self::JSON_FLAGS);
        } catch (\JsonException $e) {
            throw new EnvelopeException(
                'el evento no es serializable a JSON: ' . $e->getMessage()
                . ' (¿INF, NAN o una cadena que no es UTF-8 válido en `data`?)',
                previous: $e
            );
        }

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
     * Un fallo aquí es POISON: el mensaje ni siquiera es interpretable, así que nunca
     * llega al handler — 04-errors.md §1.3.
     *
     * Sobre fidelidad numérica (01-envelope.md §2.5): `json_decode` convierte los números
     * de `data` a `int`/`float` de PHP, así que un `4995.00` vuelve a salir como `4995.0`
     * — igual que en el SDK de Python, y a diferencia de Go y Java, que conservan los
     * bytes crudos. La forma de no tener el problema es la que recomienda la propia spec:
     * no metas decimales en `data`; los importes van como entero en la unidad mínima.
     *
     * @throws PoisonError
     */
    public static function parse(string $payload): FluxEvent
    {
        try {
            /** @var mixed $raw */
            $raw = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new PoisonError(
                'el cuerpo del mensaje no es JSON válido',
                'MALFORMED_JSON',
                $e
            );
        }

        // `json_decode(..., true)` colapsa `{}` y `[]` en el mismo `array` vacío de PHP.
        // El primer carácter no blanco de un JSON válido distingue los dos sin volver a
        // decodificar el cuerpo entero, que en un mensaje de 1 MiB no es gratis.
        $firstChar = substr(ltrim($payload), 0, 1);
        if (!is_array($raw) || $firstChar !== '{') {
            throw new PoisonError(
                'el cuerpo del mensaje no es un objeto JSON',
                'NOT_AN_OBJECT'
            );
        }

        if (($raw['specversion'] ?? null) !== Protocol::SPEC_VERSION) {
            throw new PoisonError(
                'specversion no soportada: ' . self::describe($raw['specversion'] ?? null)
                . ' (se esperaba "' . Protocol::SPEC_VERSION . '")',
                'UNSUPPORTED_SPECVERSION'
            );
        }

        // `null` cuenta como ausente: 01-envelope.md §3.3 prohíbe darle significado propio
        // a un valor vacío, así que un atributo obligatorio a `null` está igual de mal que
        // no estar.
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

        // Ausente, `null` y `""` son lo mismo aquí, y se evalúa ANTES que el enum: la
        // tabla de 01-envelope.md §3.1 fija que `"dataclassification": ""` es
        // MISSING_REQUIRED_EXTENSION y no INVALID_DATACLASSIFICATION. Si dos SDKs emiten
        // códigos distintos ante la misma entrada, agrupar por causa deja de funcionar.
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
        // El `is_string` va delante y no sobra: `in_array(['a' => 1], [...], true)` no es
        // un error en PHP pero sí lo sería comparar un array con `===` contra cadenas en
        // otros lenguajes. Aquí además evita que un array acabe interpolado en el mensaje.
        if (!is_string($classification) || !in_array($classification, self::VALID_CLASSIFICATIONS, true)) {
            // Un valor fuera del enum no se puede degradar a algo seguro: la retención y
            // las ACLs de 06-security.md §5 se derivan de él, así que inventarle un
            // sentido es peor que rechazar el mensaje.
            throw new PoisonError(
                'dataclassification inválido: ' . self::describe($classification)
                . '. Valores permitidos: ' . implode(', ', self::VALID_CLASSIFICATIONS),
                'INVALID_DATACLASSIFICATION'
            );
        }

        // Los tipos son exactos: `{"tenantid": 42}` es POISON, no el tenant "42".
        // En PHP hay que comprobarlo a mano — el lenguaje coacciona con entusiasmo y
        // `$evento->tenantid == 42` sería `true` para el tenant "42" y también para el
        // entero 42. Jackson coacciona, Go rechaza, Node lo propagaba como número: el
        // mismo mensaje, tres comportamientos, y el que "funciona" es el peor
        // — 01-envelope.md §2.4.
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
            // Estricto a propósito: permitir atributos raíz ad-hoc lleva a serializar JSON
            // dentro de un string y el envelope deja de ser interoperable. Las extensiones
            // de CloudEvents solo admiten escalares — 01-envelope.md §3.4.
            throw new PoisonError(
                'atributos raíz desconocidos: ' . implode(', ', $unknown)
                . '. Todo metadato adicional va dentro de `data`',
                'UNKNOWN_ROOT_ATTRIBUTE'
            );
        }

        // Simétrico con `build()`: `data` DEBE ser un objeto, ni array ni escalar
        // (01-envelope.md §4). Comprobar solo `is_array` no bastaría porque en PHP una
        // lista JSON TAMBIÉN es un array, y un `{"data":[1,2,3]}` de un productor ajeno
        // llegaría al handler como lista en vez de ser POISON. `[]` sí se acepta: es el
        // objeto vacío colapsado por `json_decode` (ver arriba).
        if (!is_array($raw['data']) || ($raw['data'] !== [] && array_is_list($raw['data']))) {
            throw new PoisonError(
                '`data` debe ser un objeto JSON en la raíz (ni array ni escalar), llegó '
                . (is_array($raw['data']) ? 'un array JSON' : get_debug_type($raw['data'])),
                'WRONG_ATTRIBUTE_TYPE'
            );
        }

        try {
            // El desempaquetado con claves de texto son argumentos con nombre (PHP 8.1+),
            // que es lo más parecido al `FluxEvent(**raw)` del SDK de Python. Los nombres
            // del constructor son exactamente los del envelope, así que no hay mapeo que
            // desincronizar.
            return new FluxEvent(...$raw);
        } catch (\TypeError|\ArgumentCountError $e) {
            // Solo puede pasar con los atributos que NO están en STRING_ATTRIBUTES:
            // `dlqattempts` como cadena, `subject` como número… Con `strict_types=1` PHP
            // lanza TypeError en vez de coaccionar, y sin este catch el consumidor se
            // caería en vez de mandar el mensaje a la DLQ.
            throw new PoisonError(
                'un atributo del envelope tiene un tipo incorrecto: ' . $e->getMessage(),
                'WRONG_ATTRIBUTE_TYPE',
                $e
            );
        }
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
    ): FluxEvent {
        return $event->withDlq(
            $reason->value,
            $attempts,
            $consumer,
            self::truncateUtf8($error, self::MAX_DLQ_ERROR_BYTES),
            self::now(),
        );
    }

    /** Quita las extensiones `dlq*`. El `id` original se conserva — 04-errors.md §4.1. */
    public static function stripDlqExtensions(FluxEvent $event): FluxEvent
    {
        return $event->withoutDlq();
    }

    // ─── Utilidades ───────────────────────────────────────────────────────────

    /**
     * Recorta a N BYTES sin partir un carácter UTF-8 por la mitad.
     *
     * Node y Python recortan por CARACTERES (`slice(0, 1024)`, `[:1024]`). Aquí se recorta
     * por bytes porque el presupuesto que importa es el del mensaje de 1 MiB, y porque
     * `mb_substr` obligaría a depender de `ext-mbstring`. Para errores ASCII —la inmensa
     * mayoría— el resultado es idéntico.
     */
    private static function truncateUtf8(string $value, int $maxBytes): string
    {
        if (strlen($value) <= $maxBytes) {
            return $value;
        }

        $cut = substr($value, 0, $maxBytes);

        // Un corte a mitad de secuencia multibyte dejaría bytes que `json_encode` rechaza
        // con "Malformed UTF-8 characters", y el evento de DLQ no llegaría a escribirse
        // — justo el caso en que más falta hace. `preg_match('//u', …)` falla si la cadena
        // no es UTF-8 válido, así que se sueltan bytes hasta que lo sea (máximo 3).
        while ($cut !== '' && @preg_match('//u', $cut) !== 1) {
            $cut = substr($cut, 0, -1);
        }

        return $cut;
    }

    /** Representación segura de un valor cualquiera para un mensaje de error. */
    private static function describe(mixed $value): string
    {
        $json = json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

        return $json !== false ? $json : get_debug_type($value);
    }
}
