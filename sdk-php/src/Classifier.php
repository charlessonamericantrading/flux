<?php

declare(strict_types=1);

namespace Flux;

/**
 * Clasificación de errores del handler.
 * Contrato normativo: specification/04-errors.md §2
 *
 * Este fichero es el punto donde el protocolo se encuentra con la realidad operativa del
 * ecosistema. Todo lo demás en el SDK es mecánica; esto es política.
 *
 * ⚠️ Regla dura de 04-errors.md §1.1: la clasificación se decide por **semántica**, con el
 * mecanismo idiomático de la plataforma, y **NUNCA** haciendo `str_contains()` sobre el
 * mensaje de la excepción. Un mensaje es texto para humanos: cambia con la versión de la
 * librería, con el locale y con el sistema operativo. Un port literal de la lista de
 * códigos de libuv de Node produjo un bug real — en Windows el mismo corte de red se
 * clasificaba PERMANENT y en Linux RETRYABLE.
 *
 * En PHP los mecanismos idiomáticos son cuatro, y este clasificador usa los cuatro:
 *
 * 1. **Tipos de excepción**, empezando por los marcadores estándar de PSR-18
 *    (`NetworkExceptionInterface` significa literalmente "no se pudo hablar con el
 *    servidor"; `RequestExceptionInterface`, "la petición nunca va a ser válida").
 * 2. **Status HTTP**, leído de los accesores de los clientes reales.
 * 3. **SQLSTATE** de `PDOException`: la clase `08` es "connection exception" y la `40`
 *    "transaction rollback" (deadlock, fallo de serialización). Es el equivalente exacto
 *    y portable de "contención en base de datos" y "red no disponible" del protocolo.
 * 4. **Errno de socket** (`SOCKET_ECONNRESET`…), comparado por NOMBRE de constante y no
 *    por valor: en Windows valen 10054 y en Linux 104, y la constante existe con el mismo
 *    nombre en ambos.
 */
final class Classifier
{
    /**
     * Status HTTP que merecen reintento. Nótese qué NO está aquí: 400, 403, 404 y 422 son
     * PERMANENT — reintentarlos es gastar 51 minutos para obtener la misma respuesta.
     */
    public const RETRYABLE_HTTP_STATUS = [429, 502, 503, 504];

    /**
     * Marcadores semánticos de "fallo del entorno". Se comprueban con `instanceof` contra
     * el FQCN como cadena: si la librería no está instalada, la comprobación es `false` y
     * no hace falta un `class_exists` por cada una.
     */
    public const DEFAULT_TRANSIENT_EXCEPTIONS = [
        // PSR-18: el marcador estándar. "The request cannot be completed because of
        // network issues." No hay señal más semántica que esta en todo PHP.
        'Psr\\Http\\Client\\NetworkExceptionInterface',
        'GuzzleHttp\\Exception\\ConnectException',
        'Symfony\\Component\\HttpClient\\Exception\\TransportException',
        'Symfony\\Contracts\\HttpClient\\Exception\\TransportExceptionInterface',
        'Amp\\Socket\\SocketException',
        'React\\Socket\\ConnectionException',
    ];

    /**
     * Marcadores semánticos de "esto no mejora esperando". Un `RequestExceptionInterface`
     * de PSR-18 es una petición que nunca fue válida: reintentarla es determinista.
     */
    public const DEFAULT_PERMANENT_EXCEPTIONS = [
        'Psr\\Http\\Client\\RequestExceptionInterface',
    ];

    /**
     * Códigos de socket inequívocamente transitorios, por NOMBRE POSIX.
     *
     * Se conservan los nombres de Node y Python para que la taxonomía sea idéntica en los
     * seis SDKs: un `ECONNRESET` debe producir el mismo `code` en las métricas venga de
     * donde venga. `EAI_AGAIN` no tiene constante en PHP y se omite; el fallo temporal de
     * DNS se cubre por tipo de excepción del cliente HTTP.
     */
    public const TRANSIENT_SOCKET_CODES = [
        'ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE', 'EHOSTUNREACH', 'ENETUNREACH',
    ];

    /**
     * Clases SQLSTATE transitorias — el mecanismo portable de PDO.
     *
     * `08` connection exception · `40` transaction rollback (deadlock, serialization
     * failure) · `53` insufficient resources (pool/conexiones agotadas) · `57` operator
     * intervention (`57P03` = "the database system is starting up", literalmente
     * "dependencia arrancando" de 04-errors.md §1.1).
     */
    public const TRANSIENT_SQLSTATE_CLASSES = ['08', '40', '53', '57'];

    private readonly ErrorClass $unknownClass;
    private readonly ?int $unknownBudget;
    private readonly ErrorClass $timeoutClass;
    private readonly ClassifierOptions $options;

    public function __construct(?ClassifierOptions $options = null)
    {
        $options ??= new ClassifierOptions();
        $this->options = $options;

        $this->unknownClass = $options->unknownErrorPolicy === 'permanent'
            ? ErrorClass::Permanent
            : ErrorClass::Retryable;

        // Solo la política acotada impone un tope propio; las otras dos dejan mandar al
        // `max_deliver` del consumidor — 04-errors.md §2.1.
        $this->unknownBudget = $options->unknownErrorPolicy === 'retryable-bounded'
            ? $options->unknownRetryBudget
            : null;

        $this->timeoutClass = $options->timeoutPolicy === 'permanent'
            ? ErrorClass::Permanent
            : ErrorClass::Retryable;
    }

    /**
     * Traduce un error cualquiera a una de las tres clases. El runtime lo usa así:
     *
     *     RETRYABLE  → nak(retryAfterMs ?? backoff canónico)
     *     PERMANENT  → term() + publicar en dlq.<subject> con dlqattempts = intento actual
     *     POISON     → term() + publicar en dlq.<subject> + alerta inmediata
     *
     * El orden de evaluación es deliberado: lo más específico primero, y el default al
     * final. Esa última línea es la decisión de política de verdad.
     */
    public function classify(\Throwable $e): Classification
    {
        // 1. Una excepción tipada de flux siempre gana: la aplicación sabe más que el SDK.
        if ($e instanceof FluxError) {
            return new Classification(
                errorClass: $e->fluxClass(),
                code: $e->errorCode ?? self::shortName($e),
                retryAfterMs: $e instanceof RetryableError ? $e->retryAfterMs : null,
            );
        }

        // 2. Reglas de la aplicación: solo ella conoce sus dependencias — 04-errors.md §2.
        foreach ($this->options->rules as $rule) {
            $result = $rule($e);
            if ($result instanceof Classification) {
                return $result;
            }
        }

        // 3. Status HTTP: la señal más fiable que da una dependencia.
        $status = self::extractHttpStatus($e);
        if ($status !== null) {
            $retryable = in_array($status, self::RETRYABLE_HTTP_STATUS, true);

            return new Classification(
                errorClass: $retryable ? ErrorClass::Retryable : ErrorClass::Permanent,
                code: "HTTP_{$status}",
                retryAfterMs: $retryable ? self::extractRetryAfterMs($e) : null,
            );
        }

        // 4. Marcadores semánticos por tipo. Los permanentes van primero porque una misma
        //    jerarquía puede implementar los dos interfaces de PSR-18 y "la petición nunca
        //    fue válida" es la información más específica de las dos.
        if (self::instanceOfAny($e, $this->options->permanentExceptions)) {
            return new Classification(ErrorClass::Permanent, self::shortName($e));
        }
        if (self::instanceOfAny($e, $this->options->transientExceptions)) {
            return new Classification(ErrorClass::Retryable, self::shortName($e));
        }

        // 5. Errores de socket, por nombre de constante.
        $socket = self::extractSocketCode($e);
        if ($socket !== null && in_array($socket, self::TRANSIENT_SOCKET_CODES, true)) {
            return new Classification(ErrorClass::Retryable, $socket);
        }

        // 6. Base de datos: contención y conexión son transitorias por definición.
        $sqlstate = self::extractSqlState($e);
        if ($sqlstate !== null) {
            $isTransient = in_array(substr($sqlstate, 0, 2), self::TRANSIENT_SQLSTATE_CLASSES, true);

            return new Classification(
                errorClass: $isTransient ? ErrorClass::Retryable : ErrorClass::Permanent,
                code: "SQLSTATE_{$sqlstate}",
            );
        }

        // 7. Timeouts — política configurable.
        if (self::isTimeout($e)) {
            return new Classification($this->timeoutClass, 'TIMEOUT');
        }

        // 8. Lo desconocido. Aquí se decide el comportamiento del ecosistema ante lo que
        //    nadie previó. El default acotado da al transitorio una segunda oportunidad sin
        //    regalarle 51 minutos de cola al sistemático — y el tope va en la clasificación,
        //    no en `max_deliver`, para no recortar los reintentos de los RETRYABLE
        //    reconocidos — 04-errors.md §2.1.
        return new Classification(
            errorClass: $this->unknownClass,
            code: $socket ?? 'UNKNOWN',
            maxAttempts: $this->unknownBudget,
        );
    }

    // ─── Extractores ──────────────────────────────────────────────────────────

    /**
     * Busca el status HTTP donde lo dejan los clientes reales: Guzzle y PSR-18 lo cuelgan
     * de `getResponse()->getStatusCode()`, Symfony HttpKernel y Laravel exponen
     * `getStatusCode()` directamente, y varios SDK lo dejan en una propiedad pública.
     *
     * ⚠️ Deliberadamente NO se usa `getCode()`: en Guzzle resulta ser el status, pero en
     * el 90 % de las excepciones de PHP es un cero o un código de driver, y tomarlo por un
     * status HTTP clasificaría mal en silencio.
     */
    public static function extractHttpStatus(\Throwable $e): ?int
    {
        if (method_exists($e, 'getResponse')) {
            $response = $e->getResponse();
            if (is_object($response) && method_exists($response, 'getStatusCode')) {
                $status = $response->getStatusCode();
                if (is_int($status) && $status >= 100 && $status < 600) {
                    return $status;
                }
            }
        }

        if (method_exists($e, 'getStatusCode')) {
            $status = $e->getStatusCode();
            if (is_int($status) && $status >= 100 && $status < 600) {
                return $status;
            }
        }

        foreach (['status', 'statusCode'] as $property) {
            if (isset($e->{$property}) && is_int($e->{$property})) {
                $status = $e->{$property};
                if ($status >= 100 && $status < 600) {
                    return $status;
                }
            }
        }

        return null;
    }

    /**
     * Lee `Retry-After` (en segundos) si la dependencia lo expone.
     *
     * Su forma de fecha HTTP se ignora a propósito: interpretarla mal daría un retraso
     * absurdo, y el backoff canónico es una respuesta segura.
     */
    public static function extractRetryAfterMs(\Throwable $e): ?float
    {
        if (!method_exists($e, 'getResponse')) {
            return null;
        }

        $response = $e->getResponse();
        if (!is_object($response) || !method_exists($response, 'getHeaderLine')) {
            return null;
        }

        $raw = $response->getHeaderLine('Retry-After');
        if (!is_string($raw) || !preg_match('/^\d+(\.\d+)?$/', $raw)) {
            return null;
        }

        $seconds = (float) $raw;

        return $seconds > 0 ? $seconds * 1000 : null;
    }

    /**
     * SQLSTATE de un error de base de datos, si lo hay.
     *
     * Se busca por las dos convenciones reales del ecosistema y no por `instanceof
     * \PDOException`: PDO deja el estado en `errorInfo[0]` (y en `getCode()`), y Doctrine
     * DBAL lo expone como `getSQLState()`. Comprobar el tipo concreto obligaría además a
     * que `ext-pdo` estuviese cargada para que este SDK funcione, que no viene a cuento.
     *
     * Un `HY000` genérico no dice nada útil, así que se descarta y el error cae en la
     * política de lo desconocido — fingir que es permanente sería peor que admitir que no
     * se sabe.
     */
    public static function extractSqlState(\Throwable $e): ?string
    {
        $state = null;

        if (method_exists($e, 'getSQLState')) {
            $value = $e->getSQLState();
            $state = is_string($value) ? $value : null;
        }

        if ($state === null && isset($e->errorInfo) && is_array($e->errorInfo)) {
            $state = isset($e->errorInfo[0]) && is_string($e->errorInfo[0]) ? $e->errorInfo[0] : null;
        }

        if ($state === null && $e instanceof \PDOException && is_string($e->getCode())) {
            $state = $e->getCode();
        }

        if ($state === null || strlen($state) !== 5 || $state === 'HY000') {
            return null;
        }

        return $state;
    }

    /**
     * Nombre POSIX del error de socket, si la excepción lo expone.
     *
     * Se compara por NOMBRE de constante y no por valor: en Windows `SOCKET_ECONNRESET`
     * vale 10054 (WSAECONNRESET) y en Linux 104. Comparar valores haría que el mismo corte
     * de red fuese RETRYABLE en un sistema operativo y desconocido en el otro — exactamente
     * el bug que 04-errors.md §1.1 documenta.
     */
    public static function extractSocketCode(\Throwable $e): ?string
    {
        $errno = null;
        foreach (['getErrno', 'getSocketErrno'] as $method) {
            if (method_exists($e, $method)) {
                $value = $e->{$method}();
                if (is_int($value)) {
                    $errno = $value;
                    break;
                }
            }
        }
        if ($errno === null) {
            foreach (['errno', 'socketErrno'] as $property) {
                if (isset($e->{$property}) && is_int($e->{$property})) {
                    $errno = $e->{$property};
                    break;
                }
            }
        }

        return $errno !== null ? self::socketErrorName($errno) : null;
    }

    /**
     * `104` (Linux) o `10054` (Windows) → `"ECONNRESET"`.
     *
     * Pública porque es útil desde la aplicación: si usas `ext-sockets` directamente,
     * `throw new RetryableError($msg, Classifier::socketErrorName(socket_last_error($s)))`
     * mete el error en la taxonomía con el mismo código que emitirían Node y Python.
     */
    public static function socketErrorName(int $errno): ?string
    {
        foreach (self::TRANSIENT_SOCKET_CODES as $name) {
            $constant = 'SOCKET_' . $name;
            if (defined($constant) && constant($constant) === $errno) {
                return $name;
            }
        }

        return null;
    }

    /**
     * ¿Es un timeout?
     *
     * PHP no tiene una excepción de timeout estándar —cada librería trae la suya sin
     * jerarquía común—, así que se mira el **nombre corto de la clase**, igual que hace el
     * SDK de Python. Que quede claro por qué esto no viola la regla de §1.1: un nombre de
     * clase es API pública y estable; un mensaje de error no lo es ni pretende serlo.
     */
    public static function isTimeout(\Throwable $e): bool
    {
        return str_contains(strtolower(self::shortName($e)), 'timeout');
    }

    /** @param list<class-string|string> $classes */
    private static function instanceOfAny(\Throwable $e, array $classes): bool
    {
        foreach ($classes as $class) {
            if ($e instanceof $class) {
                return true;
            }
        }

        return false;
    }

    private static function shortName(\Throwable $e): string
    {
        $class = $e::class;
        $position = strrpos($class, '\\');

        return $position === false ? $class : substr($class, $position + 1);
    }
}
