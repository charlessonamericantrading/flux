<?php

declare(strict_types=1);

namespace Flux;

/**
 * Propagación automática de contexto entre eventos.
 * Contrato normativo: specification/01-envelope.md §5
 *
 * Si un desarrollador tiene que rellenar `correlationid` a mano, el SDK ha fallado en su
 * único trabajo. Donde el SDK de Node usa `AsyncLocalStorage` y el de Python
 * `contextvars.ContextVar`, aquí basta con una variable estática y un `try/finally`: PHP
 * ejecuta un solo flujo por proceso, así que "el evento que se está procesando ahora
 * mismo" es un concepto literal y no necesita almacenamiento por tarea.
 *
 * ⚠️ Limitación conocida: si el proceso usa **Fibers** (ReactPHP, Amp, Swoole con
 * corrutinas) para procesar varios eventos a la vez, este almacenamiento estático se
 * comparte entre fibras y el `correlationid` se cruzaría. El bucle de consumo del SDK es
 * estrictamente secuencial y no las usa; si tu handler las usa por dentro, publica desde
 * la fibra principal o pasa el contexto a mano.
 *
 * Nota de PHP: la clase NO se declara `readonly` aunque todas sus propiedades lo sean.
 * PHP 8.2 prohíbe las propiedades **estáticas** dentro de una `readonly class` (error
 * fatal de compilación), y `$current` tiene que serlo: es el almacenamiento ambiental.
 * Marcar cada propiedad como `readonly` da exactamente la misma garantía de inmutabilidad
 * para las instancias.
 */
final class EventContext
{
    public function __construct(
        public readonly string $correlationid,
        /** `id` del evento en curso — pasa a ser el `causationid` de lo que se publique. */
        public readonly string $causationid,
        public readonly string $tenantid,
        public readonly ?string $traceparent = null,
        public readonly ?string $tracestate = null,
    ) {
    }

    private static ?self $current = null;

    /** Contexto del evento que se está procesando ahora mismo, si lo hay. */
    public static function current(): ?self
    {
        return self::$current;
    }

    /**
     * Ejecuta `$fn` con `$context` activo y restaura el anterior pase lo que pase.
     * Equivalente a `runWithContext` de Node y al context manager `use_context` de Python.
     *
     * @template T
     * @param callable():T $fn
     * @return T
     */
    public static function run(self $context, callable $fn): mixed
    {
        $previous = self::$current;
        self::$current = $context;
        try {
            return $fn();
        } finally {
            // `finally` y no una línea al final: si el handler lanza —y lanzar es la forma
            // normal de señalar un error en flux— el contexto quedaría pegado al proceso y
            // el siguiente evento heredaría la correlación del anterior.
            self::$current = $previous;
        }
    }

    public static function fromEvent(FluxEvent $event): self
    {
        return new self(
            correlationid: $event->correlationid,
            // El `id` del evento entrante es la CAUSA del saliente: `correlationid`
            // responde "¿de qué flujo forma parte?" y `causationid` "¿quién lo causó
            // exactamente?" — 01-envelope.md §3.2.
            causationid: $event->id,
            tenantid: $event->tenantid,
            traceparent: $event->traceparent,
            tracestate: $event->tracestate,
        );
    }

    /**
     * Lee `traceparent` del span de OpenTelemetry activo, si lo hay.
     *
     * Se resuelve con `class_exists` y fallo silencioso a propósito: flux no depende de
     * OpenTelemetry pero lo aprovecha cuando está presente. Una dependencia dura obligaría
     * a instalarlo a todo servicio que use el SDK.
     */
    public static function activeTraceparent(): ?string
    {
        /** @var class-string|string $span */
        $span = 'OpenTelemetry\\API\\Trace\\Span';
        if (!class_exists($span)) {
            return null;
        }

        try {
            $context = $span::getCurrent()->getContext();
            if (!$context->isValid()) {
                return null;
            }

            return sprintf(
                '00-%s-%s-%02x',
                $context->getTraceId(),
                $context->getSpanId(),
                $context->getTraceFlags(),
            );
        } catch (\Throwable) {
            // Una versión de la API de OTel distinta de la esperada no debe impedir
            // publicar: el traceparent es opcional (01-envelope.md §3.2).
            return null;
        }
    }
}
