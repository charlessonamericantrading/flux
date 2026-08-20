<?php

declare(strict_types=1);

namespace Flux;

/**
 * Un evento de flux — CloudEvents 1.0, perfil flux.
 * Contrato normativo: specification/01-envelope.md
 *
 * Los nombres de las extensiones van en minúscula pegada (`correlationid`, no
 * `correlationId`) porque CloudEvents restringe los nombres de extensión a `[a-z0-9]`
 * sin separadores. No es estilo: es la especificación (01-envelope.md §3). Por eso son
 * la única excepción al `camelCase` del resto del SDK — renombrarlas rompería el
 * envelope en el cable.
 *
 * Es `readonly` por lo mismo que en Node es `readonly` y en Python `frozen`: un evento
 * es un hecho consumado del pasado. Para derivar uno modificado están `withDlq()` y
 * `withoutDlq()`.
 */
final readonly class FluxEvent
{
    /**
     * @param array<string,mixed> $data Objeto JSON en la raíz — 01-envelope.md §4.
     */
    public function __construct(
        public string $id,
        public string $source,
        public string $type,
        public string $time,
        public string $dataschema,
        public string $correlationid,
        public string $tenantid,
        public string $producerversion,
        /** `public` | `internal` | `confidential` | `restricted`. */
        public string $dataclassification,
        public array $data,
        public string $specversion = Protocol::SPEC_VERSION,
        public string $datacontenttype = 'application/json',
        /**
         * ⚠️ Id del AGREGADO (`"ped-123"`), NO el subject de NATS.
         * Son dos cosas distintas con el mismo nombre — 01-envelope.md §2.1.
         */
        public ?string $subject = null,
        public ?string $causationid = null,
        public ?string $partitionkey = null,
        public ?string $traceparent = null,
        public ?string $tracestate = null,
        /** `retryable` | `permanent` | `poison`. Solo presente en la DLQ. */
        public ?string $dlqreason = null,
        public ?int $dlqattempts = null,
        public ?string $dlqconsumer = null,
        public ?string $dlqerror = null,
        public ?string $dlqtime = null,
    ) {
    }

    /**
     * Vuelca el evento a un array listo para `json_encode`.
     *
     * **El orden de inserción es normativo** (01-envelope.md §6): en PHP el orden de las
     * claves de un array determina el orden de salida de `json_encode`, así que este
     * método es el único sitio donde se decide la secuencia de bytes del ecosistema.
     * `data` va SIEMPRE el último, y las extensiones `dlq*` van ANTES de `data`.
     *
     * Esa regla nació de una divergencia real: el SDK de Node construía el evento de DLQ
     * con `{...event, dlq*}`, dejando las extensiones DESPUÉS de `data`, mientras Python,
     * Go y Java las ponían antes. El mismo evento, dos secuencias de bytes — y flux
     * depende de los bytes en cuatro sitios: replay verbatim desde la DLQ, firma
     * criptográfica futura, deduplicación por hash de contenido y fixtures compartidos.
     *
     * Los atributos ausentes se **omiten**, no se emiten como `null`: ausente ≠ vacío
     * (01-envelope.md §3.3). Un `causationid: null` en el cable obligaría a cada
     * consumidor a distinguir los dos casos.
     *
     * @return array<string,mixed>
     */
    public function toArray(): array
    {
        $out = [
            'specversion'     => $this->specversion,
            'id'              => $this->id,
            'source'          => $this->source,
            'type'            => $this->type,
            'time'            => $this->time,
            'datacontenttype' => $this->datacontenttype,
            'dataschema'      => $this->dataschema,
        ];

        if ($this->subject !== null) {
            $out['subject'] = $this->subject;
        }

        $out['correlationid']      = $this->correlationid;
        $out['tenantid']           = $this->tenantid;
        $out['producerversion']    = $this->producerversion;
        $out['dataclassification'] = $this->dataclassification;

        foreach ([
            'causationid' => $this->causationid,
            'partitionkey' => $this->partitionkey,
            'traceparent' => $this->traceparent,
            'tracestate' => $this->tracestate,
            'dlqreason' => $this->dlqreason,
            'dlqattempts' => $this->dlqattempts,
            'dlqconsumer' => $this->dlqconsumer,
            'dlqerror' => $this->dlqerror,
            'dlqtime' => $this->dlqtime,
        ] as $name => $value) {
            if ($value !== null) {
                $out[$name] = $value;
            }
        }

        // Último, siempre. Ver el bloque de arriba.
        $out['data'] = $this->data;

        return $out;
    }

    /**
     * Copia con las extensiones `dlq*` puestas. El CloudEvent original queda íntegro:
     * sin wrapper, para que el replay sea `del(.dlq*)` y republicar — 04-errors.md §3.
     */
    public function withDlq(
        string $reason,
        int $attempts,
        string $consumer,
        string $error,
        string $time,
    ): self {
        return $this->copy([
            'dlqreason'   => $reason,
            'dlqattempts' => $attempts,
            'dlqconsumer' => $consumer,
            'dlqerror'    => $error,
            'dlqtime'     => $time,
        ]);
    }

    /** Quita las extensiones `dlq*`. El `id` original se conserva — 04-errors.md §4.1. */
    public function withoutDlq(): self
    {
        return $this->copy([
            'dlqreason'   => null,
            'dlqattempts' => null,
            'dlqconsumer' => null,
            'dlqerror'    => null,
            'dlqtime'     => null,
        ]);
    }

    /**
     * Equivalente a `dataclasses.replace` de Python. PHP no tiene `with`-ers nativos,
     * así que se reconstruye el objeto entero con argumentos con nombre.
     *
     * @param array<string,mixed> $overrides
     */
    private function copy(array $overrides): self
    {
        $attributes = [
            'id'                 => $this->id,
            'source'             => $this->source,
            'type'               => $this->type,
            'time'               => $this->time,
            'dataschema'         => $this->dataschema,
            'correlationid'      => $this->correlationid,
            'tenantid'           => $this->tenantid,
            'producerversion'    => $this->producerversion,
            'dataclassification' => $this->dataclassification,
            'data'               => $this->data,
            'specversion'        => $this->specversion,
            'datacontenttype'    => $this->datacontenttype,
            'subject'            => $this->subject,
            'causationid'        => $this->causationid,
            'partitionkey'       => $this->partitionkey,
            'traceparent'        => $this->traceparent,
            'tracestate'         => $this->tracestate,
            'dlqreason'          => $this->dlqreason,
            'dlqattempts'        => $this->dlqattempts,
            'dlqconsumer'        => $this->dlqconsumer,
            'dlqerror'           => $this->dlqerror,
            'dlqtime'            => $this->dlqtime,
        ];

        return new self(...array_merge($attributes, $overrides));
    }
}
