<?php

declare(strict_types=1);

namespace Flux;

use Flux\Metrics\MetricsSink;
use Flux\Signing\SigningOptions;
use Flux\Validation\ValidationOptions;
use Psr\Log\LoggerInterface;

/**
 * Configuración del cliente. Lo que el SDK necesita saber para rellenar el envelope sin
 * que el desarrollador escriba un solo atributo — 01-envelope.md §5.
 */
final readonly class ConnectOptions
{
    /**
     * @param string $service Nombre del servicio. Alimenta `source` y los nombres de
     *        durable consumer. DEBE ser kebab-case: NATS aceptaría `FacturacionAPI__…`
     *        sin error y el incumplimiento quedaría latente hasta que alguien parsease
     *        nombres en una herramienta. Se valida en `connect()`.
     * @param string $environment `produccion`, `staging`, `dev`. Alimenta `source`.
     * @param string $version SemVer del servicio. Va en `producerversion`; sin él un bug
     *        de payload en producción no se puede acotar a un despliegue.
     * @param string $tenantId Tenant por defecto de los eventos publicados. `system` para
     *        eventos de plataforma — y **solo** para eso: `"system"` NO DEBE usarse como
     *        comodín ni como default cuando el tenant real se desconoce, porque si no se
     *        sabe de quién es un evento el bug está aguas arriba (09-multitenancy.md §5).
     * @param TenantIsolation $tenantIsolation Con `Strict`, toda suscripción filtra por
     *        tenant y suscribirse sin uno configurado lanza `TenantIsolationException`
     *        — 09-multitenancy.md §3.
     * @param SigningOptions|null $signing Firma Ed25519 de eventos. Extensión **OPCIONAL**
     *        — 07-signing.md. Traslada la autenticidad del canal al evento: un evento
     *        firmado sigue siendo verificable dentro de un fichero, un backup o un correo,
     *        donde ya no hay ACL que lo respalde.
     * @param MetricsSink|null $metrics Destino de las métricas. `null` = `NoMetrics`.
     *        Los nombres y las etiquetas los fija el protocolo (08-observability.md), no la
     *        aplicación; lo que la aplicación elige es el backend.
     * @param string $classification Clasificación por defecto — 06-security.md §5.
     * @param array<string,string> $schemas Mapa exacto subject → URI de `dataschema`.
     *        Gana sobre `schemaBaseUrl`.
     * @param string|null $schemaBaseUrl Base para derivar `dataschema` cuando el subject
     *        no está en `schemas`.
     * @param bool $createStreams Crear el stream del dominio si no existe. Comodidad de
     *        DESARROLLO: en producción los provisiona IaC — 02-naming.md §3.2.
     * @param (\Closure(string,PoisonError,string):void)|null $onPoison Se invoca ante un
     *        POISON, con `(subject, error, cuerpo crudo)`. Es el único caso que DEBE
     *        despertar a alguien — 04-errors.md §1.3.
     * @param (\Closure(string,FluxEvent,Classification):void)|null $onDlq Se invoca al
     *        enrutar cualquier evento a la DLQ.
     * @param ValidationOptions|null $validation Validación L3 del payload contra su JSON
     *        Schema — 00-protocol.md §5. `null` (default) es nivel L2 y no cuesta nada.
     *        Con `ValidationMode::Strict`, publicar un payload que viola su contrato falla
     *        en el productor en vez de aparecer como un misterio en un consumidor de otro
     *        equipo la semana que viene.
     * @param int $pendingPollMs Cada cuánto preguntar al servidor por el `num_pending` de
     *        cada consumidor, en milisegundos. `0` lo desactiva.
     *
     *        No es un capricho de configuración: `flux_consumer_pending` tiene DOS fuentes
     *        obligatorias (08-observability.md §2.3) y esta es la segunda. Los metadatos de
     *        cada mensaje entregado son gratis y frescos, pero **si el bucle del consumidor
     *        muere dejan de llegar mensajes**, así que un gauge alimentado solo desde ahí se
     *        queda PLANO en su último valor en vez de crecer — una línea horizontal,
     *        indistinguible de "no pasa nada". El sondeo sigue corriendo y reporta el
     *        `num_pending` creciente, que es la señal.
     *
     *        ⚠️ En PHP el sondeo solo corre **dentro de `run()`**, en el worker CLI. Ver la
     *        sección "El modelo de ejecución de PHP" del README.
     */
    public function __construct(
        public string $service,
        public string $environment,
        public string $version,
        public string $tenantId = 'system',
        public TenantIsolation $tenantIsolation = TenantIsolation::Off,
        public string $classification = 'internal',
        public array $schemas = [],
        public ?string $schemaBaseUrl = null,
        public ?ClassifierOptions $classifier = null,
        public ?SigningOptions $signing = null,
        public ?MetricsSink $metrics = null,
        public bool $createStreams = true,
        public ?\Closure $onPoison = null,
        public ?\Closure $onDlq = null,
        public ?LoggerInterface $logger = null,
        public ?ValidationOptions $validation = null,
        public int $pendingPollMs = 15_000,
    ) {
    }
}
