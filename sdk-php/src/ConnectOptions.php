<?php

declare(strict_types=1);

namespace Flux;

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
     *        eventos de plataforma.
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
     */
    public function __construct(
        public string $service,
        public string $environment,
        public string $version,
        public string $tenantId = 'system',
        public string $classification = 'internal',
        public array $schemas = [],
        public ?string $schemaBaseUrl = null,
        public ?ClassifierOptions $classifier = null,
        public bool $createStreams = true,
        public ?\Closure $onPoison = null,
        public ?\Closure $onDlq = null,
        public ?LoggerInterface $logger = null,
    ) {
    }
}
