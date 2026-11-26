<?php

declare(strict_types=1);

namespace Flux\Metrics;

/**
 * Las métricas de las capacidades `state` y `rpc` (08-observability.md §2.4 y §2.5; `protocol.json` → `state.metrics` y `rpc.metrics`).
 *
 * Van **fuera de las siete** de todo SDK L2, en una interfaz aparte, para no romper a quien ya implementa `MetricsSink`: el bus las emite
 * solo si el destino de métricas también la implementa. **Nunca** se etiquetan con el tenant, la clave, el `id`, el `correlationid` ni
 * quien llama: una serie por cada uno.
 */
interface StateRpcMetrics
{
    /**
     * `flux_state_writes_total{contract,outcome}` de un `put` o un `delete`.
     *
     * @param 'ok'|'stale'|'conflict'|'invalid_schema' $outcome
     */
    public function stateWrite(string $contract, string $outcome): void;

    /**
     * `flux_rpc_client_requests_total{operation,outcome}`.
     *
     * @param 'ok'|'unavailable'|'timeout'|'retryable'|'permanent'|'invalid_schema'|'invalid_reply' $outcome
     */
    public function rpcClientRequest(string $operation, string $outcome): void;

    /**
     * `flux_rpc_server_requests_total{operation,outcome}`.
     *
     * @param 'ok'|'retryable'|'permanent'|'poison'|'invalid_schema'|'invalid_signature'|'stale_request' $outcome
     */
    public function rpcServerRequest(string $operation, string $outcome): void;

    /**
     * `flux_rpc_duration_seconds{operation,role}`.
     *
     * @param 'client'|'server' $role
     */
    public function rpcDuration(string $operation, string $role, float $seconds): void;
}
