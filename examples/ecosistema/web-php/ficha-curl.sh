#!/usr/bin/env bash
# El equivalente de ficha.php con `curl`: lo que haría un cron, un script de shell o cualquier sistema que sepa hablar HTTP. Lo ejecuta el test.
#
#   FLUX_GATEWAY_URL=http://127.0.0.1:8080 FLUX_GATEWAY_TOKEN=flx_… bash ficha-curl.sh cli-1 9990
set -euo pipefail
: "${FLUX_GATEWAY_URL:?define FLUX_GATEWAY_URL}"
: "${FLUX_GATEWAY_TOKEN:?define FLUX_GATEWAY_TOKEN}"
cliente="${1:?uso: ficha-curl.sh <clienteId> <totalCents>}"
total="${2:?uso: ficha-curl.sh <clienteId> <totalCents>}"

# fragmento:curl-ficha
curl -sS --fail-with-body "$FLUX_GATEWAY_URL/v1/state/clientes.cliente.v1/$cliente" \
  -H "Authorization: Bearer $FLUX_GATEWAY_TOKEN"
curl -sS --fail-with-body -X POST "$FLUX_GATEWAY_URL/v1/rpc/clientes.cliente.v1.calcular-descuento" \
  -H "Authorization: Bearer $FLUX_GATEWAY_TOKEN" -H 'Content-Type: application/json' -H 'Flux-Timeout-Ms: 5000' \
  -d "{\"clienteId\":\"$cliente\",\"totalCents\":$total,\"moneda\":\"EUR\"}"
# /fragmento:curl-ficha
