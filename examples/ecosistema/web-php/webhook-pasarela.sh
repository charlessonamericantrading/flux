#!/usr/bin/env bash
# Lo que hace la pasarela de pagos (un TERCERO) al cobrar: un POST firmado al gateway. Sirve para probar el webhook a mano, y lo ejecuta el test.
# La firma es `t=<unix>,v1=<hmac-sha256(secreto, t + "." + cuerpo)>`: la misma que firma Stripe y la que usa el gateway para sus propias entregas.
#
#   FLUX_GATEWAY_URL=http://127.0.0.1:8080 PASARELA_SECRETO=… bash webhook-pasarela.sh evt_1 ped-001
set -euo pipefail
: "${FLUX_GATEWAY_URL:?define FLUX_GATEWAY_URL}"
: "${PASARELA_SECRETO:?define PASARELA_SECRETO}"
evento="${1:?uso: webhook-pasarela.sh <id del evento> <pedidoId>}"
pedido="${2:?uso: webhook-pasarela.sh <id del evento> <pedidoId>}"

# fragmento:curl-webhook
cuerpo="{\"id\":\"$evento\",\"created\":$(date +%s),\"amount\":9000,\"currency\":\"EUR\",\"metadata\":{\"pedidoId\":\"$pedido\"},\"customer\":{\"email\":\"pagador@example.com\"}}"
t="$(date +%s)"
firma="$(printf '%s.%s' "$t" "$cuerpo" | openssl dgst -sha256 -hmac "$PASARELA_SECRETO" -hex | awk '{print $NF}')"
curl -sS --fail-with-body -X POST "$FLUX_GATEWAY_URL/v1/hooks/pasarela" \
  -H 'Content-Type: application/json' -H "X-Pasarela-Firma: t=$t,v1=$firma" -d "$cuerpo"
# /fragmento:curl-webhook
