#!/usr/bin/env bash
# El criterio de aceptación de la Fase 4, con `curl`: lo que hace un sistema SIN conexión NATS (un PHP en un hosting compartido, un cron)
# contra el gateway. Lo ejecuta tal cual `gateway/test/e2e-entrada.test.mjs` contra un broker real; es también el ejemplo del README.
#
#   GATEWAY_URL=http://127.0.0.1:8080 \
#   TOKEN_TIENDA=flx_… TOKEN_CATALOGO=flx_… PASARELA_SECRETO=… \
#   bash gateway/examples/demo.sh
#
# Sale con 0 si cada paso respondió lo que debía; con 1 en el primero que no.
set -euo pipefail

: "${GATEWAY_URL:?define GATEWAY_URL (http://127.0.0.1:8080)}"
: "${TOKEN_TIENDA:?define TOKEN_TIENDA}"
: "${TOKEN_CATALOGO:?define TOKEN_CATALOGO}"
: "${PASARELA_SECRETO:?define PASARELA_SECRETO (el secreto con el que firma la pasarela de pagos)}"

salida="$(mktemp)"
trap 'rm -f "$salida"' EXIT

# paso <título> <estado esperado> <curl…>: imprime el estado y el cuerpo, y falla si el estado no es el esperado.
paso() {
  local titulo="$1" esperado="$2"; shift 2
  local estado
  estado="$(curl -sS -o "$salida" -w '%{http_code}' "$@")"
  printf '== %s -> %s\n%s\n' "$titulo" "$estado" "$(cat "$salida")"
  if [ "$estado" != "$esperado" ]; then
    echo "✗ se esperaba $esperado" >&2
    exit 1
  fi
}

# 1. Publicar un evento. El cuerpo es `data`; el sobre (id, source, time, type, correlationid, traceparent) lo pone el SDK.
paso "publicar un evento" 202 \
  -X POST "$GATEWAY_URL/v1/events/pedidos.pedido.v1.creado" \
  -H "Authorization: Bearer $TOKEN_TIENDA" -H 'Content-Type: application/json' -H 'Flux-Aggregate-Id: ped-123' \
  -d '{"pedidoId":"ped-123","clienteId":"cli-987","aggregateVersion":1,"totalCents":9990,"moneda":"EUR"}'

# 2. Escribir estado (el dueño del contrato) y leerlo (quien lo observa).
paso "escribir estado" 200 \
  -X PUT "$GATEWAY_URL/v1/state/catalogo.producto.v1/prod-1" \
  -H "Authorization: Bearer $TOKEN_CATALOGO" -H 'Content-Type: application/json' \
  -d '{"productoId":"prod-1","nombre":"Mesa de roble","aggregateVersion":1}'
paso "leer estado" 200 \
  "$GATEWAY_URL/v1/state/catalogo.producto.v1/prod-1" -H "Authorization: Bearer $TOKEN_TIENDA"

# 3. Una llamada (petición/respuesta) a una operación que atiende un servicio nativo.
paso "llamar a una operación" 200 \
  -X POST "$GATEWAY_URL/v1/rpc/precios.tarifa.v1.calcular" \
  -H "Authorization: Bearer $TOKEN_TIENDA" -H 'Content-Type: application/json' -H 'Flux-Timeout-Ms: 5000' \
  -d '{"sku":"prod-1","cantidad":3}'

# 4. Lo que el broker NO concede se deniega aunque el token sea válido: catalogo-web no tiene concedida esa operación.
paso "una operación no concedida (la deniega el broker)" 403 \
  -X POST "$GATEWAY_URL/v1/rpc/precios.tarifa.v1.calcular" \
  -H "Authorization: Bearer $TOKEN_CATALOGO" -H 'Content-Type: application/json' -d '{"sku":"prod-1","cantidad":3}'

# 5. Un webhook de un tercero (la pasarela de pagos) enviado TRES veces: el mismo aviso reenviado produce el mismo evento, y los
#    consumidores —que deduplican por id— aplican UN efecto. Firma `t=<unix>,v1=<hmac-sha256(secreto, t.cuerpo)>` con openssl.
cuerpo='{"id":"evt_demo_1","created":1787221600,"amount":9990,"currency":"EUR","customer":{"email":"ana@example.com"}}'
for intento in 1 2 3; do
  t="$(date +%s)"
  firma="$(printf '%s.%s' "$t" "$cuerpo" | openssl dgst -sha256 -hmac "$PASARELA_SECRETO" -hex | awk '{print $NF}')"
  paso "webhook de la pasarela, envío $intento" 202 \
    -X POST "$GATEWAY_URL/v1/hooks/pasarela" \
    -H 'Content-Type: application/json' -H "X-Pasarela-Firma: t=$t,v1=$firma" -d "$cuerpo"
done

echo "✓ aceptación"
