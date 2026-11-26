#!/usr/bin/env bash
# Reproduce las observaciones del plan v1.1 que AÚN no son casos de conformance/cases/ (la
# petición/respuesta, Fase 3) contra un nats-server REAL. La ACL ya es `node conformance/security.mjs`
# y el estado replicado, `node conformance/state-broker.mjs`.
#
#   (cd conformance && npm install)
#   docs/plan-v1.1/evidencia/ejecutar.sh
#
# Necesita `nats-server` 2.14.x en el PATH, o su ruta en NATS_SERVER. Sin Docker:
#   GOBIN=$PWD/bin go install github.com/nats-io/nats-server/v2@v2.14.5
set -euo pipefail

AQUI="$(cd "$(dirname "$0")" && pwd)"
BIN="${NATS_SERVER:-nats-server}"
TMP="$(mktemp -d)"
PID=""
limpiar() { [ -n "$PID" ] && kill "$PID" 2>/dev/null || true; rm -rf "$TMP"; }
trap limpiar EXIT

arrancar() {
  "$BIN" -c "$1" >"$TMP/servidor.log" 2>&1 &
  PID=$!
  sleep 1.5
  kill -0 "$PID" 2>/dev/null || { cat "$TMP/servidor.log"; exit 1; }
}
parar() { kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; PID=""; }

# ── El diseño candidato: petición/respuesta (Fase 3) ─────────────────────────────────────────
sed "s|__STORE__|$TMP/js2|" "$AQUI/candidata.conf" >"$TMP/candidata.conf"
arrancar "$TMP/candidata.conf"
node "$AQUI/diseno-candidato.mjs"
parar
