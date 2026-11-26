#!/usr/bin/env bash
# Reproduce las observaciones del plan v1.1 contra un nats-server REAL.
#
#   (cd conformance && npm install)
#   docs/plan-v1.1/evidencia/ejecutar.sh
#
# Necesita `nats-server` 2.14.x en el PATH, o su ruta en NATS_SERVER. Sin Docker:
#   GOBIN=$PWD/bin go install github.com/nats-io/nats-server/v2@v2.14.5
set -euo pipefail

AQUI="$(cd "$(dirname "$0")" && pwd)"
RAIZ="$(cd "$AQUI/../../.." && pwd)"
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

# ── 1. La ACL v1, la que generaba el repo ANTES de la Fase 1 ──────────────────
# El generador ya no produce esta configuración (esa es la corrección), así que se usa el
# fixture congelado: conformance/fixtures/acl-v1.conf. Ya trae usuario y contraseña en lugar
# de `nkey`; solo se añade un usuario `ops` sin restricciones que prepara los streams, y el
# puerto y el almacén de JetStream, que el fixture deja a la config que lo incluye.
STORE="$TMP/js1" RAIZ="$RAIZ" node -e '
  const t = require("node:fs").readFileSync(`${process.env.RAIZ}/conformance/fixtures/acl-v1.conf`, "utf8")
    .replace("    users: [", "    users: [\n      { user: \"ops\", password: \"pw\" },");
  process.stdout.write(`port: 4233\njetstream { store_dir: "${process.env.STORE}" }\n` + t);
' >"$TMP/generada.conf"

arrancar "$TMP/generada.conf"
node "$AQUI/acl-generada.mjs"
parar

# ── 2. El diseño candidato ────────────────────────────────────────────────────
sed "s|__STORE__|$TMP/js2|" "$AQUI/candidata.conf" >"$TMP/candidata.conf"
echo
arrancar "$TMP/candidata.conf"
node "$AQUI/diseno-candidato.mjs"
parar
