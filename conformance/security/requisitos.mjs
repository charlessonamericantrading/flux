/**
 * Los requisitos de las suites que levantan un broker real: se saltan con aviso, o FALLAN con `FLUX_REQUIRE_BROKER`. Los comparten
 * `conformance/security.mjs` y `conformance/interop-rpc.mjs`.
 */

import { spawnSync } from "node:child_process";
import { statSync } from "node:fs";
import path from "node:path";

import { opensslDisponible } from "../../scripts/lib/pki.mjs";
import { ROOT } from "../../scripts/lib/nats-harness.mjs";

// ─── Requisitos: se saltan con aviso, o FALLAN con FLUX_REQUIRE_BROKER ───────

/** `FLUX_REQUIRE_BROKER` definida y no vacía, `0` o `false`. */
export const exigido = (() => {
  const v = (process.env.FLUX_REQUIRE_BROKER ?? "").trim().toLowerCase();
  return v !== "" && v !== "0" && v !== "false";
})();

/** Qué falta de lo que la suite necesita, o `undefined`. */
export function falta(que) {
  switch (que) {
    case "nats-server": {
      const bin = process.env.NATS_SERVER ?? "nats-server";
      const r = spawnSync(bin, ["--version"], { encoding: "utf8" });
      return r.error || r.status !== 0
        ? `no se pudo ejecutar nats-server («${bin}»). Define NATS_SERVER con su ruta; sin Docker: GOBIN=$PWD/bin go install github.com/nats-io/nats-server/v2@v2.14.5`
        : undefined;
    }
    case "openssl":
      return opensslDisponible().ok ? undefined : "no hay `openssl` en el PATH (la PKI efímera de TLS lo necesita)";
    case "sdk-node/dist":
      return statSync(path.join(ROOT, "sdk-node", "dist", "index.js"), { throwIfNoEntry: false })?.isFile()
        ? undefined
        : "no existe sdk-node/dist: ejecuta (cd sdk-node && npm install && npm run build)";
    case "cargo":
      return spawnSync("cargo", ["--version"], { encoding: "utf8" }).status === 0 ? undefined : "no hay `cargo` en el PATH (el SDK de Rust no se puede compilar)";
    default:
      return `requisito desconocido «${que}»`;
  }
}

/** Sale: con aviso y 0, o —con FLUX_REQUIRE_BROKER— con el error y 1. */
export function sinRequisitoGlobal(motivo, suite = "la suite de seguridad") {
  if (exigido) {
    console.error(
      `✗ ${motivo}.\n  FLUX_REQUIRE_BROKER está activada: ${suite} es obligatoria, y saltarla daría un CI verde sin haber ejecutado nada contra el broker.`,
    );
    process.exit(1);
  }
  console.warn(
    `\nAVISO: se SALTA ${suite}: ${motivo}.\n       Define NATS_SERVER, o FLUX_REQUIRE_BROKER=1 para que su ausencia sea un fallo.\n`,
  );
  process.exit(0);
}

