/**
 * Un broker en modo OPERADOR/JWT montado a mano, para probar `.creds` de verdad.
 *
 * `nsc` es la herramienta oficial para esto, pero no está en el CI ni en la mayoría de
 * máquinas, y añadirla solo para un test es una dependencia que se rompe en silencio. Un
 * JWT de NATS es un ed25519 sobre `base64url(cabecera).base64url(claims)`: con `nkeys` —que
 * el cliente ya trae— y `node:crypto` basta. El broker real decide si lo acepta, que es lo
 * que se quiere probar: si el JWT estuviera mal firmado, `.creds` no conectaría.
 */

import { createHash } from "node:crypto";
import { writeFileSync } from "node:fs";
import { join } from "node:path";

import { nkeys, TE } from "@nats-io/transport-node";

type Par = ReturnType<typeof nkeys.createUser>;

const b64u = (b: Uint8Array | string) => Buffer.from(b).toString("base64url");
const decodificar = (b: Uint8Array) => new TextDecoder().decode(b);

const BASE32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
function base32(buf: Uint8Array): string {
  let bits = 0;
  let v = 0;
  let out = "";
  for (const byte of buf) {
    v = (v << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      out += BASE32[(v >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }
  if (bits > 0) out += BASE32[(v << (5 - bits)) & 31];
  return out;
}

function firmar(claims: Record<string, unknown>, emisor: Par): string {
  const c: Record<string, unknown> = { ...claims, iat: Math.floor(Date.now() / 1000) };
  c.jti = base32(createHash("sha512-256").update(JSON.stringify(c)).digest());
  const h = b64u(JSON.stringify({ typ: "JWT", alg: "ed25519-nkey" }));
  const p = b64u(JSON.stringify(c));
  return `${h}.${p}.${b64u(emisor.sign(TE.encode(`${h}.${p}`)))}`;
}

const LIMITES = {
  subs: -1, data: -1, payload: -1, imports: -1, exports: -1, wildcards: true, conn: -1,
  leaf: -1, mem_storage: -1, disk_storage: -1, streams: -1, consumer: -1,
};

export interface EntornoJwt {
  /** Cuerpo de nats-server.conf (sin `listen` ni `jetstream`: los pone el test). */
  conf: string;
  /** Un `.creds` válido de un usuario de la cuenta PROD, como lo escribiría `nsc`. */
  creds: string;
  /** Lo mismo, pero con la semilla de OTRA clave: el JWT no corresponde a quien firma. */
  credsConSemillaAjena: string;
  /** Escribe un `.creds` en `dir` y devuelve su ruta. */
  escribirCreds(dir: string, nombre: string, contenido: string): string;
}

export function crearEntornoJwt(): EntornoJwt {
  const operador = nkeys.createOperator();
  const sistema = nkeys.createAccount();
  const prod = nkeys.createAccount();
  const usuario = nkeys.createUser();
  const ajeno = nkeys.createUser();

  const jwtOperador = firmar(
    {
      iss: operador.getPublicKey(),
      name: "op-test",
      sub: operador.getPublicKey(),
      nats: { type: "operator", version: 2, system_account: sistema.getPublicKey() },
    },
    operador,
  );
  const cuenta = (nombre: string, par: Par, limits: object) =>
    firmar(
      { iss: operador.getPublicKey(), name: nombre, sub: par.getPublicKey(), nats: { limits, type: "account", version: 2 } },
      operador,
    );
  const jwtUsuario = firmar(
    {
      iss: prod.getPublicKey(),
      name: "svc",
      sub: usuario.getPublicKey(),
      nats: { pub: {}, sub: {}, subs: -1, data: -1, payload: -1, type: "user", version: 2 },
    },
    prod,
  );

  const creds = (semilla: Uint8Array) =>
    [
      "-----BEGIN NATS USER JWT-----",
      jwtUsuario,
      "------END NATS USER JWT------",
      "",
      "************************* IMPORTANT *************************",
      "    NKEY Seed printed below can be used to sign and prove identity.",
      "    NKEYs are sensitive and should be treated as secrets.",
      "",
      "-----BEGIN USER NKEY SEED-----",
      decodificar(semilla),
      "------END USER NKEY SEED------",
      "",
      "*************************************************************",
      "",
    ].join("\n");

  return {
    conf: `operator: ${jwtOperador}
system_account: ${sistema.getPublicKey()}
resolver: MEMORY
resolver_preload: {
  ${sistema.getPublicKey()}: ${cuenta("SYS", sistema, { ...LIMITES, mem_storage: 0, disk_storage: 0 })}
  ${prod.getPublicKey()}: ${cuenta("PROD", prod, LIMITES)}
}
`,
    creds: creds(usuario.getSeed()),
    credsConSemillaAjena: creds(ajeno.getSeed()),
    escribirCreds(dir, nombre, contenido) {
      const ruta = join(dir, nombre);
      writeFileSync(ruta, contenido, { mode: 0o600 });
      return ruta;
    },
  };
}
