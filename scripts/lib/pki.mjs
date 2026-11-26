/**
 * PKI efímera con `openssl`: una CA propia y un certificado de servidor para `localhost` y
 * `127.0.0.1`. La usan la suite de seguridad (`conformance/security.mjs`, certificados de dos
 * días en un temporal) y `scripts/dev-secure.mjs` (certificados de desarrollo, un año).
 *
 * Por qué `openssl` y no una librería: el repo no tiene ninguna dependencia de criptografía de
 * certificados en Node, y añadir una para un test que solo necesita ficheros PEM sería cobrar
 * a todo el que clone el repo. `openssl` está en cualquier CI y en cualquier máquina de
 * desarrollo; si falta, el error lo dice por su nombre.
 *
 * Por qué ficheros de configuración y no `-addext`: `-addext` no existe en LibreSSL (el
 * `openssl` de macOS hasta hace poco), y una herramienta de desarrollo que solo funciona en
 * Linux es una que cada compañero arregla por su cuenta.
 *
 * Sin dependencias: `scripts/` no tiene package.json.
 */

import { spawnSync } from "node:child_process";
import { chmod, rm, writeFile } from "node:fs/promises";
import path from "node:path";

/** `{ ok: true, version }` o `{ ok: false, motivo }`. */
export function opensslDisponible() {
  const r = spawnSync("openssl", ["version"], { encoding: "utf8" });
  if (r.error || r.status !== 0) {
    return { ok: false, motivo: "no hay `openssl` en el PATH" };
  }
  return { ok: true, version: r.stdout.trim() };
}

function openssl(args, cwd) {
  const r = spawnSync("openssl", args, { encoding: "utf8", cwd });
  if (r.error || r.status !== 0) {
    throw new Error(`openssl ${args.slice(0, 2).join(" ")} falló: ${(r.stderr || r.error?.message || "").trim()}`);
  }
  return r.stdout;
}

/** Una CA autofirmada. Devuelve las rutas de `<nombre>.pem` y `<nombre>.key`. */
async function crearCa(dir, nombre, cn, dias) {
  const cnf = path.join(dir, `${nombre}.cnf`);
  await writeFile(
    cnf,
    `[req]\ndistinguished_name = dn\nprompt = no\n[dn]\nCN = ${cn}\n` +
      `[v3_ca]\nbasicConstraints = critical,CA:TRUE\nkeyUsage = critical,keyCertSign,cRLSign\n`,
  );
  const key = path.join(dir, `${nombre}.key`);
  const pem = path.join(dir, `${nombre}.pem`);
  openssl(["ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", key]);
  openssl(["req", "-x509", "-new", "-key", key, "-sha256", "-days", String(dias), "-config", cnf, "-extensions", "v3_ca", "-out", pem]);
  await chmod(key, 0o600);
  return { pem, key };
}

/**
 * Crea en `dir`:
 *   ca.pem / ca.key              la CA que firma el certificado del servidor
 *   servidor.pem / servidor.key  certificado con SAN `DNS:localhost` e `IP:127.0.0.1`
 *   otra-ca.pem                  (con `otraCa`) una CA AJENA: no firmó nada. Sirve para probar que
 *                                un cliente que confía en otra CA rechaza el certificado del
 *                                servidor
 *
 * `claveLegible`: la clave del servidor con permisos 0644 en vez de 0600. Solo para el perfil
 * `secure` de docker-compose, donde el proceso del contenedor no es el dueño del fichero.
 *
 * Los ficheros intermedios (configuración, petición de firma, número de serie) se borran: en el
 * directorio solo queda lo que alguien va a usar.
 */
export async function crearPki(dir, { dias = 2, claveLegible = false, otraCa = true } = {}) {
  const ca = await crearCa(dir, "ca", "flux-test-ca", dias);
  const otra = otraCa ? await crearCa(dir, "otra-ca", "flux-otra-ca", dias) : undefined;
  // De la CA ajena solo importa su certificado: su clave no firma nada.
  if (otra) await rm(otra.key, { force: true });

  const key = path.join(dir, "servidor.key");
  const csr = path.join(dir, "servidor.csr");
  const pem = path.join(dir, "servidor.pem");
  const ext = path.join(dir, "servidor.ext");
  await writeFile(
    ext,
    "subjectAltName = DNS:localhost,IP:127.0.0.1\nbasicConstraints = CA:FALSE\n" +
      "keyUsage = digitalSignature\nextendedKeyUsage = serverAuth\n",
  );
  openssl(["ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", key]);
  openssl(["req", "-new", "-key", key, "-subj", "/CN=localhost", "-out", csr]);
  openssl([
    "x509", "-req", "-in", csr, "-CA", ca.pem, "-CAkey", ca.key, "-CAcreateserial",
    "-days", String(dias), "-sha256", "-extfile", ext, "-out", pem,
  ]);
  await chmod(key, claveLegible ? 0o644 : 0o600);
  for (const f of [csr, ext, path.join(dir, "ca.cnf"), path.join(dir, "otra-ca.cnf"), path.join(dir, "ca.srl")]) {
    await rm(f, { force: true });
  }

  return {
    ca: ca.pem,
    caKey: ca.key,
    otraCa: otra?.pem,
    servidorCert: pem,
    servidorKey: key,
  };
}
