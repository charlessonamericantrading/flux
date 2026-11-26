#!/usr/bin/env node
/**
 * El entorno SEGURO de las pruebas de integración del SDK de PHP: un `nats-server` REAL con TLS, una nkey por servicio y las ACLs que
 * genera `scripts/gen-acl.mjs`, aprovisionado con `flux provision` y credenciales de operación. Lo mismo que levanta la suite de
 * seguridad (`conformance/security/entorno.mjs`), no un broker abierto: un broker abierto no prueba nada de seguridad.
 *
 *   node sdk-php/tests/Integration/entorno.mjs [--dev] [--sin-aprovisionar] [--variante alias:servicio:quita[,quita…][:suscripcion]]
 *
 * Imprime UNA línea de JSON por stdout cuando está listo —url, CA, semillas y lo que pasó con `provision`— y se queda vivo hasta que se
 * cierra su stdin o recibe SIGTERM; entonces mata el servidor y borra el temporal. PHPUnit lo lanza con `proc_open` (`SecureBroker`).
 *
 *   --dev                concede además `CONSUMER.CREATE` de cada durable propio (`gen-acl.mjs --dev`)
 *   --sin-aprovisionar   no ejecuta `flux provision`: ningún stream ni durable existe (el caso de `NOT_PROVISIONED`)
 *   --variante …         un usuario extra `alias` con los permisos del servicio MENOS los que empiezan por cada `quita` (publicación) o,
 *                        con el tercer campo, las suscripciones que empiezan por `suscripcion`. «Quitar un permiso se nota»
 *   --sin-tls            un listener sin TLS (los casos de `CONNECTION_REJECTED` con TLS exigido sin TLS en el broker)
 *   --altname-malo       otro listener con TLS cuyo certificado NO cubre 127.0.0.1 (la CA es la correcta: lo que falla es el HOST)
 *   --mtls               otro listener que EXIGE certificado de cliente, y el certificado y la clave de un cliente válido
 *
 * Respeta `NATS_SERVER` (el binario), `FLUX_TEST_PORTS` (`desde-hasta`) y `NODE_EXTRA_CA_CERTS`.
 */

import { execFileSync, spawnSync } from "node:child_process";
import { writeFile } from "node:fs/promises";
import path from "node:path";

import { crearPki } from "../../../scripts/lib/pki.mjs";
import {
  Banco,
  CLI,
  EJEMPLO,
  ROOT,
  conUsuarios,
  credsDe,
  gen,
  nuevaClave,
  permisosJson,
  usuarioConf,
  usuarioOpsConf,
} from "../../../scripts/lib/nats-harness.mjs";

const argv = process.argv.slice(2);
const dev = argv.includes("--dev");
const sinAprovisionar = argv.includes("--sin-aprovisionar");
const sinTls = argv.includes("--sin-tls");
const altnameMalo = argv.includes("--altname-malo");
const mtls = argv.includes("--mtls");
const variantes = [];
for (let i = 0; i < argv.length; i++) {
  if (argv[i] === "--variante") {
    const [alias, servicio, quita = "", suscripcion = ""] = argv[++i].split(":");
    variantes.push({
      alias,
      servicio,
      quita: quita.split(",").filter(Boolean),
      suscripcion: suscripcion.split(",").filter(Boolean),
    });
  }
}

const [desde, hasta] = (process.env.FLUX_TEST_PORTS ?? "7600-7699").split("-").map(Number);
const banco = new Banco({
  prefijo: "flux-php-",
  puertoBase: desde,
  puertos: hasta - desde + 1,
  natsServer: process.env.NATS_SERVER ?? "nats-server",
});
const texto = (seed) => (typeof seed === "string" ? seed : new TextDecoder().decode(seed));
const salir = (codigo, mensaje) => {
  if (mensaje) console.error(mensaje);
  banco.limpiar().finally(() => process.exit(codigo));
};
banco.instalarManejadores((e) => console.error(e));

try {
  await banco.iniciar();
  const tmp = banco.tmp;
  const prod = permisosJson(...(dev ? ["--dev"] : []));
  const ops = nuevaClave();
  const identidades = Object.fromEntries(prod.map((s) => [s.name, nuevaClave()]));

  const mapa = path.join(tmp, "nkeys.json");
  await writeFile(mapa, JSON.stringify(Object.fromEntries(Object.entries(identidades).map(([n, k]) => [n, k.pub]))));
  const g = gen(EJEMPLO, "--nkeys", mapa, ...(dev ? ["--dev"] : []));
  if (g.codigo !== 0) throw new Error(`gen-acl.mjs falló:\n${g.errores}`);

  // Los usuarios extra: los permisos de un servicio, MENOS lo que la variante quita.
  let extra = usuarioOpsConf(ops.pub);
  const clavesVariantes = {};
  for (const v of variantes) {
    const base = prod.find((s) => s.name === v.servicio);
    if (!base) throw new Error(`variante de un servicio que el manifiesto no declara: ${v.servicio}`);
    const publish = base.publish.filter((p) => !v.quita.some((q) => p.startsWith(q)));
    let subscribe = base.subscribe.filter((p) => !v.suscripcion.some((q) => p.startsWith(q)));
    // Un permiso que se quita y no existe haría una variante que no cambia nada: pasaría las mismas pruebas que el servicio entero.
    if (publish.length === base.publish.length && subscribe.length === base.subscribe.length) {
      throw new Error(`la variante ${v.alias} no quita nada de ${v.servicio}: ${v.quita.join(",")}${v.suscripcion.join(",")} no coincide con ningún permiso`);
    }
    // `allow: []` NO deniega nada (A-25): una suscripción que se quedara vacía se sustituye por un subject que nadie usa.
    if (subscribe.length === 0) subscribe = ["_INBOX.variante-sin-concesion.>"];
    const k = nuevaClave();
    clavesVariantes[v.alias] = k;
    extra += usuarioConf(k.pub, publish, subscribe, base.allowResponses);
  }
  const acl = conUsuarios(g.salida, extra);

  const pki = await crearPki(tmp, { dias: 2 });
  const monitorPort = await banco.puertoLibre();
  const top = `tls {\n  cert_file: "${pki.servidorCert}"\n  key_file: "${pki.servidorKey}"\n  timeout: 2\n}\nhttp: "127.0.0.1:${monitorPort}"\n`;
  const srv = await banco.arrancar("principal", acl, { top, servidorNombre: "flux-php-seguro" });
  const url = `tls://127.0.0.1:${srv.port}`;

  const credsOps = path.join(tmp, "ops.creds");
  await writeFile(credsOps, credsDe(ops.seed), { mode: 0o600 });
  let provision = { codigo: null, salida: "", errores: "" };
  if (!sinAprovisionar) {
    const r = spawnSync(process.execPath, [CLI, "provision", EJEMPLO, "--server", url, "--creds", credsOps], {
      encoding: "utf8",
      cwd: ROOT,
      env: { ...process.env, NO_COLOR: "1", NODE_EXTRA_CA_CERTS: pki.ca },
    });
    provision = { codigo: r.status, salida: r.stdout, errores: r.stderr };
  }

  // Un listener SIN TLS (otro servidor): lo que ve un cliente que EXIGE TLS contra un broker que no lo ofrece.
  let plano;
  if (sinTls) {
    const p = await banco.arrancar("plano", acl, { servidorNombre: "flux-php-plano" });
    plano = { url: `nats://127.0.0.1:${p.port}` };
  }

  // Certificados extra, firmados por LA MISMA CA que el del servidor principal: lo que cambia es el host (SAN) o el certificado de cliente.
  const openssl = (...a) => execFileSync("openssl", a, { stdio: "pipe" });
  const emitir = async (nombre, cn, san) => {
    await writeFile(path.join(tmp, `${nombre}.ext`), `subjectAltName=${san}\nbasicConstraints=CA:FALSE\n`);
    openssl("ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", path.join(tmp, `${nombre}.key`));
    openssl("req", "-new", "-key", path.join(tmp, `${nombre}.key`), "-subj", `/CN=${cn}`, "-out", path.join(tmp, `${nombre}.csr`));
    openssl("x509", "-req", "-in", path.join(tmp, `${nombre}.csr`), "-CA", pki.ca, "-CAkey", pki.caKey, "-CAcreateserial", "-days", "2", "-sha256", "-extfile", path.join(tmp, `${nombre}.ext`), "-out", path.join(tmp, `${nombre}.pem`));
    return { cert: path.join(tmp, `${nombre}.pem`), key: path.join(tmp, `${nombre}.key`) };
  };
  let altname;
  if (altnameMalo) {
    const c = await emitir("altname", "otro.example", "DNS:otro.example");
    const a = await banco.arrancar("altname", acl, { top: `tls {\n  cert_file: "${c.cert}"\n  key_file: "${c.key}"\n  timeout: 2\n}\n`, servidorNombre: "flux-php-altname" });
    altname = { url: `tls://127.0.0.1:${a.port}` };
  }
  let mtlsInfo;
  if (mtls) {
    const cliente = await emitir("cliente", "cliente-de-prueba", "DNS:cliente");
    const m = await banco.arrancar("mtls", acl, {
      top: `tls {\n  cert_file: "${pki.servidorCert}"\n  key_file: "${pki.servidorKey}"\n  ca_file: "${pki.ca}"\n  verify: true\n  timeout: 2\n}\n`,
      servidorNombre: "flux-php-mtls",
    });
    mtlsInfo = { url: `tls://127.0.0.1:${m.port}`, certFile: cliente.cert, keyFile: cliente.key };
  }

  const salida = {
    url,
    puerto: srv.port,
    monitor: `http://127.0.0.1:${monitorPort}`,
    ca: pki.ca,
    otraCa: pki.otraCa,
    servidorCert: pki.servidorCert,
    servidorKey: pki.servidorKey,
    credsOps,
    ops: texto(ops.seed),
    servicios: Object.fromEntries(Object.entries(identidades).map(([n, k]) => [n, { semilla: texto(k.seed), publica: k.pub }])),
    variantes: Object.fromEntries(Object.entries(clavesVariantes).map(([a, k]) => [a, { semilla: texto(k.seed), publica: k.pub }])),
    provision,
    plano,
    altname,
    mtls: mtlsInfo,
    dir: tmp,
  };
  process.stdout.write(`${JSON.stringify(salida)}\n`);

  // Vivo hasta que PHPUnit cierre el pipe (o llegue SIGTERM, que ya instaló `instalarManejadores`).
  process.stdin.resume();
  process.stdin.on("end", () => salir(0));
} catch (e) {
  salir(1, `entorno.mjs: ${e.message}`);
}
