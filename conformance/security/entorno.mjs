/**
 * El entorno que comparten `conformance/security.mjs` y `conformance/interop-rpc.mjs`: un nats-server con TLS, la ACL que genera
 * `scripts/gen-acl.mjs` (con las VARIANTES estropeadas a propósito que pidan los casos), las identidades nkey de cada servicio,
 * `flux provision` con credenciales de operación y el contexto del intérprete.
 *
 * Es lo que antes vivía dentro de `security.mjs`: se sacó a un módulo para que el runner de interoperabilidad entre SDKs
 * ejecute sus escenarios BAJO LOS MISMOS PERMISOS que la suite de seguridad y no bajo una ACL escrita a mano. Dos copias de
 * «cómo se monta el broker» habrían acabado divergiendo, y la que mintiera sería la que daba por buena una ACL distinta de la real.
 */

import { spawnSync } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";

import { crearPki } from "../../scripts/lib/pki.mjs";
import {
  CLI,
  EJEMPLO,
  ROOT,
  conUsuarios,
  conectar,
  credsDe,
  gen,
  jetstream,
  jetstreamManager,
  nuevaClave,
  usuarioConf,
  usuarioOpsConf,
} from "../../scripts/lib/nats-harness.mjs";
import { actoresDe, claveVariante, esVariante, Interprete, normalizarActor, permisosDeVariante } from "./interprete.mjs";

/** Los permisos de TODOS los servicios de un manifiesto, tal y como los afirma `gen-acl.mjs --json`. */
export const permisosDe = (manifiesto, ...args) => JSON.parse(gen(manifiesto, "--json", ...args).salida);

/**
 * El manifiesto de la suite: `examples/services.json` MÁS los servicios que el JSON de los casos declare en
 * `scenario.extraServices` (un servicio que observa estado y además llama a una operación, que el ejemplo no tiene). Se escribe
 * en el temporal: el ejemplo del repo no se toca —lo usan `verify-acl`, la CLI y la documentación— y el resultado sigue siendo
 * lo que `gen-acl.mjs` lee de verdad, no una simulación.
 */
export async function manifiestoDeLaSuite(suite, tmp) {
  const extra = suite.scenario?.extraServices ?? [];
  if (extra.length === 0) return EJEMPLO;
  const base = JSON.parse(await readFile(EJEMPLO, "utf8"));
  for (const s of extra) {
    if (base.services.some((x) => x.name === s.name)) throw new Error(`scenario.extraServices: «${s.name}» ya está en examples/services.json`);
  }
  const ruta = path.join(tmp, "services-suite.json");
  await writeFile(ruta, JSON.stringify({ ...base, services: [...base.services, ...extra] }, null, 2));
  return ruta;
}

/**
 * Levanta un nats-server con TLS, la ACL dada y un puerto de MONITORIZACIÓN (`/connz`: las suscripciones reales de cada
 * conexión, que ningún cliente puede falsear), conecta `ops` y aprovisiona con `flux provision`. Devuelve el contexto, o lanza.
 */
export async function levantar(banco, nombre, { acl, identidades, ops, pki, manifiesto = EJEMPLO }) {
  const monitorPort = await banco.puertoLibre();
  const top = `tls {\n  cert_file: "${pki.servidorCert}"\n  key_file: "${pki.servidorKey}"\n  timeout: 2\n}\nhttp: "127.0.0.1:${monitorPort}"\n`;
  const srv = await banco.arrancar(nombre, acl, { top, servidorNombre: `flux-security-${nombre}` });
  const url = `tls://127.0.0.1:${srv.port}`;

  const nc = await conectar(url, { nombre: "ops", seed: ops.seed, tls: { caFile: pki.ca }, etiqueta: "flux-security" });
  const opsConn = { nc, jsm: await jetstreamManager(nc), js: jetstream(nc) };

  // `flux provision` con credenciales de operación, por TLS con la CA efímera. La CLI no tiene
  // una opción `--ca`, así que la CA llega por el mecanismo de Node (NODE_EXTRA_CA_CERTS): un
  // operador con una CA privada tendría que hacer lo mismo.
  const credsOps = path.join(banco.tmp, `ops-${nombre}.creds`);
  await writeFile(credsOps, credsDe(ops.seed), { mode: 0o600 });
  const r = spawnSync(process.execPath, [CLI, "provision", manifiesto, "--server", url, "--creds", credsOps], {
    encoding: "utf8",
    cwd: ROOT,
    env: { ...process.env, NO_COLOR: "1", NODE_EXTRA_CA_CERTS: pki.ca },
  });
  const provision = { codigo: r.status, salida: r.stdout, errores: r.stderr };
  return { srv, url, ops: opsConn, provision, identidades, pki, monitor: `http://127.0.0.1:${monitorPort}` };
}

/** Lo que el intérprete necesita de un servidor levantado. */
export const baseCtx = (l, ctxPrincipal) => ({
  url: l.url,
  caFile: l.pki.ca,
  otraCa: l.pki.otraCa,
  ops: l.ops,
  canon: ctxPrincipal.canon,
  identidades: l.identidades,
  variantes: new Map(),
});

/**
 * Monta el entorno completo para los casos de `suite`: identidades, ACL generada (con las variantes que pidan sus pasos), PKI,
 * servidor, `flux provision` e intérprete. Con `aclFichero`, la ACL es la de ese fichero (la v1 congelada, para ver qué casos
 * detectan una ACL rota) y no hay variantes.
 *
 * @returns {{ l, contexto, interprete, generada, prod, dev, variantes, provision: { ok, creados, esperados: { streams, durables } } }}
 */
export async function crearEntorno({ banco, suite, aclFichero }) {
  const tmp = banco.tmp;
  const generada = !aclFichero;
  const manifiesto = await manifiestoDeLaSuite(suite, tmp);
  const prod = permisosDe(manifiesto);
  const dev = permisosDe(manifiesto, "--dev");
  const ops = nuevaClave();
  const canon = JSON.parse(await readFile(path.join(ROOT, "protocol.json"), "utf8")).consumer;

  const identidades = new Map([["ops", ops]]);
  const variantes = new Map();
  let acl;
  if (generada) {
    for (const s of prod) identidades.set(s.name, nuevaClave());
    const mapa = path.join(tmp, "nkeys.json");
    await writeFile(mapa, JSON.stringify(Object.fromEntries(prod.map((s) => [s.name, identidades.get(s.name).pub]))));
    const g = gen(manifiesto, "--nkeys", mapa);
    if (g.codigo !== 0) throw new Error(`gen-acl.mjs falló:\n${g.errores}`);

    // Las variantes que los casos piden: los permisos generados, estropeados a propósito.
    let extra = "";
    for (const caso of suite.cases) {
      for (const paso of caso.steps) {
        for (const a of actoresDe(paso).map(normalizarActor).filter(esVariante)) {
          const clave = claveVariante(a);
          if (variantes.has(clave)) continue;
          // Una variante imposible (quita un permiso que el generador ya no concede) no aborta la
          // suite: hace fallar los pasos que la usan, con el motivo. Es justo lo que ocurre si
          // alguien quita un permiso del generador.
          try {
            const k = nuevaClave();
            const perms = permisosDeVariante(a, { prod, dev });
            variantes.set(clave, k);
            extra += usuarioConf(k.pub, perms.publish, perms.subscribe, perms.allowResponses);
          } catch (e) {
            variantes.set(clave, { error: e.message });
          }
        }
      }
    }
    extra += usuarioOpsConf(ops.pub);
    acl = conUsuarios(g.salida, extra);
  } else {
    // La ACL antigua entra con usuario y contraseña (no tiene nkeys): `<servicio>` / `pw`.
    for (const s of prod) identidades.set(s.name, { user: s.name, pass: "pw" });
    acl = conUsuarios(await readFile(path.resolve(aclFichero), "utf8"), usuarioOpsConf(ops.pub));
  }

  const pki = await crearPki(tmp, { dias: 2 });
  const l = await levantar(banco, "principal", { acl, identidades, ops, pki, manifiesto });

  const durables = prod.reduce((n, s) => n + s.durables.length, 0);
  // EVT_ y DLQ_ por dominio, y STATE_ por cada dominio con estado.
  const streams = 2 * new Set(prod.flatMap((s) => s.owns)).size + new Set(prod.flatMap((s) => s.stateOwns)).size;
  const creados = /(\d+) creado/.exec(l.provision.salida)?.[1];
  const provisionOk = l.provision.codigo === 0 && Number(creados) === streams + durables;

  const contexto = { ...baseCtx(l, { canon }), generada, ops: l.ops, srv: l.srv, pki, canon, variantes, monitor: l.monitor, generadas: prod };
  return {
    l,
    contexto,
    interprete: new Interprete(contexto),
    generada,
    prod,
    dev,
    variantes,
    provision: { ok: provisionOk, creados, esperados: { streams, durables }, salida: l.provision.salida, errores: l.provision.errores },
  };
}
