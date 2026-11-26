/**
 * `flux call <operacion> --data '{…}'` — una petición a mano, para depurar
 * (specification/11-request-reply.md §4, §12 «Herramientas»).
 *
 * Construye el sobre **como lo hace un SDK** (`id` UUIDv7 generado, `source` `/<entorno>/flux-cli`,
 * `time` con 3 decimales, las cuatro extensiones, `data` el último) y NUNCA lo escribe a mano ni con
 * un atributo que el SDK no escribe: la CLI no pide `id`, `time`, `type`, `correlationid` ni
 * `traceparent`. Lo manda por **core NATS** a `rpc.<operacion>` con un subject de respuesta propio
 * (el inbox privado `_INBOX.flux-cli`), por una conexión que anuncia `headers` y `no_responders`
 * (la librería lo hace sola; sin ello `RPC_UNAVAILABLE` sería un timeout, A-57), y traduce lo que
 * ve a un código estable de 04-errors.md §7.
 *
 * Como el resto de la CLI, no usa el SDK de flux: una herramienta para depurar el SDK no puede
 * construirse sobre el SDK.
 */

import { readFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { c, OK, FAIL, WARN } from "./ui.mjs";
import { datosOcultos, marcaOculto, redactarSobre } from "./privacidad.mjs";
import {
  ErrorRpc,
  ahoraRfc3339,
  canonRpc,
  comprobarTamano,
  construirPeticion,
  esquemaPorDefecto,
  parsearTimeout,
  serializar,
  uuidv7,
  validarClasificacion,
  validarData,
  validarEntorno,
  validarOperacion,
  validarRespuesta,
  validarTenantLlamada,
} from "./rpc-model.mjs";

const RAIZ = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const SERVICIO = "flux-cli";
/** El inbox privado de la CLI (06-security.md §3.3): `_INBOX.<servicio>`; la ACL de quien la use debe conceder `_INBOX.flux-cli.>`. */
export const INBOX = `_INBOX.${SERVICIO}`;
const dec = new TextDecoder();

const versionDeLaCli = () => JSON.parse(readFileSync(join(RAIZ, "cli", "package.json"), "utf8")).version;

// ─── Entradas ────────────────────────────────────────────────────────────────────────────────────

/**
 * `--data '{…}'`, `--data @fichero.json` o `--data @-` (stdin). Un error NO reproduce el texto: el
 * `data` de una orden puede llevar datos personales y los mensajes de error acaban en logs.
 */
export async function leerData(valor, { leerStdin = leerStdinTodo } = {}) {
  if (valor === undefined) {
    throw new ErrorRpc(
      "INVALID_RPC_CALL",
      "falta `--data`: el cuerpo de la petición, un objeto JSON, p.ej. `--data '{\"clienteId\":\"cli-1\"}'` (o `@fichero.json`, o `@-` para stdin). " +
        "Si la operación no lleva parámetros, `--data '{}'`.",
    );
  }
  let texto = valor;
  if (typeof valor === "string" && valor.startsWith("@")) {
    const ruta = valor.slice(1);
    try {
      texto = ruta === "-" ? await leerStdin() : await readFile(ruta, "utf8");
    } catch (e) {
      throw new ErrorRpc(
        "INVALID_RPC_CALL",
        `no se pudo leer ${ruta === "-" ? "stdin" : `el fichero «${ruta}»`} de \`--data\` (${e.code ?? "error de lectura"}). Comprueba la ruta, o pasa el JSON directamente: \`--data '{…}'\`.`,
      );
    }
  }
  let data;
  try {
    data = JSON.parse(texto);
  } catch (e) {
    const pos = /position (\d+)/.exec(e.message)?.[1];
    throw new ErrorRpc(
      "INVALID_RPC_CALL",
      `\`--data\` no es JSON válido${pos ? ` (el error está en la posición ${pos})` : ""}. ` +
        "Las claves y los textos van entre comillas dobles y en el shell todo el JSON entre comillas simples: `--data '{\"clienteId\":\"cli-1\"}'`.",
    );
  }
  return validarData(data);
}

async function leerStdinTodo() {
  const trozos = [];
  for await (const t of process.stdin) trozos.push(t);
  return Buffer.concat(trozos).toString("utf8");
}

// ─── El bundle y la validación L3 (sin red) ──────────────────────────────────────────────────────

/** `{ ruta, bundle }`, o `null` si no se pidió ninguno y el de por defecto no está. Uno pedido y ilegible es un error. */
export function cargarBundle(rutaPedida) {
  const ruta = rutaPedida ?? join(RAIZ, "schemas", "bundle.json");
  try {
    return { ruta, bundle: JSON.parse(readFileSync(ruta, "utf8")) };
  } catch (e) {
    if (rutaPedida === undefined && e.code === "ENOENT") return null;
    throw new Error(
      `no se pudo leer el bundle «${ruta}» (${e.code ?? e.message}). Regénéralo con \`node scripts/bundle-schemas.mjs\`, o quita \`--bundle\` para no validar contra el esquema.`,
    );
  }
}

/** ajv es una dependencia OPCIONAL (L3 es opt-in): importación diferida, como en el SDK de Node. `null` si no está. */
async function cargarAjv() {
  try {
    const mod = await import("ajv/dist/2020.js");
    return mod.default?.default ?? mod.default ?? mod.Ajv2020;
  } catch {
    return null;
  }
}

/** Los errores de ajv en español, con la RUTA y nunca el valor. */
function explicarErrorAjv(e) {
  const ruta = e.instancePath || "(raíz)";
  const p = e.params ?? {};
  switch (e.keyword) {
    case "required": return `${ruta}/${p.missingProperty} es obligatorio`;
    case "additionalProperties": return `${ruta} no admite el campo «${p.additionalProperty}»`;
    case "type": return `${ruta} debe ser de tipo ${[].concat(p.type).join(" o ")}`;
    case "enum": return `${ruta} no es uno de los valores permitidos`;
    case "const": return `${ruta} no es el valor fijo que exige el esquema`;
    case "pattern": return `${ruta} no cumple el patrón ${p.pattern}`;
    case "minimum": return `${ruta} debe ser ≥ ${p.limit}`;
    case "maximum": return `${ruta} debe ser ≤ ${p.limit}`;
    case "minLength": return `${ruta} es demasiado corto (mínimo ${p.limit} caracteres)`;
    case "maxLength": return `${ruta} es demasiado largo (máximo ${p.limit} caracteres)`;
    default: return `${ruta} ${e.message ?? "no cumple el esquema"}`;
  }
}

/** `null` si valida; si no, la lista de rutas que incumplen (TODAS, no solo la primera). `undefined` si no se pudo validar (sin ajv). */
async function validarContraEsquema(esquema, data) {
  const Ajv = await cargarAjv();
  if (!Ajv) return undefined;
  const fn = new Ajv({ allErrors: true, strict: false }).compile(esquema);
  return fn(data) ? null : (fn.errors ?? []).map(explicarErrorAjv);
}

// ─── Qué se ve desde fuera, traducido a un código estable (11-request-reply.md §4.3) ─────────────

const nombresDe = (e) => {
  const n = [];
  for (let x = e, i = 0; x && i < 5; x = x.cause, i++) n.push(x.name, x.constructor?.name, x.message);
  return n.filter(Boolean).join(" | ");
};

/** El fallo de `nc.request` → `ErrorRpc`, por NOMBRE (con dos copias de la librería `instanceof` falla sin ruido). */
function traducirFallo(e, { o, timeoutMs, id, capturan }) {
  const n = nombresDe(e);
  if (/NoResponders?/i.test(n)) {
    return new ErrorRpc(
      "RPC_UNAVAILABLE",
      `nadie atiende «${o.operacion}» (el broker contestó \`no responders\`: nadie escucha ${o.subject}). ` +
        `Comprueba que el servicio dueño del dominio «${o.dom}» está desplegado y ha llamado a respond() para ESTA operación y versión exactas (${o.subject}) — ` +
        "una errata o una versión aún no desplegada dan esto—; y que su ACL le deja suscribirse a `rpc." + o.dom + ".>`: un dueño sin ese permiso parece un servicio caído (A-73). " +
        "Es RETRYABLE y no ejecutó nada: ninguna instancia recibió la petición.",
    );
  }
  if (/PermissionViolation|Permissions? Violation/i.test(n)) {
    const subject = /(?:Publish|Subscription) to "([^"]+)"/.exec(e.message ?? e.cause?.message ?? "")?.[1];
    const sub = /Subscription to/i.test(n);
    return new ErrorRpc(
      "PERMISSION_DENIED",
      sub
        ? `el broker deniega a esta conexión suscribirse a su inbox${subject ? ` («${subject}»)` : ""}, y sin él ninguna respuesta puede llegar. ` +
          `La CLI usa el inbox privado \`${INBOX}.>\`: la ACL del usuario con el que te conectas (\`--creds\`) debe concederlo (06-security.md §3.3).`
        : `el broker deniega a esta conexión publicar en ${subject ? `«${subject}»` : o.subject}: ` +
          `su usuario no tiene concedida la operación ${o.operacion}. Añádela a las llamadas del servicio en el manifiesto y regenera la ACL con \`gen-acl.mjs\`, o usa las credenciales de otro usuario (\`--creds\`). ` +
          "Con esta librería falla en ~1 ms; con otras solo se vería como un timeout (A-66, A-72).",
    );
  }
  if (/Timeout/i.test(e.name ?? "") || /Timeout/i.test(n)) {
    return new ErrorRpc(
      "RPC_TIMEOUT",
      `no llegó respuesta en ${timeoutMs} ms. OJO: la operación PUEDE haberse ejecutado —un responder lento la ejecuta y su respuesta llega tarde (A-61), y uno que muere con la petición la pierde (A-62)—: ` +
        `no la repitas sin que el responder deduplique por el \`id\` de la petición (${id}). ` +
        `Si es lenta de verdad, sube \`--timeout\` (máximo ${canonRpc().timeout.maxMs} ms).` +
        (capturan?.length
          ? ` Y MIRA ESTO: ${capturan.length === 1 ? "el stream" : "los streams"} ${capturan.join(", ")} captura${capturan.length === 1 ? "" : "n"} ${o.subject}: ` +
            "un stream SIN acuse (`no_ack`) cuenta como interés y convierte `no responders` en un timeout (A-67). Ejecuta `flux doctor`."
          : ""),
      { puedeHaberseEjecutado: true },
    );
  }
  return null;
}

// ─── Salida ──────────────────────────────────────────────────────────────────────────────────────

const ms = (x) => (x === null || x === undefined ? "" : `${x < 10 ? x.toFixed(1) : Math.round(x)} ms`);

/** Pasa el resultado a lo que se imprime con `--json` (el sobre de la petición y el de la respuesta, con `data` redactado igual que en `tail`). */
function paraJson(r, showData) {
  return {
    ok: r.ok,
    operation: r.operacion ?? null,
    subject: r.subject ?? null,
    ms: r.ms === null || r.ms === undefined ? null : Math.round(r.ms * 10) / 10,
    request: r.peticion ? redactarSobre(r.peticion, showData) : null,
    ...(r.respuesta && { reply: redactarSobre(r.respuesta, showData) }),
    ...(r.error && { error: r.error }),
    ...(r.notas.length && { notes: r.notas }),
  };
}

function imprimirHumano(r, { showData, verbose }) {
  const lineas = [];
  const cabeceraOp = r.operacion ? `${c.bold(r.operacion)} ${c.gray(`→ ${r.subject}`)}` : "";
  if (r.ok) {
    lineas.push(`${OK} ${cabeceraOp}  ${c.gray(ms(r.ms))}`);
    const e = r.respuesta;
    lineas.push(`  ${c.gray("respuesta")}  ${e.type} · de ${e.source} · tenant ${e.tenantid}`);
    lineas.push(`  ${c.gray("id")}         ${e.id} ${c.gray(`(causationid = id de la petición ${r.peticion.id})`)}`);
    if (datosOcultos(e.dataclassification) && !showData) {
      lineas.push(`  ${c.gray("data")}       ${c.yellow(marcaOculto(e.dataclassification))} ${c.gray("— usa --show-data si de verdad lo necesitas")}`);
    } else {
      lineas.push(`  ${c.gray("data")}       ${JSON.stringify(e.data, null, 2).replace(/\n/g, "\n              ")}`);
    }
  } else {
    const er = r.error;
    const clase = er.class.toUpperCase();
    lineas.push(
      `${FAIL} ${c.bold(er.code)} ${c.gray(`[${clase}]`)}${r.ms !== null && r.ms !== undefined ? `  ${c.gray(ms(r.ms))}` : ""}` +
        (er.origin === "remote" ? c.gray("  — error del servicio que responde (com.flux.rpc.error.v1)") : ""),
    );
    if (cabeceraOp) lineas.push(`  ${cabeceraOp}`);
    lineas.push(`  ${er.message}`);
    if (er.retryAfterMs !== undefined) {
      lineas.push(`  ${c.gray(`retryAfterMs ${er.retryAfterMs}: una sugerencia para el PRIMER reintento (04-errors.md §2.2)`)}`);
    }
    if (r.respuesta) lineas.push(`  ${c.gray(`responde ${r.respuesta.source} · id ${r.respuesta.id}`)}`);
  }
  if (r.peticion) {
    lineas.push(`  ${c.gray(`petición id ${r.peticion.id} · correlationid ${r.peticion.correlationid} · tenant ${r.peticion.tenantid}`)}`);
  }
  for (const n of r.notas) lineas.push(`  ${WARN} ${c.gray(n)}`);
  if (verbose && r.peticion) {
    lineas.push(c.gray("  petición enviada:"));
    lineas.push(JSON.stringify(redactarSobre(r.peticion, showData), null, 2).replace(/^/gm, "    "));
  }
  (r.ok ? console.log : console.error)(lineas.join("\n"));
}

// ─── El comando ──────────────────────────────────────────────────────────────────────────────────

/**
 * `conectar()` abre la conexión (la de la CLI: URL y credenciales ya resueltas); se llama DESPUÉS de
 * validar todo lo que se puede validar sin red, para que una operación mal escrita no se vea como
 * «no se pudo conectar». Devuelve el código de salida: 0 con respuesta, 1 con un error (de la
 * llamada o del servicio que responde), 2 si no se pudo conectar (lo decide `conectar`).
 */
export async function comandoCall(operacion, opts, { conectar }) {
  const notas = [];
  const r = { ok: false, operacion: null, subject: null, ms: null, peticion: null, respuesta: null, error: null, notas };
  let nc;
  try {
    // 0. Validar y construir SIN red, en el orden de 11-request-reply.md §4.1.
    const o = validarOperacion(operacion);
    r.operacion = o.operacion;
    r.subject = o.subject;
    const timeoutMs = parsearTimeout(opts.timeout);
    const data = await leerData(opts.data);
    const clasificacion = validarClasificacion(opts.classification ?? "internal");
    const tenantId = validarTenantLlamada(opts.tenant);
    const entorno = validarEntorno(opts.env ?? process.env.FLUX_ENV);
    if (opts.aggregateId !== undefined && (typeof opts.aggregateId !== "string" || opts.aggregateId === "")) {
      throw new ErrorRpc("INVALID_RPC_CALL", "`--aggregate-id` debe ser un texto no vacío: el identificador del agregado sobre el que se pide (`cli-1`). Es el `subject` del sobre, NO el subject de NATS.");
    }

    // L3: `data` contra el esquema de la petición, del bundle y sin red.
    const cargado = cargarBundle(opts.bundle);
    const indice = cargado?.bundle.rpc?.[o.operacion];
    let dataschema = indice?.request;
    if (!cargado) notas.push("sin bundle (schemas/bundle.json): no se valida `data` contra el esquema y el `dataschema` es el <major>.0.0. Pásalo con `--bundle`.");
    else if (!indice) notas.push(`«${o.operacion}» no está en el índice rpc del bundle (${cargado.ruta}): no se valida \`data\` y el \`dataschema\` es el <major>.0.0.`);
    if (opts.validate !== false && indice) {
      const esquema = cargado.bundle.schemas?.[indice.request];
      if (!esquema) {
        throw new ErrorRpc("INVALID_SCHEMA", `el bundle indexa la petición en ${indice.request} pero no trae el esquema: regéneralo con \`node scripts/bundle-schemas.mjs\`.`);
      }
      const malos = await validarContraEsquema(esquema, data);
      if (malos === undefined) {
        notas.push("no se validó `data`: falta el paquete `ajv` (dependencia opcional: `npm install ajv` en cli/).");
      } else if (malos) {
        throw new ErrorRpc(
          "INVALID_SCHEMA",
          `la petición no cumple ${indice.request}: ${malos.join("; ")}. ` +
            "Arregla `--data` (las rutas de arriba, ningún valor), o usa `--no-validate` para mandarla igualmente y ver qué contesta el responder.",
        );
      }
    }
    dataschema ??= esquemaPorDefecto(o, "request");

    const id = uuidv7();
    const peticion = construirPeticion({
      operacion: o.operacion,
      tenantId,
      aggregateId: opts.aggregateId,
      id,
      source: `/${entorno}/${SERVICIO}`,
      time: ahoraRfc3339(),
      correlationid: id,
      producerversion: versionDeLaCli(),
      classification: clasificacion,
      dataschema,
      data,
    });
    const bytes = serializar(peticion);
    comprobarTamano(bytes, undefined); // 1 MiB; el `max_payload` del broker, con la conexión abierta
    r.peticion = peticion; // una petición que no cabe no se imprime: son hasta megabytes de `data`

    // 1. Publicar por core NATS con un subject de respuesta propio.
    nc = await conectar();
    r.peticion = null;
    comprobarTamano(bytes, nc.info?.max_payload);
    r.peticion = peticion;

    // 2. Esperar, lo primero que ocurra.
    const t0 = performance.now();
    let llegada;
    try {
      llegada = await nc.request(o.subject, bytes, { timeout: timeoutMs });
    } catch (e) {
      r.ms = performance.now() - t0;
      const traducido = traducirFallo(e, { o, timeoutMs, id, capturan: null });
      if (!traducido) throw e;
      if (traducido.code === "RPC_TIMEOUT") {
        // ¿Hay un stream que captura la operación? Con credenciales de operación se sabe; sin ellas, no se sabe y se calla.
        const capturan = await streamsQueCapturan(nc, o.subject);
        throw traducirFallo(e, { o, timeoutMs, id, capturan });
      }
      throw traducido;
    }
    r.ms = performance.now() - t0;

    // 3-4. ¿Es la respuesta a ESTA petición? Si es un error, se traduce a su clase.
    const v = validarRespuesta(
      { operacion: o.operacion, id: peticion.id, correlationid: peticion.correlationid, tenantid: peticion.tenantid },
      llegada.data,
      { showData: opts.showData },
    );
    r.respuesta = v.evento;
    if (v.clase === "error") {
      r.error = {
        code: v.error.code,
        class: v.error.class,
        message: v.error.message,
        ...(v.error.retryAfterMs !== undefined && { retryAfterMs: v.error.retryAfterMs }),
        origin: "remote",
      };
    } else {
      // 5. Con L3, `data` contra el esquema de la respuesta.
      const esquemaRespuesta = opts.validate !== false && indice ? cargado.bundle.schemas?.[indice.reply] : undefined;
      if (esquemaRespuesta) {
        const malos = await validarContraEsquema(esquemaRespuesta, v.evento.data);
        if (malos) {
          throw new ErrorRpc(
            "INVALID_SCHEMA",
            `la RESPUESTA no cumple ${indice.reply}: ${malos.join("; ")}. El fallo es del servicio que responde, no de esta llamada. ` +
              "Usa `--no-validate` para ver la respuesta tal cual.",
            { respuesta: v.evento },
          );
        }
      }
      r.ok = true;
    }
  } catch (e) {
    if (!(e instanceof ErrorRpc)) {
      if (nc) await nc.close().catch(() => {});
      throw e; // un fallo que no es de la llamada (conexión cerrada, bug): lo cuenta index.mjs
    }
    r.error = { code: e.code, class: e.clase, message: e.detalle, origin: "local" };
    if (e.respuesta) r.respuesta = e.respuesta;
  }
  if (nc) await nc.close().catch(() => {});

  if (opts.jsonOut) console.log(JSON.stringify(paraJson(r, opts.showData), null, 2));
  else imprimirHumano(r, { showData: opts.showData, verbose: opts.verbose });
  return r.ok ? 0 : 1;
}

/** Los streams que capturan `subject`, o `null` si no se sabe (sin permiso sobre la API de JetStream, o sin JetStream). */
async function streamsQueCapturan(nc, subject) {
  try {
    const m = await nc.request("$JS.API.STREAM.NAMES", JSON.stringify({ subject }), { timeout: 1500 });
    const j = JSON.parse(dec.decode(m.data));
    return Array.isArray(j.streams) ? j.streams : j.error ? null : [];
  } catch {
    return null;
  }
}
