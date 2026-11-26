/**
 * Modelo del estado replicado (specification/10-state.md) para la CLI: nombres, validación,
 * subjects, durables y lectura del sobre. Sin red: todo lo de aquí se prueba sin broker.
 *
 * La CLI **no usa el SDK de flux** (una herramienta construida sobre el SDK no puede
 * diagnosticar los fallos del propio SDK), así que repite el algoritmo de nombres y las reglas de
 * validación. Para que no puedan divergir de la spec en silencio, **ninguna constante vive
 * aquí**: los patrones, los límites y la configuración canónica se leen de `protocol.json`
 * (bloque `state`), que `scripts/check-protocol-json.mjs` vigila contra la prosa.
 *
 * Los mensajes de error llevan el código de 04-errors.md §6 (`INVALID_STATE_KEY`…) para que lo
 * que dice la CLI sea lo mismo que dice un SDK ante la misma entrada.
 */

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { nanos } from "@nats-io/transport-node";
import { AckPolicy, DeliverPolicy, ReplayPolicy } from "@nats-io/jetstream";

const RAIZ = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const dec = new TextDecoder();

/** Un error de entrada del operador: se imprime tal cual, sin traza. */
export class ErrorEstado extends Error {
  constructor(code, message) {
    super(code ? `${code}: ${message}` : message);
    this.name = "ErrorEstado";
    this.code = code;
  }
}

// ─── Lo canónico, de protocol.json (carga perezosa) ──────────────────────────────────────────────
//
// Perezosa a propósito: `flux validate` y `flux keygen` no necesitan protocol.json, y un
// checkout sin él no debe impedir que arranque la CLI entera.

let _canon;

/**
 * `protocol.json` ya interpretado: los regex compilados y el bloque `state` tal cual. Cada
 * campo se llama como en el fichero para que `grep` encuentre de dónde sale.
 */
export function canon() {
  if (_canon) return _canon;
  const p = JSON.parse(readFileSync(join(RAIZ, "protocol.json"), "utf8"));
  const s = p.state;
  if (!s) throw new Error("protocol.json no tiene el bloque `state`: ¿es un checkout anterior a la 1.1?");
  _canon = {
    estado: s,
    consumidor: p.consumer,
    reservados: p.naming.reservedDomains.names,
    reContrato: new RegExp(s.contract.pattern),
    reTenant: new RegExp(s.tenant.pattern),
    maxTenant: s.tenant.maxLength,
    reClave: new RegExp(s.key.pattern),
    reSubject: new RegExp(s.subject.pattern),
    reServicio: new RegExp(p.naming.service.pattern),
    reDurable: new RegExp(s.watch.durable.pattern),
    reTipoValor: new RegExp(s.type.value.pattern),
    reTipoLapida: new RegExp(s.type.tombstone.pattern),
    reSubjectDlq: new RegExp(s.dlq.subject.pattern),
  };
  return _canon;
}

// ─── Validación (10-state.md §2.3, §2.4, §2.7) ───────────────────────────────────────────────────

/**
 * `clientes.cliente.v1` → `{ dom, agr, major, contrato }`. Es lo que recibe `state.watch`: el
 * subject sin `state.`, sin tenant y sin clave.
 */
export function validarContrato(texto) {
  const k = canon();
  if (typeof texto !== "string" || !k.reContrato.test(texto)) {
    const t = typeof texto === "string" ? texto : String(texto);
    const pista = t.startsWith("state.")
      ? " Sin el prefijo `state.`: el contrato es <dominio>.<agregado>.v<major>."
      : t.split(".").length > 3
        ? " Tiene más de 3 tokens: el contrato es el subject de estado sin `state.`, sin tenant y sin clave."
        : "";
    throw new ErrorEstado(
      null,
      `contrato inválido «${t}»: el formato es <dominio>.<agregado>.v<major> (p.ej. clientes.cliente.v1).${pista}`,
    );
  }
  const [dom, agr, major] = texto.split(".");
  if (k.reservados.includes(dom)) {
    throw new ErrorEstado(
      "RESERVED_DOMAIN",
      `«${dom}» no puede ser un dominio: ${k.reservados.map((r) => `${r}.`).join(" ")} son espacios de nombres del protocolo (10-state.md §2.7).`,
    );
  }
  return { dom, agr, major, contrato: texto };
}

export function validarTenant(tenant) {
  const k = canon();
  if (typeof tenant !== "string" || !k.reTenant.test(tenant) || tenant.length > k.maxTenant) {
    throw new ErrorEstado(
      "INVALID_TENANT_FOR_STATE",
      `el tenant «${tenant}» no vale para un estado: kebab-case en minúsculas, de 1 a ${k.maxTenant} caracteres ` +
        `(${k.estado.tenant.pattern}). No hay tenant por defecto (10-state.md §2.4).`,
    );
  }
  return tenant;
}

/**
 * Con coincidencia COMPLETA. El broker no valida nada de esto (A-41): acepta `*`, `>`, `a.b`
 * (dos tokens: la entrada queda fuera de todo filtro, sin error) y 2000 caracteres. Y un `*`
 * llegaría al DIRECT.GET como comodín y devolvería OTRA clave.
 */
export function validarClave(clave) {
  const k = canon();
  if (typeof clave !== "string" || !k.reClave.test(clave)) {
    throw new ErrorEstado(
      "INVALID_STATE_KEY",
      `la clave «${clave}» no cumple ${k.estado.key.pattern}: un único token de NATS, sin puntos ni ` +
        `comodines, de 1 a ${k.estado.key.maxLength} caracteres y sin empezar por «_» (10-state.md §2.3).`,
    );
  }
  return clave;
}

export function validarServicio(servicio) {
  if (typeof servicio !== "string" || !canon().reServicio.test(servicio)) {
    throw new ErrorEstado(
      null,
      `nombre de servicio inválido «${servicio}»: kebab-case en minúsculas (el que pasa a connect() como serviceName).`,
    );
  }
  return servicio;
}

// ─── Nombres (10-state.md §2.1, §2.2, §5.2) ──────────────────────────────────────────────────────

const aNombreNats = (t) => t.replace(/-/g, "_").toUpperCase();

/** `clientes` → `STATE_CLIENTES`; `linea-envio` → `STATE_LINEA_ENVIO`. */
export const streamDe = (dom) => `STATE_${aNombreNats(dom)}`;
export const dlqStreamDe = (dom) => `DLQ_${aNombreNats(dom)}`;

export const subjectDe = (contrato, tenant, clave) => `state.${contrato}.${tenant}.${clave}`;

/** El filtro del durable: `…v1.acme.*`, o `…v1.*.*` sin tenant (10-state.md §5.2). */
export const filtroDe = (contrato, tenant) => `state.${contrato}.${tenant ?? "*"}.*`;

/**
 * `<servicio>__state_<dom>_<agr>_v<M>` y, si filtra por tenant, `__<tenant>`: los puntos y los
 * guiones del CONTRATO y del tenant pasan a `_`; el nombre del servicio conserva los suyos.
 * Si el tenant no fuese en el nombre, un durable compartido REPARTIRÍA los mensajes entre sus
 * clientes (A-47) y cada uno vería la mitad de la foto.
 */
export function durableDe(servicio, contrato, tenant) {
  const nombre =
    `${servicio}__state_${contrato.replace(/[.-]/g, "_")}` + (tenant ? `__${tenant.replace(/-/g, "_")}` : "");
  const k = canon();
  if (!k.reDurable.test(nombre)) {
    throw new ErrorEstado(null, `el durable calculado «${nombre}» no cumple ${k.estado.watch.durable.pattern}`);
  }
  if (nombre.length > 255) {
    throw new ErrorEstado(null, `el durable calculado mide ${nombre.length} caracteres y NATS admite 255 como máximo (A-42)`);
  }
  return nombre;
}

/**
 * `state.clientes.cliente.v1.acme.cli-1` → sus partes; `null` si no es un subject de estado
 * canónico (p.ej. la clave `a.b` de A-41 deja 7 tokens: es una entrada fantasma).
 */
export function parsearSubject(subject) {
  const k = canon();
  if (!k.reSubject.test(subject)) return null;
  const [, dom, agr, major, tenant, clave] = subject.split(".");
  if (tenant.length > k.maxTenant) return null;
  return { dom, agr, major, contrato: `${dom}.${agr}.${major}`, tenant, clave };
}

/**
 * ¿Todo subject que encaja con `objetivo` encaja también con `patron`? Patrones de NATS: `*`
 * es un token, `>` es el resto (al menos uno). Sirve para saber si un stream CAPTURA un subject
 * (`DLQ_CLIENTES` y `dlq.state.clientes.>`), que es lo que decide si un fallo tiene dónde caer.
 */
export function cubre(patron, objetivo) {
  const p = patron.split(".");
  const o = objetivo.split(".");
  for (let i = 0; i < p.length; i++) {
    if (p[i] === ">") return o.length > i;
    if (o[i] === undefined) return false;
    if (o[i] === ">") return false; // el objetivo traga más de lo que el patrón alcanza
    if (p[i] === "*") continue;
    if (p[i] !== o[i]) return false;
  }
  return p.length === o.length;
}

// ─── El durable de un observador (10-state.md §5.2) ──────────────────────────────────────────────

/**
 * La configuración EXACTA de un durable de estado: `deliver_policy: last_per_subject` y todo
 * lo demás, la canónica de 03-delivery.md §2 (la de `protocol.json` → `consumer`). Es la que
 * crea `flux state resync`, y la contra la que `flux doctor` compara los durables que existen.
 */
export function configDurableEstado(durable, filtro) {
  const k = canon().consumidor;
  return {
    durable_name: durable,
    filter_subject: filtro,
    ack_policy: AckPolicy.Explicit,
    ack_wait: nanos(k.ackWaitSeconds * 1000),
    max_deliver: k.maxDeliver,
    backoff: k.backoffMs.map((ms) => nanos(ms)),
    max_ack_pending: k.maxAckPending,
    deliver_policy: DeliverPolicy.LastPerSubject,
    replay_policy: ReplayPolicy.Instant,
  };
}

/** Los campos de un durable que se comparan con la canónica. */
export const CAMPOS_DURABLE = [
  "filter_subject", "ack_policy", "ack_wait", "max_deliver", "backoff",
  "max_ack_pending", "deliver_policy", "replay_policy",
];

/** `filter_subject`, o el único de `filter_subjects`. */
export const filtroDeConfig = (cfg) =>
  cfg.filter_subject ?? (cfg.filter_subjects?.length === 1 ? cfg.filter_subjects[0] : undefined);

/** Diferencias campo a campo entre lo deseado y lo que el servidor tiene. */
export function diferenciasConfig(deseado, existente, campos = CAMPOS_DURABLE) {
  const ex = { ...existente, filter_subject: filtroDeConfig(existente) };
  const out = [];
  for (const campo of campos) {
    if (JSON.stringify(deseado[campo]) !== JSON.stringify(ex[campo])) {
      out.push({ campo, deseado: deseado[campo], existente: ex[campo] });
    }
  }
  return out;
}

// ─── Leer una entrada (10-state.md §5.4) ─────────────────────────────────────────────────────────

/**
 * Lo que la CLI sabe de un mensaje de estado: si es una lápida, su versión y **qué le pasa**.
 * No lanza ni lo descarta: una herramienta de diagnóstico tiene que MOSTRAR lo roto, no
 * negarse a leerlo como haría un consumidor (§5.4: ahí un mensaje así es POISON y no llega al
 * handler).
 *
 * `info` es `parsearSubject(subject)`; con `null` (subject no canónico) no se compara identidad.
 */
export function inspeccionar(bytes, info) {
  const k = canon();
  const r = { legible: false, envelope: null, lapida: false, aggregateVersion: undefined, problemas: [] };
  const falla = (code, texto) => r.problemas.push({ code, texto });

  let e;
  try {
    e = JSON.parse(dec.decode(bytes));
  } catch {
    falla(null, "el cuerpo no es JSON");
    return r;
  }
  if (e === null || typeof e !== "object" || Array.isArray(e)) {
    falla(null, "el cuerpo no es un objeto JSON");
    return r;
  }
  r.legible = true;
  r.envelope = e;

  const tipo = typeof e.type === "string" ? e.type : "";
  r.lapida = k.reTipoLapida.test(tipo);
  if (!r.lapida && !k.reTipoValor.test(tipo)) {
    falla("STATE_SUBJECT_MISMATCH", `type «${tipo}» no es el de un estado (com.flux.state.<dominio>.<agregado>.v<major>)`);
  }

  const v = e.data?.aggregateVersion;
  if (Number.isSafeInteger(v) && v >= 1) r.aggregateVersion = v;
  else falla("MISSING_AGGREGATE_VERSION", "data no tiene un aggregateVersion entero ≥ 1");

  if (info) {
    if (e.tenantid !== info.tenant) {
      falla("STATE_SUBJECT_MISMATCH", `tenantid del sobre «${e.tenantid}» ≠ «${info.tenant}» del subject`);
    }
    if (e.subject !== info.clave) {
      falla("STATE_SUBJECT_MISMATCH", `subject del sobre «${e.subject}» ≠ clave «${info.clave}» del subject`);
    }
    const esperado = r.lapida
      ? `com.flux.state.${info.dom}.${info.agr}.deleted.${info.major}`
      : `com.flux.state.${info.dom}.${info.agr}.${info.major}`;
    if (tipo && (k.reTipoValor.test(tipo) || r.lapida) && tipo !== esperado) {
      falla("STATE_SUBJECT_MISMATCH", `type «${tipo}» no es el del contrato (esperado «${esperado}»)`);
    }
  }
  if (e.dataclassification === "restricted") {
    falla("INVALID_DATACLASSIFICATION", "restricted no se admite en estado (10-state.md §7): un consumidor lo trataría como POISON");
  }
  return r;
}

/**
 * El `data` de un evento de DLQ de estado es EXACTAMENTE `{ key, revision }` (10-state.md §6):
 * la DLQ guarda 90 días, y un dato personal ya borrado seguiría vivo ahí tres meses. Si lleva
 * algo más, el SDK que lo escribió está copiando el dato: se avisa en vez de imprimirlo.
 */
export function resumenDlqEstado(e) {
  const d = e?.data;
  const claves = d !== null && typeof d === "object" && !Array.isArray(d) ? Object.keys(d) : [];
  const extra = claves.filter((k) => k !== "key" && k !== "revision");
  const ok = extra.length === 0 && typeof d?.key === "string" && Number.isInteger(d?.revision);
  return { ok, key: d?.key, revision: d?.revision, extra: ok ? [] : extra.length ? extra : ["forma inválida"] };
}
