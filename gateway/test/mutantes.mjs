#!/usr/bin/env node
/**
 * Mutantes del gateway: «una prueba que pasa con el código roto no vale».
 *
 * Por cada mutante se COPIA `src/` a `gateway/.mutantes/<n>/`, se estropea UNA garantía importante con un cambio mínimo y exacto
 * (la cadena a cambiar debe aparecer UNA vez: si el código cambia y el mutante deja de aplicarse, falla aquí) y se ejecutan las pruebas
 * que deben detectarlo, apuntadas a la copia con `FLUX_GATEWAY_SRC`. **Cada mutante DEBE hacer fallar sus pruebas.** Si alguno
 * sobrevive, este script sale con 1 y dice cuál: esa garantía no la vigila ninguna prueba.
 *
 *   node test/mutantes.mjs                 todos (varios minutos: los de extremo a extremo arrancan un broker)
 *   node test/mutantes.mjs --solo token    solo los que contengan «token» en el nombre
 *   node test/mutantes.mjs --lista         los nombres
 *
 * Primero comprueba que las pruebas PASAN sobre la copia sin estropear (si no, un «fallo» no demostraría nada).
 *
 * Entorno: el de las pruebas (NATS_SERVER, FLUX_TEST_PORTS, FLUX_REQUIRE_BROKER).
 */

import { spawnSync } from "node:child_process";
import { cpSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const AQUI = path.dirname(fileURLToPath(import.meta.url));
const GATEWAY = path.join(AQUI, "..");
const SRC = path.join(GATEWAY, "src");
const TMP = path.join(GATEWAY, ".mutantes");

const U = "auth.test.mjs";
const F = "firma.test.mjs";
const C = "config.test.mjs";
const E = "errores.test.mjs";
const IN = "e2e-entrada.test.mjs";
const HK = "e2e-hooks.test.mjs";
const SA = "e2e-salida.test.mjs";
const CI = "e2e-ciclo.test.mjs";

/** @type {Array<{ nombre: string, cambios: Array<[string, string, string]>, pruebas: string[] }>} [archivo, de, a] */
const MUTANTES = [
  // ─── El token ──────────────────────────────────────────────────────────────────────────────────────────────────
  { nombre: "comparar el token con === en lugar de en tiempo constante", pruebas: [U], cambios: [["auth.mjs", 'if (crypto.timingSafeEqual(presentado, t.digest) && m) acierto = acierto ?? t;', 'if (presentado.toString("hex") === t.digest.toString("hex") && m) acierto = acierto ?? t;']] },
  { nombre: "token: salir en la primera coincidencia (el tiempo dice cuál es)", pruebas: [U], cambios: [["auth.mjs", 'if (crypto.timingSafeEqual(presentado, t.digest) && m) acierto = acierto ?? t;', 'if (crypto.timingSafeEqual(presentado, t.digest) && m) return { cliente: t.cliente, tokenId: t.tokenId };']] },
  { nombre: "token: comparar el token en claro (sin el SHA-256)", pruebas: [U], cambios: [["auth.mjs", 'const presentado = crypto.createHash("sha256").update(m ? m[1] : "", "utf8").digest();', 'const presentado = Buffer.from((m ? m[1] : "").padEnd(32, "x").slice(0, 32));']] },
  { nombre: "sin token: dejar pasar como el primer cliente", pruebas: [IN], cambios: [["servidor.mjs", 'const quien = autenticador.autenticar(cabecera(req, "authorization"));', 'const quien = autenticador.autenticar(cabecera(req, "authorization")) ?? { cliente: "tienda-web", tokenId: "tienda-web-1" };']] },
  { nombre: "un 401 distinto si el token no existe", pruebas: [IN], cambios: [["servidor.mjs", 'throw errorEdge("EDGE_UNAUTHENTICATED", "falta un token válido: `Authorization: Bearer <token>`");', 'throw errorEdge("EDGE_UNAUTHENTICATED", cabecera(req, "authorization") ? "el token no es de ningún cliente" : "falta un token válido: `Authorization: Bearer <token>`");']] },
  { nombre: "las llamadas rpc salen por la identidad de tienda-web, sea cual sea el cliente (el broker ya no puede denegar)", pruebas: [IN], cambios: [["servidor.mjs", "const respuesta = await c.bus.request(operacion, data, opciones);", 'const respuesta = await clientes.get("tienda-web").bus.request(operacion, data, opciones);']] },
  { nombre: "los eventos salen por la identidad de tienda-web, sea cual sea el cliente (el broker ya no puede denegar)", pruebas: [IN], cambios: [["servidor.mjs", "const evento = await c.bus.publish(subject, data, opciones);", 'const evento = await clientes.get("tienda-web").bus.publish(subject, data, opciones);']] },
  { nombre: "el estado se escribe por la identidad de catalogo-web, sea cual sea el cliente (el broker ya no puede denegar)", pruebas: [IN], cambios: [["servidor.mjs", "const r = await c.bus.state.put(contrato, clave, data, opciones);", 'const r = await clientes.get("catalogo-web").bus.state.put(contrato, clave, data, opciones);']] },
  { nombre: "Flux-Tenant sin comprobar contra los tenants del cliente", pruebas: [IN], cambios: [["servidor.mjs", "if (!cliente.tenants.includes(t)) {", "if (false) {"]] },

  // ─── La firma ──────────────────────────────────────────────────────────────────────────────────────────────────
  { nombre: "firmar SIN el timestamp", pruebas: [F], cambios: [["firma.mjs", 'if (t !== null && t !== undefined) h.update(`${t}.`, "utf8");', ""]] },
  { nombre: "no verificar el HMAC", pruebas: [F, HK], cambios: [["firma.mjs", 'if (!acierta) return { ok: false, motivo: "firma" };', ""]] },
  { nombre: "la tolerancia solo por detrás (asimétrica)", pruebas: [F, HK], cambios: [["firma.mjs", "if (Math.abs(ahoraMs / 1000 - segundos) > toleranciaSeg)", "if (ahoraMs / 1000 - segundos > toleranciaSeg)"]] },
  { nombre: "sin tolerancia de tiempo", pruebas: [F, HK], cambios: [["firma.mjs", "if (Math.abs(ahoraMs / 1000 - segundos) > toleranciaSeg)", "if (false)"]] },
  { nombre: "decodificar el hexadecimal sin validar su formato", pruebas: [F], cambios: [["firma.mjs", "if (HEX64.test(v)) v1.push(v);", "v1.push(v);"]] },
  { nombre: "la firma comparada con === en lugar de en tiempo constante", pruebas: [F], cambios: [["firma.mjs", "if (iguales(esperado, recibido)) acierta = true;", "if (esperado === recibido) acierta = true;"]] },
  { nombre: "firma: salir en el primer v1 que acierta", pruebas: [F], cambios: [["firma.mjs", "for (const recibido of v1) if (iguales(esperado, recibido)) acierta = true;", "for (const recibido of v1) if (!acierta && iguales(esperado, recibido)) acierta = true;"]] },
  { nombre: "un solo secreto (sin rotación)", pruebas: [F], cambios: [["firma.mjs", 'return `t=${t},${secretos.map((s) => `v1=${hmacHex(s, t, cuerpo)}`).join(",")}`;', 'return `t=${t},v1=${hmacHex(secretos[0], t, cuerpo)}`;']] },
  { nombre: "el instante de la firma FIJO en todos los intentos (no se renueva)", pruebas: [SA], cambios: [["entrega.mjs", "const t = Math.floor(reloj() / 1000);", "const t = 1787221600;"]] },

  // ─── Los webhooks de terceros ──────────────────────────────────────────────────────────────────────────────────
  { nombre: "id aleatorio en el webhook (sin externalKey ni time)", pruebas: [HK], cambios: [["hooks.mjs", "    opciones.externalKey = `${h.nombre}:${k}`;\n    opciones.time = new Date(ms).toISOString();\n", ""]] },
  { nombre: "la clave externa sin el nombre del hook", pruebas: [HK], cambios: [["hooks.mjs", "opciones.externalKey = `${h.nombre}:${k}`;", "opciones.externalKey = `${k}`;"]] },
  { nombre: "el instante del evento «ahora» en lugar del del hecho", pruebas: [HK], cambios: [["hooks.mjs", "opciones.time = new Date(ms).toISOString();", "opciones.time = new Date().toISOString();"]] },
  { nombre: "no rechazar una firma de webhook inválida (t-v1)", pruebas: [HK], cambios: [["hooks.mjs", 'if (!r.ok) throw errorEdge("EDGE_INVALID_WEBHOOK_SIGNATURE", `la firma del webhook no verifica (${r.motivo}): revisa el secreto y que el cuerpo no se reserialice por el camino`);', ""]] },
  { nombre: "no rechazar una firma de webhook inválida (hex / sha256=)", pruebas: [HK], cambios: [["hooks.mjs", "} else if (!verificarHmacSimple({", "} else if (false && !verificarHmacSimple({"]] },
  { nombre: "el webhook publica el cuerpo entero (con el email) en lugar de los campos elegidos", pruebas: [HK], cambios: [["hooks.mjs", 'if (h.datos.modo === "pick") {', "if (false) {"]] },
  { nombre: "el webhook publica como internal en lugar de confidential", pruebas: [HK], cambios: [["hooks.mjs", "const opciones = { tenantId, classification: h.classification };", 'const opciones = { tenantId, classification: "internal" };']] },
  { nombre: "interpretar el cuerpo ANTES de verificar la firma (un JSON roto da 400, no 401)", pruebas: [HK], cambios: [["hooks.mjs", "  // 1. La firma, sobre los bytes en bruto.\n", "  try { JSON.parse(UTF8.decode(cuerpo)); } catch { throw errorEdge(\"EDGE_INVALID_BODY\", \"el cuerpo del webhook no es JSON válido en UTF-8\"); }\n  // 1. La firma, sobre los bytes en bruto.\n"]] },

  // ─── La entrega saliente ───────────────────────────────────────────────────────────────────────────────────────
  { nombre: "una redirección 3xx cuenta como entregada", pruebas: [E, SA], cambios: [["entrega.mjs", "  if (status >= 200 && status <= 299) return null;\n  const code", "  if (status >= 200 && status <= 399) return null;\n  const code"]] },
  { nombre: "un 4xx permanente se reintenta", pruebas: [E, SA], cambios: [["entrega.mjs", "  return { class: ErrorClass.PERMANENT, code };\n}\n\n/** La clasificación de un fallo de red", "  return { class: ErrorClass.RETRYABLE, code };\n}\n\n/** La clasificación de un fallo de red"]] },
  { nombre: "un 500 con el presupuesto completo (no acotado a 2)", pruebas: [E, SA], cambios: [["entrega.mjs", "return { class: ErrorClass.RETRYABLE, code, maxAttempts: PRESUPUESTO_ACOTADO, ...(retryAfterMs !== undefined && { retryAfterMs }) };", "return { class: ErrorClass.RETRYABLE, code, ...(retryAfterMs !== undefined && { retryAfterMs }) };"]] },
  { nombre: "un 503 sin Retry-After (se ignora)", pruebas: [E, SA], cambios: [["entrega.mjs", "return ms > 0 ? Math.min(ms, RETRY_AFTER_MAX_MS) : undefined;", "return undefined;"]] },
  { nombre: "Retry-After sin tope", pruebas: [E], cambios: [["entrega.mjs", "return ms > 0 ? Math.min(ms, RETRY_AFTER_MAX_MS) : undefined;", "return ms > 0 ? ms : undefined;"]] },
  { nombre: "un 2xx que no es ack (todo se reintenta)", pruebas: [E, SA], cambios: [["entrega.mjs", "if (status >= 200 && status <= 299) return null;", "if (status === 200) return null;"]] },
  { nombre: "un certificado que no verifica como reintentable", pruebas: [E], cambios: [["entrega.mjs", 'if (code && TLS_NO_VERIFICA.has(code)) return { class: ErrorClass.PERMANENT, code: "TLS_VERIFICATION_FAILED" };', 'if (code && TLS_NO_VERIFICA.has(code)) return { class: ErrorClass.RETRYABLE, code: "TLS_VERIFICATION_FAILED" };']] },
  { nombre: "la entrega no comprueba la clasificación (confidential y restricted salen)", pruebas: [SA], cambios: [["entrega.mjs", 'if (evento.dataclassification === "restricted" || !entrega.allowClassifications.includes(evento.dataclassification)) {', "if (false) {"]] },
  { nombre: "restricted sale si la entrega lo lista", pruebas: [SA], cambios: [["entrega.mjs", 'if (evento.dataclassification === "restricted" || !entrega.allowClassifications.includes(evento.dataclassification)) {', 'if (!entrega.allowClassifications.includes(evento.dataclassification) && evento.dataclassification !== "restricted") {']] },
  { nombre: "el cuerpo de la entrega no es el CloudEvent (solo `data`)", pruebas: [SA], cambios: [["entrega.mjs", "const cuerpo = Buffer.from(serialize(evento));", "const cuerpo = Buffer.from(JSON.stringify(evento.data));"]] },
  { nombre: "el cuerpo de la entrega reserializado con espacios", pruebas: [SA], cambios: [["entrega.mjs", "const cuerpo = Buffer.from(serialize(evento));", "const cuerpo = Buffer.from(JSON.stringify(evento, null, 1));"]] },
  { nombre: "Idempotency-Key aleatoria en cada intento", pruebas: [SA], cambios: [["entrega.mjs", '"idempotency-key": idempotencyKey,', '"idempotency-key": String(Math.random()),']] },
  { nombre: "firmar otra cosa que los bytes enviados", pruebas: [SA], cambios: [["entrega.mjs", 'firmar({ secretos: entrega.secretos, t, cuerpo })', 'firmar({ secretos: entrega.secretos, t, cuerpo: cuerpo.subarray(0, cuerpo.length - 1) })']] },
  { nombre: "el Content-Type de la entrega application/json", pruebas: [SA], cambios: [["entrega.mjs", 'contentType: "application/cloudevents+json; charset=utf-8"', 'contentType: "application/json"']] },
  { nombre: "sin plazo total: un receptor mudo cuelga el intento", pruebas: [SA], cambios: [["entrega.mjs", "const temporizador = setTimeout(() => req.destroy(Object.assign(new Error(\"plazo agotado\"), { code: \"PLAZO\" })), timeoutMs);", "const temporizador = setTimeout(() => {}, timeoutMs);"]] },

  // ─── Límites ───────────────────────────────────────────────────────────────────────────────────────────────────
  { nombre: "sin límite de cuerpo (ni declarado ni *chunked*)", pruebas: [IN, HK], cambios: [["cuerpo.mjs", "if (declarado !== undefined && Number(declarado) > limite) return Promise.reject(new CuerpoDemasiadoGrande(limite, Number(declarado)));", ""], ["cuerpo.mjs", "if (total > limite) return fin(reject, new CuerpoDemasiadoGrande(limite));", ""], ["servidor.mjs", 'if (Number(req.headers["content-length"]) > limites.maxBodyBytes) throw new CuerpoDemasiadoGrande(limites.maxBodyBytes, Number(req.headers["content-length"]));', ""]] },
  { nombre: "un 413 sin cerrar la conexión (Node lee y descarta todo lo declarado)", pruebas: [IN], cambios: [["servidor.mjs", 'res.once("finish", () => req.destroy());', ""], ["servidor.mjs", 'responder(res, err.status, cuerpoDeError(err), { connection: "close" });', "responder(res, err.status, cuerpoDeError(err));"]] },
  { nombre: "un cuerpo declarado grande se lee antes de rechazarlo", pruebas: [IN], cambios: [["servidor.mjs", 'if (Number(req.headers["content-length"]) > limites.maxBodyBytes) throw new CuerpoDemasiadoGrande(limites.maxBodyBytes, Number(req.headers["content-length"]));', ""], ["cuerpo.mjs", "if (declarado !== undefined && Number(declarado) > limite) return Promise.reject(new CuerpoDemasiadoGrande(limite, Number(declarado)));", ""], ["cuerpo.mjs", "if (total > limite) return fin(reject, new CuerpoDemasiadoGrande(limite));", "if (total > 200 * limite) return fin(reject, new CuerpoDemasiadoGrande(limite));"]] },
  { nombre: "el límite por token no se aplica", pruebas: [CI], cambios: [["servidor.mjs", 'if (!lim.ok) throw errorEdge("EDGE_RATE_LIMITED", `se supera', 'if (false) throw errorEdge("EDGE_RATE_LIMITED", `se supera']] },
  { nombre: "un solo cubo para todos los tokens", pruebas: [CI], cambios: [["servidor.mjs", "const lim = (limitesPropios.get(quien.cliente) ?? limitePorToken).tomar(quien.tokenId);", 'const lim = (limitesPropios.get(quien.cliente) ?? limitePorToken).tomar("todos");']] },
  { nombre: "el límite de los webhooks no se aplica", pruebas: [CI], cambios: [["servidor.mjs", 'if (!lim.ok) throw errorEdge("EDGE_RATE_LIMITED", "demasiadas peticiones a este hook"', 'if (false) throw errorEdge("EDGE_RATE_LIMITED", "demasiadas peticiones a este hook"']] },
  { nombre: "el intervalo de comprobación de Node por defecto (los timeouts no se aplican)", pruebas: [CI], cambios: [["servidor.mjs", "connectionsCheckingInterval: limites.connectionsCheckingIntervalMs,", ""]] },
  { nombre: "server.timeout por debajo del timeout máximo del rpc", pruebas: [IN], cambios: [["servidor.mjs", "servidor.timeout = limites.serverTimeoutMs;", "servidor.timeout = 1000;"]] },

  // ─── Parada ────────────────────────────────────────────────────────────────────────────────────────────────────
  { nombre: "parada sin Connection: close en las respuestas", pruebas: [CI], cambios: [["servidor.mjs", '...(estado.parando && { connection: "close" }),', ""]] },
  { nombre: "parada que no espera a lo que vuela", pruebas: [CI], cambios: [["servidor.mjs", "while (estado.enVuelo > 0 && Date.now() < limite) {", "while (false) {"]] },
  { nombre: "parada sin plazo máximo (espera eternamente)", pruebas: [CI], cambios: [["servidor.mjs", "while (estado.enVuelo > 0 && Date.now() < limite) {", "while (estado.enVuelo > 0) {"]] },
  { nombre: "un gateway a medias: un cliente que no conecta no tumba el arranque", pruebas: [CI], cambios: [["gateway.mjs", "const bus = await conectar(opcionesDeConexion(config, c, registro));", "const bus = await conectar(opcionesDeConexion(config, c, registro)).catch(() => ({ connected: false, close: async () => {} }));"]] },
  { nombre: "el arranque con Promise.all: las conexiones en vuelo no se cierran al fallar otra", pruebas: [CI], cambios: [["gateway.mjs", "const resultados = await Promise.allSettled(", "const resultados = await Promise.all("], ["gateway.mjs", "const fallo = resultados.find((r) => r.status === \"rejected\");\n    if (fallo) throw fallo.reason;", ""]] },

  // ─── Registros ─────────────────────────────────────────────────────────────────────────────────────────────────
  { nombre: "registrar el `data` de lo que se publica", pruebas: [IN], cambios: [["registro.mjs", '"host", "tenant",', '"host", "tenant", "datos",'], ["servidor.mjs", "ctx.id = evento.id;", "ctx.id = evento.id; ctx.datos = JSON.stringify(data);"]] },
  { nombre: "registrar el token", pruebas: [IN], cambios: [["registro.mjs", '"host", "tenant",', '"host", "tenant", "autorizacion",'], ["servidor.mjs", "ctx.tokenId = quien.tokenId;", "ctx.tokenId = quien.tokenId; ctx.autorizacion = cabecera(req, \"authorization\");"]] },
  { nombre: "registrar el mensaje de un error con el texto de la excepción", pruebas: [E], cambios: [["errores.mjs", 'return errorEdge("EDGE_INTERNAL", "el gateway no pudo atender la petición por un fallo interno; no es un error de la petición. Reintentarla no lo arregla: avisa a quien opera el gateway (el detalle está en su registro)");', "return errorEdge(\"EDGE_INTERNAL\", mensaje);"]] },

  // ─── Errores HTTP ──────────────────────────────────────────────────────────────────────────────────────────────
  { nombre: "RPC_TIMEOUT como 408", pruebas: [E], cambios: [["errores.mjs", "RPC_TIMEOUT: { status: 504,", "RPC_TIMEOUT: { status: 408,"]] },
  { nombre: "PERMISSION_DENIED como 401", pruebas: [E], cambios: [["errores.mjs", "PERMISSION_DENIED: { status: 403,", "PERMISSION_DENIED: { status: 401,"]] },
  { nombre: "STATE_CONFLICT como no reintentable", pruebas: [E], cambios: [["errores.mjs", 'STATE_CONFLICT: { status: 409, clase: "retryable" }', 'STATE_CONFLICT: { status: 409, clase: "permanent" }']] },
  { nombre: "un error del que responde pierde su code", pruebas: [E, IN], cambios: [["errores.mjs", 'return new ErrorHttp(422, code ?? "PERMANENT", "permanent", mensaje);', 'return new ErrorHttp(422, "PERMANENT", "permanent", mensaje);']] },
  { nombre: "un evento de más de 1 MiB como 422 en lugar de 413", pruebas: [E, IN], cambios: [["errores.mjs", 'return /supera el l[ií]mite/.test(mensaje) ? errorEdge("EDGE_BODY_TOO_LARGE", mensaje) : errorEdge("EDGE_INVALID_REQUEST", mensaje);', 'return errorEdge("EDGE_INVALID_REQUEST", mensaje);']] },

  // ─── Configuración ─────────────────────────────────────────────────────────────────────────────────────────────
  { nombre: "gateway.json acepta un secreto en claro", pruebas: [C], cambios: [["config.mjs", "if (CLAVES_SECRETAS.has(k.toLowerCase()) && typeof x === \"string\") {", "if (false) {"]] },
  { nombre: "gateway.json acepta una semilla de NATS en un valor", pruebas: [C], cambios: [["config.mjs", "if (SEMILLA_NATS.test(v)) err(", "if (false) err("]] },
  { nombre: "dos entregas para el mismo subject", pruebas: [C], cambios: [["config.mjs", "if (vistas.has(clave)) err(", "if (false) err("]] },
  { nombre: "dos hooks con el mismo nombre", pruebas: [C], cambios: [["config.mjs", "if (hooksVistos.has(nombre)) err(", "if (false) err("]] },
  { nombre: "un token compartido entre clientes", pruebas: [C], cambios: [["config.mjs", "if (digests.has(t.sha256)) err(", "if (false) err("]] },
  { nombre: "una variable de entorno que falta no se avisa", pruebas: [C], cambios: [["config.mjs", "if (faltan.size) err(", "if (false) err("]] },
  { nombre: "http:// sin la decisión explícita", pruebas: [C], cambios: [["config.mjs", 'if (url?.protocol === "http:" && def.allowInsecureHttp !== true) err(`${donde}: la URL es', 'if (false) err(`${donde}: la URL es']] },
  { nombre: "restricted permitido en una entrega", pruebas: [C], cambios: [["config.mjs", 'else if (permitidasClas.includes("restricted")) err(', "else if (false) err("]] },
  { nombre: "una entrega de estado sin autorizar confidential", pruebas: [C], cambios: [["config.mjs", 'if (esEstado && Array.isArray(permitidasClas) && !permitidasClas.includes("confidential")) err(', "if (false) err("]] },
  { nombre: "un hook sin externalKey ni time (id aleatorio sin avisar)", pruebas: [C], cambios: [["config.mjs", "if (idempotente) {", "if (false) {"]] },
];

// ─── El runner ───────────────────────────────────────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
if (args.includes("--lista")) {
  for (const [i, m] of MUTANTES.entries()) console.log(`${String(i + 1).padStart(2)}. ${m.nombre}  [${m.pruebas.join(", ")}]`);
  process.exit(0);
}
const solo = args.includes("--solo") ? args[args.indexOf("--solo") + 1] : undefined;

function ejecutar(srcDir, pruebas) {
  const r = spawnSync(process.execPath, ["--test", "--test-force-exit", "--test-reporter=tap", "--test-concurrency=1", ...pruebas.map((p) => path.join(AQUI, p))], {
    env: { ...process.env, FLUX_GATEWAY_SRC: srcDir },
    encoding: "utf8",
    cwd: GATEWAY,
    timeout: 20 * 60_000,
  });
  return { codigo: r.status, salida: `${r.stdout ?? ""}${r.stderr ?? ""}` };
}

rmSync(TMP, { recursive: true, force: true });
mkdirSync(TMP, { recursive: true });

// 0. Las pruebas pasan sobre la copia SIN estropear.
const todasLasPruebas = [...new Set(MUTANTES.flatMap((m) => m.pruebas))].filter((p) => !solo || MUTANTES.some((m) => m.nombre.includes(solo) && m.pruebas.includes(p)));
{
  const limpia = path.join(TMP, "limpia");
  cpSync(SRC, limpia, { recursive: true });
  const r = ejecutar(limpia, todasLasPruebas);
  if (r.codigo !== 0) {
    console.error(`✗ las pruebas NO pasan sobre el código sin estropear (exit ${r.codigo}): un «fallo» de un mutante no demostraría nada.\n${r.salida.split("\n").slice(-40).join("\n")}`);
    process.exit(2);
  }
  console.log(`✓ las pruebas pasan sobre el código sin estropear (${todasLasPruebas.join(", ")})\n`);
}

let detectados = 0;
const supervivientes = [];
const elegidos = MUTANTES.filter((m) => !solo || m.nombre.includes(solo));
for (const [i, m] of elegidos.entries()) {
  const dir = path.join(TMP, String(i));
  cpSync(SRC, dir, { recursive: true });
  for (const [archivo, de, a] of m.cambios) {
    const ruta = path.join(dir, archivo);
    const texto = readFileSync(ruta, "utf8");
    const veces = texto.split(de).length - 1;
    if (veces !== 1) {
      console.error(`✗ el mutante «${m.nombre}» NO se aplica: «${de.slice(0, 70)}…» aparece ${veces} veces en src/${archivo} (debe ser 1). El código cambió: actualiza el mutante.`);
      process.exit(2);
    }
    writeFileSync(ruta, texto.replace(de, () => a));
  }
  const t0 = Date.now();
  const r = ejecutar(dir, m.pruebas);
  const ms = Date.now() - t0;
  if (r.codigo !== 0) {
    detectados++;
    // QUÉ prueba lo detectó: una detección «por un fallo cualquiera» (un import roto, un arranque que no llega a arrancar) no demuestra que la
    // garantía esté vigilada. Se muestran las pruebas hoja que fallaron, las de mayor sangría en la salida TAP.
    const fallan = [...r.salida.matchAll(/^( +)not ok \d+ - (.+)$/gm)].map((x) => ({ n: x[1].length, t: x[2] }));
    const hondo = Math.max(0, ...fallan.map((f) => f.n));
    const nombres = [...new Set(fallan.filter((f) => f.n === hondo).map((f) => f.t))];
    console.log(`  ✓ detectado   ${m.nombre}  (${(ms / 1000).toFixed(1)} s)${nombres.length ? `\n                  ← ${nombres.slice(0, 2).map((t) => t.slice(0, 110)).join("\n                  ← ")}${nombres.length > 2 ? `\n                  ← (+${nombres.length - 2} más)` : ""}` : "\n                  ← (sin una prueba concreta: ¿un fallo de arranque?)"}`);
  } else {
    supervivientes.push(m.nombre);
    console.log(`  ✗ SOBREVIVE   ${m.nombre}  (${(ms / 1000).toFixed(1)} s): ${m.pruebas.join(", ")} pasan con el código roto`);
  }
  rmSync(dir, { recursive: true, force: true });
}
rmSync(TMP, { recursive: true, force: true });

console.log(`\n${detectados}/${elegidos.length} mutantes detectados`);
if (supervivientes.length) {
  console.error(`✗ ${supervivientes.length} superviviente(s): ${supervivientes.join("; ")}`);
  process.exit(1);
}
