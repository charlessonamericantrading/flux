#!/usr/bin/env node
/**
 * Mutantes del SDK de Java: una prueba que solo se ha visto en verde no demuestra que detecte nada.
 *
 *   node sdk-java/mutantes.mjs                  # todos
 *   node sdk-java/mutantes.mjs --only pull,host # los que contengan esas palabras
 *   node sdk-java/mutantes.mjs --list
 *
 * Cada mutante ESTROPEA A PROPÓSITO una garantía importante en una COPIA del SDK (`sdk-java/target/mutantes/<id>/`; el código del
 * repo no se toca) y ejecuta las pruebas que deben vigilarla: DEBEN fallar. Antes de mutar se ejecutan SIN mutar: si no pasan, no
 * demuestran nada. Un mutante que no rompe ninguna prueba, o que ni siquiera compila, hace fallar el script.
 *
 * Necesita lo mismo que las pruebas con broker (NATS_SERVER, node, openssl, `npm install` en cli/) y, sin red, `MVN_OFFLINE=1`.
 * Sale con 0 si todos fueron detectados, 1 si alguno no, 2 si el entorno está mal montado.
 */
import { spawnSync } from "node:child_process";
import { cpSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SDK = dirname(fileURLToPath(import.meta.url));
const COPIAS = join(SDK, "target", "mutantes");
const F = "src/main/java/com/flux/";

/** { id, que, archivo, de, a, pruebas }: `de` debe aparecer EXACTAMENTE una vez en `archivo`. */
const MUTANTES = [
  // ── seguridad ──
  { id: "primera-conexion-de-la-libreria", que: "esperar al broker con `connectReconnectOnConnect` en vez del bucle propio (A-200): se cuelga ante lo que esperar no arregla",
    archivo: "FluxBus.java", de: "return Nats.connect(built);", a: "return Nats.connectReconnectOnConnect(built);", pruebas: ["TlsIntegrationTest#caQueNoFirmo"], cuelga: true },
  { id: "auth-como-transitorio", que: "una credencial rechazada se reintenta en vez de fallar con CONNECTION_REJECTED",
    archivo: "Security.java", de: "if (thrown instanceof AuthenticationException) {", a: "if (false && thrown instanceof AuthenticationException) {", pruebas: ["TlsIntegrationTest#usuarioYContrasena"] },
  { id: "host-sin-verificar", que: "no exigir que el certificado cubra el host (la libreria sola no lo hace)",
    archivo: "Security.java", de: '                p.setEndpointIdentificationAlgorithm("HTTPS");\n                ssl.setSSLParameters(p);', a: "                ssl.setSSLParameters(p);", pruebas: ["TlsIntegrationTest#hostFueraDeLosSan"] },
  { id: "mtls-sin-diagnostico", que: "no distinguir un broker que exige certificado de cliente de uno mudo: espera para siempre",
    archivo: "FluxBus.java", de: "if (permanent == null && tls != null && attempt.exceptions().stream()", a: "if (false && permanent == null && tls != null && attempt.exceptions().stream()", pruebas: ["TlsIntegrationTest#tlsMutuoSinCertificadoDeCliente"] },
  { id: "credenciales-vacias", que: "una credencial vacía (variable de entorno sin definir) degrada a «sin credenciales»",
    archivo: "Security.java", de: "if (value == null || value.trim().isEmpty()) {\n            throw new InvalidCredentialsException", a: "if (value == null) {\n            throw new InvalidCredentialsException", pruebas: ["SecurityUnitTest#valoresVacios"] },
  { id: "inbox-comun", que: "no fijar el inbox privado `_INBOX.<servicio>`: el broker deniega la suscripcion a su propio inbox",
    archivo: "FluxBus.java", de: "                .inboxPrefix(inboxPrefix)\n", a: "", pruebas: ["SecurityIntegrationTest#cicloDeExtremoAExtremo"] },
  { id: "crear-antes-de-preguntar", que: "crear el consumidor sin preguntar antes (CONSUMER.CREATE no se concede en produccion)",
    archivo: "FluxBus.java", de: "ConsumerInfo found = ensureConsumer(stream, durable, subject, requested);", a: "ConsumerInfo found = jsm.addOrUpdateConsumer(stream, requested);", pruebas: ["SecurityIntegrationTest#cicloDeExtremoAExtremo"] },
  { id: "peticiones-sin-vigilar", que: "las peticiones a la API de JetStream no compiten con la denegacion: timeout de la libreria en vez de PERMISSION_DENIED",
    archivo: "FluxBus.java", de: "return listener.denials.guard(options.service, subjects, hint, call::call);", a: "return call.call();", pruebas: ["SecurityIntegrationTest#publicarEnUnDominioAjeno"] },
  { id: "publish-sin-vigilar", que: "un publish denegado espera el timeout en vez de fallar con PERMISSION_DENIED",
    archivo: "FluxBus.java", de: "listener.denials.race(options.service, subject, null, () -> jetStream.publishAsync(subject, body,\n                    io.nats.client.PublishOptions.builder().messageId(messageId).build()), PUBLISH_ACK_TIMEOUT_MILLIS);",
    a: "jetStream.publishAsync(subject, body, io.nats.client.PublishOptions.builder().messageId(messageId).build()).get(PUBLISH_ACK_TIMEOUT_MILLIS, java.util.concurrent.TimeUnit.MILLISECONDS);", pruebas: ["SecurityIntegrationTest#publishDenegado"] },
  { id: "aviso-despues-de-la-operacion", que: "registrar el aviso de denegacion DESPUES de lanzar la operacion: un -ERR que llega entre medias se pierde y quien llama espera su timeout",
    archivo: "Denials.java", de: "        CompletableFuture<Denial> denial = watch(subject);\n        CompletableFuture<Denial> inbox = null;", a: "        CompletableFuture<T> adelantada = start.get();\n        start = () -> adelantada;\n        CompletableFuture<Denial> denial = watch(subject);\n        CompletableFuture<Denial> inbox = null;", pruebas: ["SecurityUnitTest#laDenegacionQueLlegaAntesDeEsperarNoSePierde"] },
  { id: "sonda-de-pull", que: "no comprobar el permiso de pull al suscribirse: el consumidor queda inerte y parece sano (A-20)",
    archivo: "FluxBus.java", de: "            ensurePullAllowed(stream, durable);\n", a: "", pruebas: ["SecurityIntegrationTest#pullDenegadoHaceFallarSubscribe"] },
  { id: "not-provisioned-como-denegacion", que: "«falta y no puedo crearlo» se da como PERMISSION_DENIED",
    archivo: "FluxBus.java", de: "throw new NotProvisionedException(ResourceKind.STREAM, name, null, options.service, denied.subject(), null,\n                    denied);", a: "throw denied;", pruebas: ["SecurityIntegrationTest#streamSinAprovisionar"] },
  { id: "aviso-en-cada-ack", que: "avisar del ack denegado en cada mensaje en vez de una vez por subject",
    archivo: "Denials.java", de: "return seen.putIfAbsent(normalize(d.subject()), d.operation()) == null ? d : null;", a: "return d;", pruebas: ["SecurityIntegrationTest#ackDenegadoSoloTieneElLog"] },

  // ── estado ──
  { id: "estado-sin-cabecera-de-secuencia", que: "publicar sin `Nats-Expected-Last-Subject-Sequence`: dos instancias del dueño se pisan y una versión vieja sobrescribe a una nueva",
    archivo: "FluxState.java", de: ".messageId(id).expectedLastSubjectSequence(expected).build())", a: ".messageId(id).build())", pruebas: ["StateIntegrationTest#conflictoDeterminista", "StateIntegrationTest#escritoresConcurrentes"] },
  { id: "estado-version-que-no-avanza", que: "escribir una versión que no avanza (el `stale` no se decide)",
    archivo: "FluxState.java", de: "if (actual != null && actual.version != null && version <= actual.version) {", a: "if (false && actual != null && actual.version != null && version <= actual.version) {", pruebas: ["StateIntegrationTest#stale"] },
  { id: "estado-sin-reconocer-el-propio-mensaje", que: "no reconocer el propio mensaje (paso 1b): un acuse perdido se convierte en `stale` con su propio dato",
    archivo: "FluxState.java", de: "if (actual != null && actual.ids.contains(id)) {", a: "if (false && actual != null && actual.ids.contains(id)) {", pruebas: ["StateIntegrationTest#elPropioMensajeSeReconoce"] },
  { id: "estado-conflicto-solo-10071", que: "reconocer solo el código 10071 del conflicto: bajo carrera en un cluster el broker da el 10164 y el SDK no reintenta",
    archivo: "FluxState.java", de: "static final List<Integer> CONFLICT_CODES = List.of(10071, 10164);", a: "static final List<Integer> CONFLICT_CODES = List.of(10071);", pruebas: ["StateUnitTest#losDosCodigosDelConflicto"] },
  { id: "estado-el-watch-pide-stream-info", que: "abrir el watch pidiendo `STREAM.INFO` del stream de estado (lista todas las claves y tenants: ningún observador lo tiene concedido, A-39)",
    archivo: "FluxState.java", de: "w.sub = bus.jetStream.subscribe(null, PullSubscribeOptions.fastBind(stream, durable));", a: "bus.jsm.getStreamInfo(stream);\n            w.sub = bus.jetStream.subscribe(null, PullSubscribeOptions.fastBind(stream, durable));", pruebas: ["StateIntegrationTest#cicloDe100Claves"] },
  { id: "estado-onsynced-no-inmediato", que: "esperar a una entrada para dar `onSynced` aunque el contrato esté vacío: el servicio queda «sincronizando» para siempre",
    archivo: "FluxState.java", de: "if (found.getNumPending() == 0 && found.getNumAckPending() == 0) {", a: "if (false && found.getNumPending() == 0 && found.getNumAckPending() == 0) {", pruebas: ["StateIntegrationTest#onSyncedConContratoVacio"] },
  { id: "estado-no-reabre-un-durable-borrado", que: "no reabrir el consumo cuando el durable se borra (`flux state resync`): el watch queda inerte con la conexión viva",
    archivo: "FluxState.java", de: "                    reopen(status);", a: "                    sleep(1000);", pruebas: ["StateIntegrationTest#resync"] },
  { id: "estado-filtro-sin-comprobar", que: "no comprobar el filtro del durable aprovisionado: entregaría las claves de otro tenant sin ningún error",
    archivo: "FluxState.java", de: "if (!java.util.Objects.equals(requested.getFilterSubject(), eff) || (filters != null && filters.size() > 1)) {", a: "if (false) {", pruebas: ["StateIntegrationTest#durableConOtroFiltro"] },
  { id: "estado-identidad-del-sobre", que: "fiarse del sobre y no del subject: un mensaje de otro tenant llega al handler",
    archivo: "FluxState.java", de: "if (!tenant.equals(event.tenantid())) {", a: "if (false) {", pruebas: ["StateIntegrationTest#sobreQueMiente"] },
  { id: "estado-clasificacion-de-la-conexion", que: "usar `internal` y no `confidential` como clasificación por defecto de un estado (retención de 30 días en vez de 7 para un dato personal)",
    archivo: "FluxState.java", de: "StateEnvelope.assertClassification(po.classification == null ? null : po.classification.wire());", a: "StateEnvelope.assertClassification(po.classification == null ? \"internal\" : po.classification.wire());", pruebas: ["StateIntegrationTest#elSobreEnElBroker"] },
];

// ── ejecución ───────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
if (args.includes("--list")) {
  for (const m of MUTANTES) console.log(`${m.id.padEnd(34)} ${m.que}`);
  process.exit(0);
}
const solo = args.includes("--only") ? args[args.indexOf("--only") + 1].split(",") : null;
const elegidos = MUTANTES.filter((m) => !solo || solo.some((s) => m.id.includes(s)));
if (elegidos.length === 0) {
  console.error("ningún mutante coincide con --only");
  process.exit(2);
}

function mvn(dir, pruebas) {
  const a = ["-B", "-q", ...(process.env.MVN_OFFLINE ? ["-o"] : []), "test", `-Dtest=${pruebas.join(",")}`,
    "-Djunit.jupiter.execution.timeout.default=25s", "-DfailIfNoTests=true"];
  // `timeout` mata el GRUPO de procesos (mvn y el JVM de surefire): una librería que se cuelga no se deja interrumpir (jnats ignora
  // la interrupción del hilo de la prueba), y sin esto el script esperaría para siempre.
  const win = process.platform === "win32";
  const r = win
    ? spawnSync("mvn", a, { cwd: dir, encoding: "utf8", shell: true })
    : spawnSync("timeout", ["-k", "5", String(LIMITE_S), "mvn", ...a], { cwd: dir, encoding: "utf8" });
  r.colgado = !win && r.status === 124;
  return r;
}

const LIMITE_S = 150;

// Foto del código al ARRANCAR: cada mutante parte de ella y no de `src/` en vivo, así que se puede seguir editando mientras corre.
const BASE = join(COPIAS, "_base");
rmSync(COPIAS, { recursive: true, force: true });
mkdirSync(BASE, { recursive: true });
cpSync(join(SDK, "pom.xml"), join(BASE, "pom.xml"));
cpSync(join(SDK, "src"), join(BASE, "src"), { recursive: true });

function copia(id) {
  const dir = join(COPIAS, id);
  rmSync(dir, { recursive: true, force: true });
  mkdirSync(dir, { recursive: true });
  cpSync(join(BASE, "pom.xml"), join(dir, "pom.xml"));
  cpSync(join(BASE, "src"), join(dir, "src"), { recursive: true });
  return dir;
}

// Las pruebas se ejecutan SIN mutar primero: si no pasan, un «detectado» no demostraría nada.
const todas = [...new Set(elegidos.flatMap((m) => m.pruebas))];
console.log(`· control: ${todas.length} prueba(s) SIN mutar deben pasar`);
const dc = copia("_control");
const rc = mvn(dc, todas);
if (rc.status !== 0) {
  console.error(`✗ las pruebas fallan sin mutar; no demuestran nada:\n${(rc.stdout + rc.stderr).split("\n").filter((l) => !/Picked up|SLF4J/.test(l)).slice(0, 40).join("\n")}`);
  process.exit(2);
}
console.log("  ✓ pasan sin mutar\n");

let fallos = 0;
for (const m of elegidos) {
  const dir = copia(m.id);
  const ruta = join(dir, F, m.archivo);
  const texto = readFileSync(ruta, "utf8");
  const veces = texto.split(m.de).length - 1;
  if (veces !== 1) {
    console.error(`✗ ${m.id}: «${m.de.slice(0, 60)}…» aparece ${veces} veces en ${m.archivo} (debe ser 1): el mutante no se puede aplicar`);
    fallos++;
    continue;
  }
  writeFileSync(ruta, texto.replace(m.de, () => m.a));
  const r = mvn(dir, m.pruebas);
  const salida = r.stdout + r.stderr;
  if (/COMPILATION ERROR/.test(salida)) {
    console.error(`✗ ${m.id}: el mutante no compila (no demuestra nada)`);
    fallos++;
  } else if (r.status !== 0 && /Tests run:.*(Failures: [1-9]|Errors: [1-9])/.test(salida)) {
    console.log(`✓ ${m.id.padEnd(34)} detectado por ${m.pruebas.join(", ")}`);
  } else if (m.cuelga && r.colgado) {
    // Una librería que se cuelga ante lo que esperar no arregla no falla una aserción: no termina. El síntoma ES el cuelgue
    // (JUnit interrumpe el hilo y jnats lo ignora), y `timeout` lo mata.
    console.log(`✓ ${m.id.padEnd(34)} detectado por ${m.pruebas.join(", ")} (no termina: se mata a los ${LIMITE_S} s)`);
  } else if (r.status === 0) {
    console.error(`✗ ${m.id}: NO DETECTADO — ${m.que}. Las pruebas ${m.pruebas.join(", ")} pasan con el SDK estropeado`);
    fallos++;
  } else {
    console.error(`✗ ${m.id}: falló pero no por las pruebas (¿entorno?):\n${salida.split("\n").filter((l) => !/Picked up|SLF4J/.test(l)).slice(0, 15).join("\n")}`);
    fallos++;
  }
  rmSync(dir, { recursive: true, force: true });
}
rmSync(dc, { recursive: true, force: true });
console.log(fallos ? `\n✗ ${fallos} mutante(s) sin detectar` : `\n✓ ${elegidos.length} mutante(s) detectados`);
process.exit(fallos ? 1 : 0);
