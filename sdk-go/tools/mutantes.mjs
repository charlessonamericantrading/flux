#!/usr/bin/env node
/**
 * Mutantes del SDK de Go: «una prueba que pasa con el código roto no vale».
 *
 *   node sdk-go/tools/mutantes.mjs                 # todos
 *   node sdk-go/tools/mutantes.mjs inbox-comun     # uno
 *   node sdk-go/tools/mutantes.mjs --lista
 *
 * Cada mutante ROMPE A PROPÓSITO una garantía del código de producción (copiando el fichero, cambiando un fragmento, y
 * restaurándolo siempre al acabar), y el conjunto de pruebas que dice vigilarla DEBE fallar. Si pasa, la prueba no vigila
 * nada. Necesita lo mismo que las pruebas de integración (NATS_SERVER, node y `(cd cli && npm install)`), con
 * FLUX_REQUIRE_BROKER=1: un salto no es un pase.
 *
 * Sale con 0 si TODOS los mutantes fueron detectados, y con 1 si alguno sobrevivió.
 */

import { spawnSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const DIR = join(dirname(fileURLToPath(import.meta.url)), "..");

/** `from` debe aparecer EXACTAMENTE una vez: un mutante que no encuentra su sitio no rompe nada y pasaría por «detectado». */
const MUTANTES = [
  // ── seguridad: inbox, primera conexión ───────────────────────────────────────────────────────────────────────────
  { id: "inbox-comun", file: "client.go", from: "nats.CustomInboxPrefix(inboxPrefix),", to: "nats.CustomInboxPrefix(inboxPrefix[:0] + \"_INBOX\"),", run: "TestSeguridadCiclo|TestG_|TestH_Cred",
    why: "sin el prefijo privado la librería usa `_INBOX.<aleatorio>`, que la ACL generada deniega" },
  { id: "retry-on-failed-connect", file: "client.go", from: "nats.RetryOnFailedConnect(false),", to: "nats.RetryOnFailedConnect(true),", run: "TestH_",
    why: "con la opción de la librería Connect() devuelve una conexión «viva» sin error ante un TLS o unas credenciales que no encajan" },
  { id: "auth-rechazada-no-se-reconoce", file: "security.go", from: "case errors.Is(err, nats.ErrAuthorization), errors.Is(err, nats.ErrAuthExpired),", to: "case errors.Is(err, nats.ErrAuthExpired),", run: "TestH_Cred|TestH_Usuario",
    why: "una credencial rechazada se tomaría por un broker que aún no está: Connect() esperaría para siempre" },
  { id: "tls-sin-servidor-tls-no-se-reconoce", file: "security.go", from: "case errors.Is(err, nats.ErrSecureConnWanted):", to: "case false:", run: "TestH_TLSExigido",
    why: "TLS pedido y no ofrecido se reintentaría para siempre" },
  { id: "certificado-no-verifica-no-se-reconoce", file: "security.go", from: "if errors.As(err, &verificacion) || errors.As(err, &desconocida) || errors.As(err, &hostname) || errors.As(err, &invalido) {", to: "if false && (errors.As(err, &verificacion) || errors.As(err, &desconocida) || errors.As(err, &hostname) || errors.As(err, &invalido)) {", run: "TestH_Certificado|TestH_Host",
    why: "un certificado que no verifica se reintentaría para siempre: lo peor que le puede pasar a quien acaba de configurar TLS" },
  { id: "mtls-sin-certificado-no-se-reconoce", file: "security.go", from: "if alerta := alertaTLSDelServidor(err); alerta != \"\" {", to: "if alerta := \"\"; alerta != \"\" {", run: "TestH_BrokerQueExige",
    why: "un broker que exige certificado de cliente se tomaría por uno que aún no arrancó" },
  { id: "auth-con-codigo-de-tls", file: "security.go", from: "return rechazo(RejectionAuth, fmt.Sprintf(", to: "return rechazo(RejectionTLS, fmt.Sprintf(", run: "TestH_Cred|TestH_Usuario",
    why: "el motivo equivocado mandaría a quien opera a buscar donde no es" },
  { id: "opcion-vacia-se-ignora", file: "security.go", from: "if len(vacios) > 0 {", to: "if false {", run: "TestInvalidCredentials",
    why: "una variable de entorno sin definir conectaría sin credenciales sin que nada lo diga" },
  { id: "varios-metodos-se-admiten", file: "security.go", from: "if len(metodos) > 1 {", to: "if false {", run: "TestInvalidCredentials",
    why: "dos métodos son ambiguos: el servidor aceptaría uno y el otro sería ruido" },
  { id: "ca-propia-se-ignora", file: "security.go", from: "cfg.RootCAs = pool", to: "_ = pool", run: "TestSeguridadCiclo|TestG_|TestH_Cred",
    why: "sin la CA propia la verificación contra las CA del sistema falla: no se puede usar un bus interno" },
  { id: "tls-mal-formado-pasa", file: "security.go", from: "if !pool.AppendCertsFromPEM(pem) {", to: "if !pool.AppendCertsFromPEM(pem) && false {", run: "TestInvalidTLSOptions",
    why: "una CA que no es un PEM se aceptaría y fallaría en el handshake con otro código" },
  // ── seguridad: denegaciones ──────────────────────────────────────────────────────────────────────────────────────
  { id: "publish-sin-guard", file: "client.go", from: "gctx, done := b.guard(ctx, subject)\n\t_, err = b.js.Publish(gctx, subject, payload, jetstream.WithMsgID(event.ID))\n\tdone()", to: "gctx, done := ctx, func() {}\n\t_, err = b.js.Publish(gctx, subject, payload, jetstream.WithMsgID(event.ID))\n\tdone()", run: "TestG_UnPublishDenegado",
    why: "un publish denegado esperaría el timeout de la API sin decir nada" },
  { id: "stream-info-sin-guard", file: "client.go", from: "gctx, done := b.guard(ctx, \"$JS.API.STREAM.INFO.\"+name)", to: "gctx, done := ctx, func() {}", run: "TestG_Publicar|TestG_SinPermisoParaConsultarElStream",
    why: "una consulta denegada esperaría el timeout de la API" },
  { id: "pull-denegado-no-falla", file: "client.go", from: "if err := b.ensurePullAllowed(ctx, stream, durable); err != nil {\n\t\treturn nil, err\n\t}", to: "_ = b.ensurePullAllowed", run: "TestG_SubscribeFalla",
    why: "un pull denegado deja un consumidor inerte que parece sano: el peor modo de fallo del modelo" },
  { id: "crear-antes-de-preguntar", file: "client.go", from: "c, err := b.js.Consumer(gctx, stream, durable)\n\tdone()", to: "c, err := b.js.Consumer(gctx, stream, durable+\"-x\")\n\tdone()", run: "TestSeguridadCiclo|TestG_UnConsumidor",
    why: "no preguntar primero deja una `Publish Violation` por arranque (y obliga a conceder CONSUMER.CREATE)" },
  { id: "consumidor-ausente-sin-notprovisioned", file: "client.go", from: "return nil, cfg, newNotProvisioned(ResourceConsumer, durable, stream, b.opts.Service, pd.Subject, \"\", pd)", to: "return nil, cfg, pd", run: "TestG_UnConsumidorQueFalta",
    why: "sin NOT_PROVISIONED quien opera ve un permiso denegado y no sabe que falta aprovisionar" },
  { id: "info-denegado-es-notprovisioned", file: "client.go", from: "if pd := deniedCause(gctx); pd != nil {\n\t\t\treturn nil, cfg, pd\n\t\t}\n\t\treturn nil, cfg, fmt.Errorf(\"flux: no se pudo consultar el consumidor", to: "if pd := deniedCause(gctx); pd != nil {\n\t\t\treturn nil, cfg, newNotProvisioned(ResourceConsumer, durable, stream, b.opts.Service, pd.Subject, \"\", pd)\n\t\t}\n\t\treturn nil, cfg, fmt.Errorf(\"flux: no se pudo consultar el consumidor", run: "TestG_SinPermisoParaConsultarElConsumidor",
    why: "si ni siquiera se puede consultar no se sabe si falta: es PERMISSION_DENIED, nunca NOT_PROVISIONED" },
  { id: "ack-denegado-se-avisa-siempre", file: "denials.go", from: "if ya || h.logger == nil {", to: "if (ya && false) || h.logger == nil {", run: "TestG_UnAck",
    why: "un log por mensaje entierra la causa bajo su propio volumen" },
  { id: "ack-denegado-sin-normalizar", file: "denials.go", from: "subject := normalizeDeniedSubject(d.subject)", to: "subject := d.subject", run: "TestG_UnAck",
    why: "cada ack lleva su secuencia: sin normalizar, el aviso «una vez por subject» no avisaría de nada" },
  { id: "violacion-no-se-reconoce", file: "denials.go", from: "m := permissionViolationRE.FindStringSubmatch(err.Error())", to: "m := permissionViolationRE.FindStringSubmatch(\"\")", run: "TestG_|TestDenegaciones",
    why: "sin entender el -ERR del broker ninguna denegación llega a nadie" },
  { id: "denegacion-del-otro-subject", file: "denials.go", from: "if d.subject == s {", to: "if d.subject == s || true {", run: "TestElGuard",
    why: "una denegación de OTRA operación no es culpa de esta" },
  // ── estado ───────────────────────────────────────────────────────────────────────────────────────────────────────
  { id: "estado-sin-cabecera-de-secuencia", file: "state.go", from: "jetstream.WithMsgID(id), jetstream.WithExpectLastSequencePerSubject(seq))", to: "jetstream.WithMsgID(id)); _ = seq", run: "TestEstadoEscritoresConcurrentes|TestEstadoUnConflicto",
    why: "sin `Nats-Expected-Last-Subject-Sequence` dos instancias del dueño se pisan: gana la última en llegar, no la versión más alta (A-33)" },
  { id: "estado-solo-el-10071", file: "state_pure.go", from: "var stateConflictCodes = [...]int{10071, 10164}", to: "var stateConflictCodes = [...]int{10071}", run: "TestElConflictoDeSecuenciaTieneDosCodigos",
    why: "en un cluster el conflicto llega como 10164 SIN número: un SDK que solo reconoce el 10071 pasa sus pruebas en un nodo y rompe en producción (A-49)" },
  { id: "estado-sin-paso-1b", file: "state.go", from: "if actual != nil && actual.hasID(id) {", to: "if false && actual != nil && actual.hasID(id) {", run: "TestEstadoUnAcusePerdidoNoSeConvierteEnStale",
    why: "sin reconocer el propio mensaje, un acuse perdido convierte una escritura buena en un `stale` con su propio dato (A-33)" },
  { id: "estado-sin-stale", file: "state.go", from: "if actual != nil && actual.version > 0 && version <= actual.version {", to: "if false && actual != nil && actual.version > 0 && version <= actual.version {", run: "TestEstadoStaleIdempotencia",
    why: "una versión que no avanza se escribiría y un valor viejo pisaría a uno nuevo" },
  { id: "estado-no-reintenta-el-conflicto", file: "state.go", from: "if isSequenceConflict(err) {", to: "if false && isSequenceConflict(err) {", run: "TestEstadoUnConflicto|TestEstadoEscritoresConcurrentes",
    why: "un conflicto de secuencia saldría como error genérico en vez de releer y reintentar" },
  { id: "estado-tenant-system-por-defecto", file: "state.go", from: "return \"\", missingTenant()", to: "t = \"system\"", run: "TestElTenantDeEstadoNoTieneDefaultNiCaeEnSystem|TestPutRechazaSinRed",
    why: "un estado sin tenant guardado en `system` es un dato en otro sitio, sin error (10-state.md §2.4)" },
  { id: "estado-sin-chequeo-de-identidad", file: "state.go", from: "if ev.TenantID != c.tenant {", to: "if false && ev.TenantID != c.tenant {", run: "TestEstadoUnMensajeQueMiente",
    why: "una entrada cuyo sobre miente sobre su tenant se aplicaría como si fuera de quien dice el subject" },
  { id: "estado-restricted-se-admite-al-consumir", file: "state.go", from: "if ev.DataClassification == ClassificationRestricted {", to: "if false && ev.DataClassification == ClassificationRestricted {", run: "TestEstadoUnaEntradaRestricted",
    why: "una entrada `restricted` (24 h de retención) viviría en un almacén que no caduca" },
  { id: "estado-restricted-se-admite-al-publicar", file: "state_pure.go", from: "case ClassificationPublic, ClassificationInternal, ClassificationConfidential:", to: "case ClassificationPublic, ClassificationInternal, ClassificationConfidential, ClassificationRestricted:", run: "TestPutRechazaSinRed|TestElOrdenDeValidacionDeEstado|TestEstadoQuitarUnPermiso",
    why: "`restricted` en un estado no es una decisión que deba tomarse por omisión (10-state.md §7)" },
  { id: "estado-dlq-conserva-el-dato", file: "state_pure.go", from: "event.Data = []byte(`{\"key\":` + jsonString(info.Key)", to: "event.Data = append([]byte(nil), event.Data...); _ = []byte(`{\"key\":` + jsonString(info.Key)", run: "TestLaDLQDeEstadoNoConservaElDatoNiLaFirma|TestEstadoElErrorDelHandlerVaALaDLQ",
    why: "la DLQ guarda 90 días: el dato personal que el dueño ya borró seguiría vivo y el derecho de supresión no se cumpliría" },
  { id: "estado-dlq-con-el-texto-del-error-ajeno", file: "state.go", from: "dlqError := c.Code + \": \" + UnclassifiedDLQError", to: "dlqError := c.Code + \": \" + err.Error()", run: "TestEstadoElErrorDelHandlerVaALaDLQ",
    why: "el error de un driver de base de datos puede citar la fila entera, y en estado es dato personal" },
  { id: "estado-watch-entrega-todo-el-historial", file: "state.go", from: "DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy,", to: "DeliverPolicy: jetstream.DeliverAllPolicy,", run: "TestEstadoUnObservadorNuevoRecibeSoloElUltimoValor",
    why: "un observador nuevo no debe reproducir el historial de la clave: solo su valor vigente" },
  { id: "estado-watch-no-reabre", file: "state.go", from: "func (s *State) reopen(ctx context.Context, w *watchState) jetstream.Consumer {", to: "func (s *State) reopen(ctx context.Context, w *watchState) jetstream.Consumer {\n\tif true {\n\t\treturn nil\n\t}", run: "TestEstadoElWatchReabreElConsumoSiSeBorraElDurable",
    why: "un durable borrado deja al observador sordo sin error: la réplica se queda sin actualizarse y parece sana (A-197)" },
  { id: "estado-put-sin-guard", file: "state.go", from: "gctx, done := s.bus.guard(ctx, subject)\n\t\tack, err := s.bus.js.PublishMsg(", to: "gctx, done := ctx, func() {}\n\t\tack, err := s.bus.js.PublishMsg(", run: "TestEstadoQuitarUnPermisoSeNota",
    why: "un put denegado esperaría el plazo de la API sin decir que falta un permiso" },
  { id: "estado-l3-cita-valores", file: "state.go", from: "return v.checkRedacted(ev, subject)", to: "return v.check(ev, subject)", run: "TestEstadoL3|TestElMensajeDeL3",
    why: "un `'ana@example.com' no cumple el formato` acabaría en los logs y, al consumir, 90 días en `dlqerror`" },
];

const argv = process.argv.slice(2);
if (argv.includes("--lista")) {
  for (const m of MUTANTES) console.log(`${m.id}\n    ${m.why}`);
  process.exit(0);
}
const solo = argv.filter((a) => !a.startsWith("--"));
const elegidos = solo.length ? MUTANTES.filter((m) => solo.includes(m.id)) : MUTANTES;
if (solo.length && elegidos.length !== solo.length) {
  console.error(`mutante desconocido: ${solo.filter((s) => !MUTANTES.some((m) => m.id === s)).join(", ")}`);
  process.exit(2);
}

const go = (args) =>
  spawnSync("go", args, { cwd: DIR, encoding: "utf8", env: { ...process.env, FLUX_REQUIRE_BROKER: process.env.FLUX_REQUIRE_BROKER ?? "1" }, timeout: 600_000 });

const base = go(["build", "./..."]);
if (base.status !== 0) {
  console.error(`el código sin mutar no compila:\n${base.stderr}`);
  process.exit(2);
}

let vivos = 0;
for (const m of elegidos) {
  const ruta = join(DIR, m.file);
  const original = readFileSync(ruta, "utf8");
  const n = original.split(m.from).length - 1;
  if (n !== 1) {
    console.error(`✗ ${m.id}: el fragmento aparece ${n} veces en ${m.file} (debe ser 1): el mutante no rompe nada`);
    vivos++;
    continue;
  }
  try {
    writeFileSync(ruta, original.replace(m.from, () => m.to));
    const r = go(["test", "-count=1", "-timeout", "150s", "-run", m.run, "./..."]);
    const compila = !/\[build failed\]|cannot use|undefined:|declared and not used/.test(r.stdout + r.stderr);
    if (r.status === 0) {
      console.log(`✗ ${m.id}: SOBREVIVIÓ — ${m.run} pasa con el código roto (${m.why})`);
      vivos++;
    } else if (!compila) {
      console.log(`? ${m.id}: el mutante no compila (no cuenta como detectado): ${(r.stdout + r.stderr).split("\n").slice(0, 4).join(" | ")}`);
      vivos++;
    } else {
      const fallos = [...(r.stdout.match(/^--- FAIL: \S+/gm) ?? [])].map((l) => l.replace("--- FAIL: ", ""));
      console.log(`✓ ${m.id}: detectado por ${fallos.length ? fallos.slice(0, 3).join(", ") + (fallos.length > 3 ? ` y ${fallos.length - 3} más` : "") : "un fallo de las pruebas"}`);
    }
  } finally {
    writeFileSync(ruta, original);
  }
}
console.log(vivos === 0 ? `\n✓ ${elegidos.length} mutantes, todos detectados` : `\n✗ ${vivos} de ${elegidos.length} mutantes NO detectados`);
process.exit(vivos === 0 ? 0 : 1);
