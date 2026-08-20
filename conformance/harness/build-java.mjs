#!/usr/bin/env node
/**
 * Construye el arnés de conformidad del SDK de Java.
 *
 *   node conformance/harness/build-java.mjs
 *
 * Deja `sdk-java/target/harness/` con TODO en `.jar`:
 *
 *   flux-conformance.jar   el SDK compilado + com.flux.ConformanceHarness
 *   jackson-*.jar          las dependencias de compilación del SDK
 *
 * Así el registro puede invocarlo con un classpath de un solo elemento y sin separador
 * de rutas: `java -cp "target/harness/*" com.flux.ConformanceHarness`. El comodín lo
 * expande el propio lanzador de Java, así que el mismo comando vale en Windows y en
 * Linux — que es la razón de empaquetar las clases en un jar en vez de apuntar a un
 * directorio y un `*` separados por `;` o por `:` según el sistema.
 *
 * ⚠️ No se versiona nada de lo que produce (`target/` está en .gitignore) y eso es
 * deliberado: un jar versionado se quedaría atrás respecto al SDK y el runner compararía
 * bytes de un código que ya no existe — la forma exacta de cobertura aparente que el
 * arnés existe para eliminar. Se reconstruye antes de cada ejecución, igual que
 * `npm run build` en sdk-node.
 *
 * Maven NO es necesario si las dependencias ya están en `~/.m2/repository` (el caso de
 * una máquina de desarrollo que ya compiló el SDK). Si faltan, se llama a Maven UNA vez
 * para descargarlas y copiarlas; es lo que ocurre en CI.
 */

import { spawnSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const AQUI = dirname(fileURLToPath(import.meta.url));
const RAIZ = join(AQUI, "..", "..");
const SDK = join(RAIZ, "sdk-java");
const DESTINO = join(SDK, "target", "harness");
const CLASES = join(DESTINO, "classes");
const JAR_ARNES = join(DESTINO, "flux-conformance.jar");

/** Corre un comando y aborta con su salida si falla. */
function correr(cmd, args, opciones = {}) {
  const r = spawnSync(cmd, args, { cwd: RAIZ, stdio: "inherit", ...opciones });
  if (r.error?.code === "ENOENT") {
    console.error(`✗ no se encontró \`${cmd}\` en el PATH`);
    process.exit(1);
  }
  if (r.status !== 0) {
    console.error(`✗ ${cmd} ${args.join(" ")} → exit ${r.status}`);
    process.exit(1);
  }
  return r;
}

// ─── Dependencias ────────────────────────────────────────────────────────────

// La versión sale del pom para que el arnés no pueda compilar contra una distinta de la
// que usa el SDK.
const pom = readFileSync(join(SDK, "pom.xml"), "utf8");
const jackson = pom.match(/<jackson\.version>([^<]+)<\/jackson\.version>/)?.[1];
if (!jackson) {
  console.error("✗ no se pudo leer <jackson.version> de sdk-java/pom.xml");
  process.exit(1);
}

rmSync(DESTINO, { recursive: true, force: true });
mkdirSync(DESTINO, { recursive: true });

const m2 = join(homedir(), ".m2", "repository", "com", "fasterxml", "jackson", "core");
const artefactos = ["jackson-databind", "jackson-core", "jackson-annotations"].map((a) => ({
  nombre: `${a}-${jackson}.jar`,
  origen: join(m2, a, jackson, `${a}-${jackson}.jar`),
}));

if (artefactos.every((a) => existsSync(a.origen))) {
  for (const a of artefactos) copyFileSync(a.origen, join(DESTINO, a.nombre));
  console.log(`✓ jackson ${jackson} desde ~/.m2`);
} else {
  console.log(`· jackson ${jackson} no está en ~/.m2; pidiéndoselo a Maven`);
  // `outputDirectory` ABSOLUTO: el plugin resuelve las rutas relativas contra el
  // directorio de trabajo, no contra el pom, y acabarían en la raíz del repositorio.
  // `shell` en Windows porque ahí `mvn` es un .cmd y Node se niega a lanzarlo sin shell.
  const mvn = spawnSync(
    "mvn",
    [
      "-B", "-q", "-f", "sdk-java/pom.xml",
      "dependency:copy-dependencies",
      "-DincludeScope=compile",
      `-DoutputDirectory=${DESTINO}`,
    ],
    { cwd: RAIZ, stdio: "inherit", shell: process.platform === "win32" },
  );
  if (mvn.error?.code === "ENOENT" || mvn.status !== 0) {
    console.error(
      "✗ faltan las dependencias de Jackson y Maven no pudo descargarlas.\n" +
        "  Instala Maven, o compila el SDK una vez para poblar ~/.m2/repository.",
    );
    process.exit(1);
  }
}

// ─── Compilación ─────────────────────────────────────────────────────────────

// Todo el SDK MENOS FluxBus: es lo único que importa io.nats, y el arnés no toca el
// broker (conformance/harness/README.md: "Sin red y sin broker"). Dejarlo fuera hace que
// el arnés se construya con Jackson y nada más.
const fuentes = readdirSync(join(SDK, "src", "main", "java", "com", "flux"))
  .filter((f) => f.endsWith(".java") && f !== "FluxBus.java")
  .map((f) => join("src", "main", "java", "com", "flux", f));

console.log(`· javac ${fuentes.length} fuentes`);
correr("javac", [
  "-encoding", "UTF-8",
  "-cp", join("target", "harness", "*"),
  "-d", CLASES,
  ...fuentes,
], { cwd: SDK });

correr("jar", ["--create", "--file", JAR_ARNES, "-C", CLASES, "."]);
rmSync(CLASES, { recursive: true, force: true });

console.log(`✓ ${JAR_ARNES}`);
console.log('  cd sdk-java && java -cp "target/harness/*" com.flux.ConformanceHarness');
