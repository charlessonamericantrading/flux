/**
 * Un proceso del banco. El runner lanza uno por rol (`node bench/worker.mjs <rol>`) y lo gobierna por IPC:
 *
 *   runner → worker   { t: "config", cfg }   prepara (conecta, abre el servidor…) y contesta { t: "listo", info }
 *                     { t: "arranca" }        empieza el tráfico (el productor, los sondeos…)
 *                     { t: "snap", id }       contadores acumulados ahora: { t: "snap", id, datos }
 *                     { t: "fin" }            para el tráfico y devuelve sus registros: { t: "fin", datos }
 *                     { t: "salir" }          cierra y termina
 *
 * Cada rol (`roles/<rol>.mjs`) exporta `crear(cfg)` → `{ arranca(), snap(), fin(), cerrar() }`. Un proceso
 * por rol y NO todo en el runner: la CPU de cada proceso se mide desde fuera con `/proc/<pid>/stat`, y
 * mezclar el cliente con el orquestador falsearía el número del cliente.
 */

const rol = process.argv[2];
if (!rol) {
  console.error("uso: node bench/worker.mjs <rol>");
  process.exit(2);
}
const mod = await import(`./roles/${rol}.mjs`);
let inst;

const enviar = (m) => process.send(m);
process.on("message", async (m) => {
  try {
    switch (m.t) {
      case "config":
        inst = await mod.crear(m.cfg);
        enviar({ t: "listo", info: inst.info?.() ?? {} });
        break;
      case "arranca":
        await inst.arranca();
        enviar({ t: "arrancado" });
        break;
      case "snap":
        enviar({ t: "snap", id: m.id, datos: inst.snap() });
        break;
      case "fin":
        enviar({ t: "fin", datos: await inst.fin() });
        break;
      case "salir":
        await inst?.cerrar?.();
        process.exit(0);
    }
  } catch (e) {
    enviar({ t: "error", mensaje: e?.stack ?? String(e), en: m.t });
  }
});
process.on("uncaughtException", (e) => {
  enviar({ t: "error", mensaje: e?.stack ?? String(e), en: "uncaughtException" });
  setTimeout(() => process.exit(1), 50);
});
process.on("unhandledRejection", (e) => {
  enviar({ t: "error", mensaje: e?.stack ?? String(e), en: "unhandledRejection" });
});
enviar({ t: "hola", pid: process.pid });
