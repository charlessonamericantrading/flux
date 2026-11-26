/**
 * La comprobación de `flux doctor` para la petición/respuesta: **ningún stream captura una
 * operación** (specification/11-request-reply.md §8, `protocol.json` → `rpc.doctor`).
 *
 * Por qué es un error y no un aviso. Un stream que captura el subject de una operación
 *   · contesta a cada petición con SU acuse (`{"stream":…,"seq":n}`), que quien llama toma por la
 *     respuesta (casi siempre antes que la real: 394 de 400, A-67) mientras el responder ejecuta la
 *     orden igualmente, y
 *   · GUARDA cada petición —datos personales incluidos— con la retención del stream.
 * Un stream sin acuse (`no_ack`) no contesta, pero cuenta como interés: `no responders`
 * (RPC_UNAVAILABLE, 1 ms) pasa a ser un timeout (RPC_TIMEOUT, 5 s), y también persiste. Y los
 * filtros más anchos (`>`, `*.>`, `*.*.*.*.*`) solo se pueden crear SIN acuse (A-69): los crea
 * alguien que «quiere verlo todo» y nadie se entera de que guardan cada petición y cada respuesta.
 *
 * La comprobación mira los subjects de cada stream de la account y los compara token a token con
 * la FORMA de toda operación —`rpc.*.*.*.*`, cinco tokens—, no con `rpc.>`: un filtro de seis
 * tokens se solapa con `rpc.>` y no puede capturar ninguna. Es función pura: se prueba sin broker.
 */

import { canonRpc, solapan } from "./rpc-model.mjs";

/**
 * `streams`: lo que devuelve `jsm.streams.list()` (cada uno con `config.name`, `config.subjects` y,
 * si se pidió así, `config.no_ack`). Devuelve:
 *   { ok, revisados, forma, capturas: [{ stream, noAck, subjects, ancho, arreglo }], findings }
 * donde cada `finding` es `{ level: "error", target, message, hint, check, … }` listo para el doctor.
 */
export function auditarStreamsRpc(streams) {
  const forma = canonRpc().forma;
  const capturas = [];
  for (const s of streams) {
    const cfg = s.config ?? s;
    const subjects = cfg.subjects ?? [];
    const malos = subjects.filter((x) => solapan(x, forma));
    if (!malos.length) continue;
    const noAck = cfg.no_ack === true;
    const resto = subjects.filter((x) => !malos.includes(x));
    capturas.push({
      stream: cfg.name,
      noAck,
      subjects: malos,
      // «Ancho»: captura más que operaciones (no empieza por el token literal `rpc`).
      ancho: malos.filter((x) => x.split(".")[0] !== "rpc"),
      restantes: resto,
    });
  }
  const findings = capturas.map((c) => {
    c.arreglo = arreglo(c);
    return {
      level: "error",
      check: "rpc-stream-captura-operaciones",
      target: c.stream,
      message: mensaje(c),
      hint: `${explicacion(c)}\n${c.arreglo}`,
      subjects: c.subjects,
      noAck: c.noAck,
      restantes: c.restantes,
    };
  });
  return { ok: capturas.length === 0, revisados: streams.length, forma, capturas, findings };
}

const lista = (xs) => xs.map((x) => `\`${x}\``).join(", ");

function mensaje(c) {
  const que = c.noAck ? "stream sin acuse, no_ack" : "stream con acuse";
  return `captura operaciones de petición/respuesta: ${lista(c.subjects)} se solapa con rpc.*.*.*.* (${que})`;
}

/** Qué hace este stream a las peticiones, en el idioma de lo que se ve desde fuera. */
function explicacion(c) {
  const partes = [];
  if (c.noAck) {
    partes.push(
      "No contesta con acuse, pero CUENTA COMO INTERÉS: una petición a una operación sin responder deja de dar `no responders` " +
        "(RPC_UNAVAILABLE en ~1 ms) y espera su timeout (RPC_TIMEOUT, 5 s) — A-67. Y guarda cada petición, con sus datos personales, con la retención del stream.",
    );
  } else {
    partes.push(
      "Contesta a cada petición con SU acuse ({\"stream\":…,\"seq\":n}): quien llama lo recibe como si fuera la respuesta (casi siempre antes que la real, " +
        "pero no siempre: el síntoma es intermitente), el responder ejecuta la orden igualmente y el stream GUARDA cada petición, con sus datos personales — A-18, A-67.",
    );
  }
  if (c.ancho.length) {
    partes.push(
      `${lista(c.ancho)} es un filtro ancho: captura además lo que no es de petición/respuesta` +
        (c.noAck ? " (JetStream solo deja crearlo sin acuse, A-69), y con él cada respuesta viaja también al stream." : "."),
    );
  }
  if (/^EVT_RPC$/i.test(c.stream)) {
    partes.push("Es el stream de eventos de un «dominio» llamado `rpc`, que está reservado (RESERVED_DOMAIN): alguien lo aprovisionó a mano.");
  }
  return partes.join(" ");
}

/** Cómo se arregla, con el comando exacto: lo que queda del stream, o borrarlo si no queda nada. */
function arreglo(c) {
  if (c.restantes.length === 0) {
    return (
      `Cómo se arregla: el stream no captura nada más, así que no tiene razón de ser como está — bórralo (\`nats stream rm ${c.stream}\`) ` +
      "o recréalo con los subjects que de verdad necesita (los eventos van en `<dominio>.>`; `rpc` es un dominio reservado). " +
      "11-request-reply.md §8."
    );
  }
  return (
    `Cómo se arregla: quita ${lista(c.subjects)} de sus subjects — \`nats stream edit ${c.stream} --subjects "${c.restantes.join(",")}"\`. ` +
    "11-request-reply.md §8."
  );
}
