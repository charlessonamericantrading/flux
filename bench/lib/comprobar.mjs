/**
 * Las COMPROBACIONES DEL PROPIO BANCO: antes de fiarse de un número hay que saber que lo que se midió es lo que se
 * creía medir. Cada repetición se comprueba contra lo que sus propios contadores dicen:
 *
 *   · el keep-alive es de verdad (se cuentan las conexiones TCP: sin él, una por petición; con él, las de los clientes);
 *   · el sondeo con ETag recibe 304 y el ingenuo no (si un servidor ignorase `If-None-Match` el «inteligente» sería ingenuo);
 *   · lo que emitieron los clientes es lo que recibió el servidor, sin errores ni peticiones omitidas;
 *   · cada mensaje se entrega UNA vez a cada receptor: ni perdidos, ni duplicados, ni ajenos.
 *
 * Una comprobación que falla NO borra la medición: se publica marcada. Y existe `bench/autoprueba.mjs`, que estropea el banco
 * a propósito para demostrar que estas comprobaciones DETECTAN lo que dicen detectar.
 */

const c = (nombre, ok, detalle) => ({ nombre, ok: !!ok, detalle });

export function comprobarA(r) {
  const out = [];
  out.push(c("sin avisos de fiabilidad (proceso saturado, carga ajena)", r.avisos.length === 0, r.avisos.join("; ")));
  if (r.metodo === "flux") {
    out.push(c("cada observador recibe cada cambio exactamente una vez", r.entregas === r.entregasEsperadas && r.duplicados === 0,
      `entregas ${r.entregas} / esperadas ${r.entregasEsperadas}, duplicados ${r.duplicados}`));
    out.push(c("ningún cambio sin ver", r.sinVer === 0, `${r.sinVer} de ${r.pares} pares`));
    out.push(c("todos los puts se aplicaron", r.puts === r.cambiosEnVentana, `puts ${r.puts}, aplicados ${r.cambiosEnVentana}`));
    return out;
  }
  const h = r.http;
  out.push(c("keep-alive real: ninguna conexión nueva en la ventana", h.conexiones === 0, `${h.conexiones} conexiones nuevas`));
  out.push(c("una conexión viva por consumidor", h.abiertas === r.n, `${h.abiertas} abiertas, ${r.n} consumidores`));
  out.push(c("lo emitido por los clientes es lo recibido por el servidor (±N en vuelo)", Math.abs(h.emitidas - h.peticiones) <= r.n, `emitidas ${h.emitidas}, recibidas ${h.peticiones}`));
  out.push(c("sin errores ni sondeos omitidos", h.errores === 0 && h.omitidas === 0, `errores ${h.errores}, omitidas ${h.omitidas}`));
  out.push(c("200 + 304 = peticiones", h.r200 + h.r304 === h.peticiones && h.otras === 0, `200:${h.r200} 304:${h.r304} otras:${h.otras}`));
  if (r.metodo === "etag") {
    // Cada cambio provoca a lo sumo UN 200 por consumidor: el resto DEBEN ser 304.
    out.push(c("con ETag, los sondeos sin cambio reciben 304", h.r304 > 0 && h.r200 <= (r.cambiosEnVentana + 1) * r.n,
      `304:${h.r304}, 200:${h.r200} (tope ${(r.cambiosEnVentana + 1) * r.n})`));
  } else {
    out.push(c("el sondeo ingenuo nunca recibe 304", h.r304 === 0, `304:${h.r304}`));
  }
  return out;
}

export function comprobarB(r) {
  const out = [];
  out.push(c("sin avisos de fiabilidad (proceso saturado, carga ajena)", r.avisos.length === 0, r.avisos.join("; ")));
  out.push(c("cada receptor recibe cada hecho (ninguno perdido)", r.perdidas === 0, `${r.perdidas} entregas perdidas de ${r.hechosContados * r.n}`));
  out.push(c("ninguna entrega ajena a la corrida", r.ajenos === 0, `${r.ajenos} ajenas`));
  if (r.metodo === "flux") {
    out.push(c("cada evento se entrega una vez a cada consumidor (sin duplicados)", r.duplicados === 0 && r.entregas === r.publicaciones * r.n,
      `entregas ${r.entregas}, esperadas ${r.publicaciones * r.n}, duplicados ${r.duplicados}`));
    out.push(c("el productor no tuvo errores de publicación", r.erroresProductor === 0, `${r.erroresProductor}`));
  } else {
    const h = r.http;
    out.push(c("un POST por hecho y receptor (±N en vuelo), sin reintentos ni fallos",
      h.reintentos === 0 && h.fallidos === 0 && Math.abs(h.recibidas - r.hechosEnVentana * r.n) <= r.n,
      `recibidas ${h.recibidas}, esperadas ≈${r.hechosEnVentana * r.n}, reintentos ${h.reintentos}, fallidos ${h.fallidos}`));
    out.push(c("keep-alive real: ninguna conexión nueva en la ventana", h.conexiones === 0, `${h.conexiones} conexiones nuevas`));
    out.push(c("una conexión viva por receptor", h.abiertas === r.n, `${h.abiertas} abiertas, ${r.n} receptores`));
    out.push(c("ningún cuerpo inválido ni duplicado", h.invalidas === 0 && h.duplicados === 0, `inválidas ${h.invalidas}, duplicadas ${h.duplicados}`));
  }
  return out;
}

export function comprobarC(r) {
  const out = [];
  out.push(c("sin errores", r.errores === 0, `${r.errores} errores en ${r.peticiones} peticiones`));
  out.push(c("el servidor atendió lo que el cliente pidió (±concurrencia en vuelo)", Math.abs((r.http?.atendidas ?? r.atendidas) - r.peticiones) <= r.concurrencia + 1,
    `pedidas ${r.peticiones}, atendidas ${r.http?.atendidas ?? r.atendidas}`));
  if (r.metodo === "http-ka") {
    out.push(c("keep-alive real: ninguna conexión nueva en la ventana y a lo sumo `concurrencia` abiertas", r.http.conexiones === 0 && r.http.abiertas <= r.concurrencia,
      `${r.http.conexiones} nuevas, ${r.http.abiertas} abiertas (concurrencia ${r.concurrencia})`));
  } else if (r.metodo === "http-cerrar") {
    out.push(c("sin keep-alive: una conexión TCP por petición", r.http.conexiones >= 0.98 * r.peticiones - r.concurrencia, `${r.http.conexiones} conexiones, ${r.peticiones} peticiones`));
  } else if (r.nats) {
    // Cada petición: 1 mensaje hacia el servidor (la petición) y 1 de vuelta (la respuesta), cada uno entra y sale del broker.
    out.push(c("el broker vio ~2 mensajes por llamada (petición y respuesta)", r.nats.in_msgs >= 1.9 * r.peticiones - 2 * r.concurrencia && r.nats.in_msgs <= 2.1 * r.peticiones + 2 * r.concurrencia + 50,
      `in_msgs ${r.nats.in_msgs} para ${r.peticiones} peticiones`));
  }
  return out;
}

export function comprobar(escenario, r) {
  return { A: comprobarA, B: comprobarB, C: comprobarC }[escenario](r);
}
