/**
 * EL «ANTES»: cómo se integran estos mismos sistemas con APIs y webhooks, fila a fila del mapa de sustitución (docs/migracion.md).
 *
 * No es un adorno: es código que FUNCIONA (`test/antes-http.test.mjs` lo ejecuta contra servidores `node:http` de verdad) y que los
 * fragmentos de la guía citan literalmente. Así «antes» y «después» se comparan con código real en los dos lados, y se ve lo que
 * cada versión obliga a escribir. Sin dependencias: solo `node:http`, `node:crypto` y `fetch`.
 *
 * Un fallo de red, un receptor caído o un reintento mal hecho NO están resueltos aquí a propósito: son justo lo que el «antes» deja a
 * cada integración.
 */

import { createHmac, timingSafeEqual } from "node:crypto";

// fragmento:antes-fila-1
// Webhook: el CRM avisa a cada sistema suscrito con un POST. Si un receptor está caído, el aviso se pierde (o hay que escribir los reintentos),
// y cada alta nueva obliga a dar de alta una URL más. Nótese el correo del cliente en el cuerpo: un dato personal en un mensaje que viaja.
export async function avisarAltaDeCliente(suscriptores, cliente) {
  await Promise.all(suscriptores.map((url) => fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ evento: "cliente.registrado", clienteId: cliente.clienteId, email: cliente.email }),
  })));
}
// /fragmento:antes-fila-1

// fragmento:antes-fila-2
// Polling, el servidor: el CRM guarda cada cambio con un número de secuencia y lo sirve a quien pregunte (`GET /cambios?desde=`).
export const cambios = [];
export function servirCambios(req, res) {
  const desde = Number(new URL(req.url, "http://crm").searchParams.get("desde") ?? 0);
  res.setHeader("content-type", "application/json");
  res.end(JSON.stringify(cambios.filter((c) => c.seq > desde)));
}
// Polling, el cliente: pregunta cada T milisegundos, haya cambiado algo o no, y por cada cliente que lo haga.
export function sondear(url, cadaMs, alCambiar) {
  let desde = 0;
  return setInterval(async () => {
    for (const c of await (await fetch(`${url}/cambios?desde=${desde}`)).json()) {
      desde = c.seq;
      alCambiar(c);
    }
  }, cadaMs);
}
// /fragmento:antes-fila-2

// fragmento:antes-fila-3
// `GET /clientes/123`: leer un dato que guarda otro sistema. Si el CRM está caído, la tienda también lo está; cada visita es una petición.
export async function leerCliente(base, clienteId) {
  const r = await fetch(`${base}/clientes/${clienteId}`);
  if (r.status === 404) return null;
  if (!r.ok) throw new Error(`el CRM respondió ${r.status}`);
  return r.json();
}
// /fragmento:antes-fila-3

// fragmento:antes-fila-4
// La carga inicial: `GET /clientes?page=` hasta que se acaben, y a escribir a mano cómo se pasa de «cargado» a «al día».
export async function cargarTodos(base) {
  const todos = [];
  for (let pagina = 1; ; pagina++) {
    const lote = await (await fetch(`${base}/clientes?page=${pagina}`)).json();
    if (lote.length === 0) return todos;
    todos.push(...lote);
  }
}
// /fragmento:antes-fila-4

// fragmento:antes-fila-5
// `POST /accion` que necesita respuesta ya: una orden. La `Idempotency-Key` es un convenio de esta API (otra, no la tendría), y nadie
// obliga al servidor a respetarla.
export async function sumarPuntos(base, clienteId, puntos, referencia) {
  const r = await fetch(`${base}/clientes/${clienteId}/puntos`, {
    method: "POST",
    headers: { "content-type": "application/json", "idempotency-key": referencia },
    body: JSON.stringify({ puntos }),
  });
  if (!r.ok) throw new Error(`el CRM respondió ${r.status}`);
  return r.json();
}
// /fragmento:antes-fila-5

// fragmento:antes-fila-6
// `GET /descuento?…`: un cálculo, no un dato guardado.
export async function calcularDescuento(base, clienteId, totalCents) {
  const r = await fetch(`${base}/descuento?clienteId=${encodeURIComponent(clienteId)}&totalCents=${totalCents}`);
  if (!r.ok) throw new Error(`el CRM respondió ${r.status}`);
  return r.json();
}
// /fragmento:antes-fila-6

// fragmento:antes-fila-7
// `POST /facturas` asíncrono: responde 202 con una dirección donde mirar, y quien pidió pregunta hasta que el estado cambie (o monta un
// webhook de resultado, que es la fila 1 otra vez). Nada une la petición con su resultado salvo la URL.
export async function facturarYEsperar(base, pedido, { cadaMs = 200, intentos = 50 } = {}) {
  const r = await fetch(`${base}/facturas`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(pedido) });
  if (r.status !== 202) throw new Error(`facturación respondió ${r.status}`);
  const { location } = await r.json();
  for (let i = 0; i < intentos; i++) {
    const f = await (await fetch(`${base}${location}`)).json();
    if (f.estado === "lista") return f;
    await new Promise((ok) => setTimeout(ok, cadaMs));
  }
  throw new Error("la factura no estuvo lista a tiempo");
}
// /fragmento:antes-fila-7

// fragmento:antes-fila-8
// Webhook entrante de un tercero (una pasarela de pagos): hay que verificar la firma sobre los bytes en bruto, comparar en tiempo constante,
// mirar la tolerancia y deduplicar por el id del evento: cuatro cosas que cada receptor escribe —y puede equivocar— por su cuenta.
export function recibirCobro({ secreto, yaVistos, guardar }) {
  return (req, res) => {
    const trozos = [];
    req.on("data", (t) => trozos.push(t));
    req.on("end", () => {
      const cuerpo = Buffer.concat(trozos);
      const [t, v1] = String(req.headers["x-pasarela-firma"] ?? "").split(",").map((p) => p.split("=")[1] ?? "");
      const esperado = createHmac("sha256", secreto).update(`${t}.`).update(cuerpo).digest("hex");
      const igual = v1.length === esperado.length && timingSafeEqual(Buffer.from(v1), Buffer.from(esperado));
      if (!igual || Math.abs(Date.now() / 1000 - Number(t)) > 300) { res.statusCode = 401; return res.end(); }
      const cobro = JSON.parse(cuerpo.toString("utf8"));
      if (!yaVistos.has(cobro.id)) { yaVistos.add(cobro.id); guardar(cobro); }
      res.statusCode = 200;
      res.end();
    });
  };
}
// /fragmento:antes-fila-8

// fragmento:antes-fila-9
// Webhook saliente a quien no puede conectarse: el bucle de reintentos, el backoff y qué hacer con lo que no se pudo entregar es tuyo.
export async function entregarConReintentos(url, evento, { intentos = 3, esperaMs = 100 } = {}) {
  for (let i = 1; ; i++) {
    let estado;
    try {
      const r = await fetch(url, { method: "POST", headers: { "content-type": "application/json", "idempotency-key": evento.id }, body: JSON.stringify(evento) });
      if (r.ok) return i;
      estado = r.status;
    } catch {
      estado = 0; // sin respuesta: se reintenta
    }
    // Un 4xx (salvo 429) no se arregla repitiéndolo: es un rechazo, y reintentarlo es ruido. Un 5xx o la red caída sí se reintentan.
    if (estado >= 400 && estado < 500 && estado !== 429) throw new Error(`el receptor rechazó el evento (${estado})`);
    if (i >= intentos) throw new Error("agotados los reintentos");
    await new Promise((ok) => setTimeout(ok, esperaMs * 2 ** i));
  }
}
// /fragmento:antes-fila-9
