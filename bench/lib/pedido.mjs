/**
 * El HECHO del escenario B: un pedido creado (`pedidos.pedido.v1.creado`), con el mismo contenido de negocio para el webhook y para
 * el evento. El número de secuencia viaja en `pedidoId` (`ped-` + base 36, que es lo que admite el esquema): así el receptor sabe
 * qué hecho es sin que el banco meta nada ajeno al contrato en `data`.
 */

export const SUBJECT_PEDIDO = "pedidos.pedido.v1.creado";

export const pedidoId = (seq) => `ped-${seq.toString(36).padStart(6, "0")}`;
export const seqDe = (id) => parseInt(id.slice(4), 36);

export function datosPedido(seq) {
  return {
    pedidoId: pedidoId(seq),
    clienteId: "cli-000042",
    aggregateVersion: 1,
    totalCents: 9990,
    moneda: "EUR",
    canal: "web",
    lineas: [
      { sku: "ABC-1", cantidad: 2, precioUnitarioCents: 4995 },
      { sku: "XYZ-9", cantidad: 1, precioUnitarioCents: 0 },
    ],
    direccionEnvioId: "dir-0001",
  };
}
