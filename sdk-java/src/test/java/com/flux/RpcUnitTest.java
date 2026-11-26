/*
 * Peticion/respuesta SIN broker: las piezas puras del cliente que no cubre el arnes cruzado. Lo que necesita un broker (el ciclo, los
 * permisos, la parada, las cinco comprobaciones de una respuesta…) esta en RpcIntegrationTest.
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flux.RpcErrors.InvalidRpcCallException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RpcUnitTest {

    @Test
    @DisplayName("readableId: solo una cadena valida (1–128 puntos de codigo, sin caracteres de control) se cita como causationid")
    void idLegible() {
        assertEquals("abc-1", FluxRpc.readableId("{\"id\":\"abc-1\"}".getBytes()));
        assertNull(FluxRpc.readableId("{\"id\":\"\"}".getBytes()));
        assertNull(FluxRpc.readableId("{\"id\":42}".getBytes()));
        assertNull(FluxRpc.readableId("{\"id\":\"a\\nb\"}".getBytes()), "un salto de linea llenaria un log");
        assertNull(FluxRpc.readableId("{\"id\":\"a\\u0085b\"}".getBytes()), "U+0085 es de control");
        assertNull(FluxRpc.readableId(("{\"id\":\"" + "x".repeat(129) + "\"}").getBytes()));
        assertEquals("x".repeat(128), FluxRpc.readableId(("{\"id\":\"" + "x".repeat(128) + "\"}").getBytes()));
        assertEquals("🚀".repeat(128), FluxRpc.readableId(("{\"id\":\"" + "🚀".repeat(128) + "\"}").getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                "128 PUNTOS DE CODIGO, no unidades UTF-16");
        assertNull(FluxRpc.readableId("no es json".getBytes()));
        assertNull(FluxRpc.readableId("[1]".getBytes()));
    }

    @Test
    @DisplayName("isUnreadableRequestError: solo un error con tenantid system y correlationid igual a su propio id (no basta uno de los dos)")
    void errorDeUnaPeticionIlegible() {
        FluxEvent base = RpcEnvelope.buildError(new RpcEnvelope.ErrorInput()
                .request(new RpcEnvelope.RequestRef("clientes.cliente.v1.obtener", "req-1", "req-1", "system", null, null))
                .id("e-1").source("/produccion/crm-api").time("2026-08-20T10:41:07.318Z").producerversion("1.0.0")
                .error(new RpcEnvelope.ErrorData("permanent", "MALFORMED_JSON", "x", null)));
        // correlationid = id del request, no el del error: NO es la forma
        assertFalse(FluxRpc.isUnreadableRequestError(base, true));
        FluxEvent propio = RpcEnvelope.buildError(new RpcEnvelope.ErrorInput()
                .request(new RpcEnvelope.RequestRef("clientes.cliente.v1.obtener", "req-1", "e-1", "system", null, null))
                .id("e-1").source("/produccion/crm-api").time("2026-08-20T10:41:07.318Z").producerversion("1.0.0")
                .error(new RpcEnvelope.ErrorData("permanent", "MALFORMED_JSON", "x", null)));
        assertTrue(FluxRpc.isUnreadableRequestError(propio, true));
        assertFalse(FluxRpc.isUnreadableRequestError(propio, false), "una respuesta que no es un error no tiene esta excepcion");
        FluxEvent otroTenant = RpcEnvelope.buildError(new RpcEnvelope.ErrorInput()
                .request(new RpcEnvelope.RequestRef("clientes.cliente.v1.obtener", "req-1", "e-1", "acme", null, null))
                .id("e-1").source("/produccion/crm-api").time("2026-08-20T10:41:07.318Z").producerversion("1.0.0")
                .error(new RpcEnvelope.ErrorData("permanent", "MALFORMED_JSON", "x", null)));
        assertFalse(FluxRpc.isUnreadableRequestError(otroTenant, true), "tenantid distinto de system: no basta el correlationid");
    }

    @Test
    @DisplayName("las opciones de una llamada se validan SIN red, con INVALID_RPC_CALL y sin recortar en silencio")
    void opcionesInvalidas() {
        assertThrows(InvalidRpcCallException.class, () -> RpcEnvelope.resolveTimeout(0));
        assertThrows(InvalidRpcCallException.class, () -> RpcEnvelope.resolveTimeout(25_001));
        assertEquals(1, RpcEnvelope.resolveTimeout(1));
        assertEquals(25_000, RpcEnvelope.resolveTimeout(25_000));
        assertEquals(5_000, RpcEnvelope.resolveTimeout((Integer) null));
    }
}
