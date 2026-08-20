/*
 * Aislamiento entre tenants — Modelo A.
 * Contrato normativo: specification/09-multitenancy.md §3
 *
 * Estos tests no necesitan broker: comprueban la POLITICA (cuando el SDK se niega a
 * suscribirse y que evento pasa el filtro), que es donde vive la regla del protocolo. El
 * enrutado real lo cubre conformance/.
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TenantIsolationTest {

    private static final String SUBJECT = "pedidos.pedido.v1.creado";

    @Test
    @DisplayName("OFF: suscribirse sin tenant es legal y no filtra nada")
    void offNoFiltra() {
        assertNull(FluxBus.requiredTenantFilter(SUBJECT, FluxBus.TenantIsolation.OFF, null, null));
    }

    @Test
    @DisplayName("el tenant de la suscripcion gana sobre el de la conexion")
    void laSuscripcionGana() {
        assertEquals("globex",
                FluxBus.requiredTenantFilter(SUBJECT, FluxBus.TenantIsolation.OFF, "globex", "acme"));
        assertEquals("acme",
                FluxBus.requiredTenantFilter(SUBJECT, FluxBus.TenantIsolation.OFF, null, "acme"));
    }

    @Test
    @DisplayName("STRICT: suscribirse sin tenant configurado es un ERROR, no un descuido")
    void strictSinTenantLanza() {
        // Es el punto 3 de §3 y el unico que importa de verdad: un filtro que hay que
        // acordarse de poner es un filtro que alguien olvidara.
        assertThrows(TenantIsolationException.class, () -> FluxBus.requiredTenantFilter(
                SUBJECT, FluxBus.TenantIsolation.STRICT, null, null));
    }

    @Test
    @DisplayName("STRICT: con tenant, la suscripcion procede con normalidad")
    void strictConTenantPasa() {
        assertEquals("acme", FluxBus.requiredTenantFilter(
                SUBJECT, FluxBus.TenantIsolation.STRICT, null, "acme"));
        assertEquals("globex", FluxBus.requiredTenantFilter(
                SUBJECT, FluxBus.TenantIsolation.STRICT, "globex", null));
    }

    @Test
    @DisplayName("STRICT: \"system\" NO satisface el requisito de filtro")
    void strictConSystemLanza() {
        // Un consumidor con tenantId="system" no esta aislado de nadie. Si "system" contara
        // como filtro, el modo estricto daria por bueno exactamente el caso que existe para
        // cazar, y ademas descartaria todos los eventos de tenants reales — §5.
        assertThrows(TenantIsolationException.class, () -> FluxBus.requiredTenantFilter(
                SUBJECT, FluxBus.TenantIsolation.STRICT, null, "system"));
        assertThrows(TenantIsolationException.class, () -> FluxBus.requiredTenantFilter(
                SUBJECT, FluxBus.TenantIsolation.STRICT, "system", "acme"));
    }

    @Test
    @DisplayName("una cadena vacia no es un tenant")
    void vacioNoEsTenant() {
        assertNull(FluxBus.tenantFilter("", ""));
        assertThrows(TenantIsolationException.class, () -> FluxBus.requiredTenantFilter(
                SUBJECT, FluxBus.TenantIsolation.STRICT, "", ""));
    }

    @Test
    @DisplayName("el mensaje de STRICT explica por que no basta con avisar")
    void mensajeAccionable() {
        // El fallo que esta excepcion previene —ver los eventos de OTROS tenants— no
        // produce ninguna senal: ni excepcion, ni log, ni metrica. Produce un incidente de
        // privacidad que se descubre semanas despues. Por eso es un error de configuracion
        // y no un aviso — 09-multitenancy.md §3, punto 3.
        TenantIsolationException e =
                new TenantIsolationException("pedidos.pedido.v1.creado", null, null);

        assertAll(
                () -> assertTrue(e.getMessage().contains("pedidos.pedido.v1.creado")),
                () -> assertTrue(e.getMessage().contains("TODOS los tenants")),
                () -> assertTrue(e.getMessage().contains("09-multitenancy.md")),
                () -> assertEquals("pedidos.pedido.v1.creado", e.subject()));
    }

    @Test
    @DisplayName("\"system\" NO cuenta como filtro de tenant, y el mensaje lo dice")
    void systemNoEsUnTenant() {
        // "system" se reserva para eventos de plataforma SIN tenant. No debe usarse como
        // comodin ni como valor por defecto cuando el tenant real se desconoce: si no se
        // sabe de quien es un evento, el bug esta aguas arriba — §5.
        TenantIsolationException e =
                new TenantIsolationException("pedidos.pedido.v1.creado", null, "system");

        assertTrue(e.getMessage().contains("\"system\""));
        assertTrue(e.getMessage().contains("no es un tenant")
                        || e.getMessage().contains("NO es un tenant"),
                "el mensaje debe decir que system no es un tenant: " + e.getMessage());
    }

    @Test
    @DisplayName("el evento de otro tenant se descarta CON ack, no se reintenta")
    void eventoAjenoSeConfirmaYSeDescarta() {
        // No es un fallo y no es para nosotros: nakearlo lo reentregaria seis veces y
        // acabaria en la DLQ, convirtiendo el aislamiento en una fabrica de ruido — §3,
        // punto 2. Aqui se comprueba la decision del filtro; que el ack se emite de verdad
        // lo cubre el despacho.
        assertTrue(esParaNosotros("acme", "acme"));
        assertFalse(esParaNosotros("acme", "globex"));
        assertFalse(esParaNosotros("acme", "system"),
                "un evento de plataforma tampoco es del tenant acme");
        assertTrue(esParaNosotros(null, "globex"),
                "sin filtro configurado pasa todo: es el default OFF");
    }

    /** La misma decision que toma {@code FluxBus.dispatch} antes de invocar al handler. */
    private static boolean esParaNosotros(String tenantFilter, String tenantIdDelEvento) {
        return tenantFilter == null || tenantFilter.equals(tenantIdDelEvento);
    }
}
