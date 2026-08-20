/*
 * Suscripcion sin filtro de tenant con el aislamiento en estricto.
 * Contrato normativo: specification/09-multitenancy.md §3
 */
package com.flux;

/**
 * Se lanza al suscribirse sin ningun tenant por el que filtrar teniendo
 * {@link FluxBus.TenantIsolation#STRICT} configurado.
 *
 * <p>Es una <b>excepcion</b> y no un aviso a proposito, y ese es exactamente el punto 3 de
 * 09-multitenancy.md §3: un filtro que hay que acordarse de poner es un filtro que alguien
 * olvidara, y el fallo —que este consumidor vea los eventos de TODOS los tenants— no
 * produce ninguna senal. No hay excepcion, no hay log, no hay metrica: hay un incidente de
 * privacidad que se descubre semanas despues, cuando alguien nota datos de un cliente en el
 * informe de otro.
 *
 * <p>Es {@link IllegalStateException} y no comprobada: no es una condicion de ejecucion que
 * la aplicacion deba manejar, es una configuracion incoherente que debe romper el arranque.
 */
public final class TenantIsolationException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    private final transient String subject;

    /**
     * @param subject             subject al que se intentaba suscribir.
     * @param subscriptionTenant  tenant de {@code SubscribeOptions}, si lo habia.
     * @param connectionTenant    tenant de {@code ConnectOptions}, si lo habia.
     */
    public TenantIsolationException(String subject, String subscriptionTenant, String connectionTenant) {
        super(build(subject, subscriptionTenant, connectionTenant));
        this.subject = subject;
    }

    private static String build(String subject, String subscriptionTenant, String connectionTenant) {
        String motivo;
        if ("system".equals(subscriptionTenant) || "system".equals(connectionTenant)) {
            // El caso que mas se equivoca: "system" parece un tenant y no lo es.
            motivo = "el tenant configurado es \"system\", que NO es un tenant sino su ausencia: "
                    + "se reserva para eventos de plataforma y no debe usarse como comodin ni como "
                    + "valor por defecto cuando el tenant real se desconoce (09-multitenancy.md §5)";
        } else {
            motivo = "no hay tenantId ni en connect() ni en subscribe()";
        }
        return "tenantIsolation=STRICT pero " + motivo + " al suscribirse a \"" + subject + "\". "
                + "Sin filtro de tenant, este consumidor veria los eventos de TODOS los tenants, y eso "
                + "no produce ningun error visible: produce un incidente de privacidad que se descubre "
                + "semanas despues (09-multitenancy.md §3).";
    }

    /** El subject al que se intentaba suscribir. */
    public String subject() {
        return subject;
    }
}
