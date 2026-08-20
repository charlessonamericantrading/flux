// Aislamiento entre tenants — Modelo A.
// Contrato normativo: specification/09-multitenancy.md §3
//
// flux v1 implementa el Modelo A: un stream por dominio con todos los tenants mezclados, y
// el SDK filtra por `tenantid` antes de invocar el handler. Conviene decir qué protege eso
// y qué no: cubre al consumidor que olvida filtrar, NO a un adversario. Todo servicio con
// acceso al dominio sigue pudiendo leer los datos de todos los tenants, porque su
// suscripción abarca el subject entero. El aislamiento duro exige una account de NATS por
// tenant (Modelo B), y eso es topología, no SDK.

namespace Flux;

/// <summary>Política de aislamiento entre tenants — 09-multitenancy.md §3.</summary>
public enum TenantIsolation
{
    /// <summary>Default. El filtrado es opcional y se decide por suscripción.</summary>
    Off,

    /// <summary>
    /// Toda suscripción filtra, y suscribirse sin tenant configurado es un ERROR de
    /// configuración.
    /// </summary>
    /// <remarks>
    /// Es el punto que importa de §3. Un filtro que hay que acordarse de poner es un filtro
    /// que alguien olvidará, y el fallo —ver los datos de otro tenant— <b>no produce ningún
    /// error</b>: produce un incidente de privacidad que se descubre semanas después.
    /// </remarks>
    Strict,
}

/// <summary>
/// Resuelve por qué tenant filtra una suscripción, y hace cumplir el modo estricto.
/// </summary>
/// <remarks>
/// Vive fuera de <see cref="FluxBus"/> —igual que <see cref="ConsumerConfigVerifier"/>—
/// porque es política del protocolo y no tiene nada que ver con NATS: así se puede probar
/// entera en un test unitario, sin broker y sin Docker.
/// </remarks>
public static class TenantFilterPolicy
{
    /// <summary>
    /// El tenant por el que hay que filtrar, o <see langword="null"/> si no hay ninguno.
    /// </summary>
    /// <remarks>
    /// El de la suscripción gana sobre el de la conexión.
    /// <para>
    /// <c>"system"</c> NO cuenta: se reserva para eventos de plataforma sin tenant y <b>no
    /// debe usarse como comodín ni como valor por defecto</b> cuando el tenant real se
    /// desconoce. Un consumidor con <c>TenantId = "system"</c> no está aislado de nadie, así
    /// que devolver aquí <c>"system"</c> haría dos cosas mal a la vez: daría por bueno en
    /// modo estricto justo el caso que existe para cazar, y descartaría los eventos de todos
    /// los tenants reales — 09-multitenancy.md §5.
    /// </para>
    /// </remarks>
    /// <param name="subscriptionTenantId">Tenant de la suscripción, si lo hay.</param>
    /// <param name="connectionTenantId">Tenant de la conexión, si lo hay.</param>
    /// <returns>El tenant efectivo, o <see langword="null"/>.</returns>
    public static string? Resolve(string? subscriptionTenantId, string? connectionTenantId)
    {
        var candidate = !string.IsNullOrEmpty(subscriptionTenantId)
            ? subscriptionTenantId
            : connectionTenantId;

        return string.IsNullOrEmpty(candidate) || string.Equals(candidate, "system", StringComparison.Ordinal)
            ? null
            : candidate;
    }

    /// <summary>
    /// Igual que <see cref="Resolve"/>, pero en <see cref="TenantIsolation.Strict"/> exige
    /// que haya filtro.
    /// </summary>
    /// <param name="subject">Subject al que se está suscribiendo.</param>
    /// <param name="isolation">Política configurada en <c>ConnectOptions</c>.</param>
    /// <param name="subscriptionTenantId">Tenant de la suscripción, si lo hay.</param>
    /// <param name="connectionTenantId">Tenant de la conexión, si lo hay.</param>
    /// <returns>El tenant efectivo, o <see langword="null"/> si el aislamiento está apagado.</returns>
    /// <exception cref="TenantIsolationException">
    /// El aislamiento es estricto y no hay ningún tenant por el que filtrar.
    /// </exception>
    public static string? Require(
        string subject,
        TenantIsolation isolation,
        string? subscriptionTenantId,
        string? connectionTenantId)
    {
        var filter = Resolve(subscriptionTenantId, connectionTenantId);
        if (isolation == TenantIsolation.Strict && filter is null)
        {
            throw new TenantIsolationException(subject, subscriptionTenantId, connectionTenantId);
        }

        return filter;
    }
}

/// <summary>
/// Se lanza al suscribirse sin ningún tenant por el que filtrar teniendo
/// <see cref="TenantIsolation.Strict"/> configurado.
/// </summary>
/// <remarks>
/// Es una <b>excepción</b> y no un aviso a propósito, y ése es exactamente el punto 3 de
/// 09-multitenancy.md §3: el fallo que previene —que este consumidor vea los eventos de
/// TODOS los tenants— no produce ninguna señal. No hay excepción, no hay log, no hay
/// métrica: hay un incidente de privacidad que se descubre semanas después, cuando alguien
/// nota datos de un cliente en el informe de otro.
/// <para>
/// Se lanza ANTES de crear el durable consumer: una suscripción mal configurada no debe
/// llegar a existir en el servidor.
/// </para>
/// </remarks>
public sealed class TenantIsolationException : InvalidOperationException
{
    /// <summary>Construye la excepción con el mensaje canónico.</summary>
    /// <param name="subject">Subject al que se intentaba suscribir.</param>
    /// <param name="subscriptionTenant">Tenant de <see cref="SubscribeOptions"/>, si lo había.</param>
    /// <param name="connectionTenant">Tenant de <see cref="ConnectOptions"/>, si lo había.</param>
    public TenantIsolationException(string subject, string? subscriptionTenant, string? connectionTenant)
        : base(Build(subject, subscriptionTenant, connectionTenant))
    {
        Subject = subject;
    }

    /// <summary>Construye la excepción con un mensaje propio.</summary>
    /// <param name="message">El mensaje.</param>
    public TenantIsolationException(string message)
        : base(message)
    {
        Subject = string.Empty;
    }

    /// <summary>Construye la excepción encadenando la causa.</summary>
    /// <param name="message">El mensaje.</param>
    /// <param name="innerException">La causa.</param>
    public TenantIsolationException(string message, Exception innerException)
        : base(message, innerException)
    {
        Subject = string.Empty;
    }

    /// <summary>El subject al que se intentaba suscribir.</summary>
    public string Subject { get; }

    private static string Build(string subject, string? subscriptionTenant, string? connectionTenant)
    {
        // El caso que más se equivoca: "system" parece un tenant y no lo es.
        var motivo =
            string.Equals(subscriptionTenant, "system", StringComparison.Ordinal) ||
            string.Equals(connectionTenant, "system", StringComparison.Ordinal)
                ? "el tenant configurado es \"system\", que NO es un tenant sino su ausencia: se " +
                  "reserva para eventos de plataforma y no debe usarse como comodín ni como valor " +
                  "por defecto cuando el tenant real se desconoce (09-multitenancy.md §5)"
                : "no hay TenantId ni en ConnectOptions ni en SubscribeOptions";

        return $"TenantIsolation=Strict pero {motivo} al suscribirse a \"{subject}\". Sin filtro de " +
               "tenant, este consumidor vería los eventos de TODOS los tenants, y eso no produce " +
               "ningún error visible: produce un incidente de privacidad que se descubre semanas " +
               "después (09-multitenancy.md §3).";
    }
}
