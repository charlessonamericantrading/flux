// Aislamiento entre tenants — Modelo A, 09-multitenancy.md §3.
//
// Estos tests no necesitan broker: comprueban la POLÍTICA (cuándo el SDK se niega a
// suscribirse y qué evento pasa el filtro), que es donde vive la regla del protocolo. El
// enrutado real lo cubre conformance/.

using Xunit;

namespace Flux.Tests;

public class TenantIsolationTests
{
    private const string Subject = "pedidos.pedido.v1.creado";

    [Fact]
    public void OffNoFiltraYSuscribirseSinTenantEsLegal()
    {
        Assert.Null(TenantFilterPolicy.Require(Subject, TenantIsolation.Off, null, null));
    }

    [Fact]
    public void ElTenantDeLaSuscripcionGanaSobreElDeLaConexion()
    {
        Assert.Equal("globex", TenantFilterPolicy.Require(Subject, TenantIsolation.Off, "globex", "acme"));
        Assert.Equal("acme", TenantFilterPolicy.Require(Subject, TenantIsolation.Off, null, "acme"));
    }

    [Fact]
    public void StrictSinTenantConfiguradoEsUnError()
    {
        // Es el punto 3 de §3 y el único que importa de verdad: un filtro que hay que
        // acordarse de poner es un filtro que alguien olvidará, y el fallo —ver los datos de
        // otro tenant— no produce ninguna señal. No hay excepción, no hay log, no hay
        // métrica: hay un incidente de privacidad que se descubre semanas después.
        var e = Assert.Throws<TenantIsolationException>(() =>
            TenantFilterPolicy.Require(Subject, TenantIsolation.Strict, null, null));

        Assert.Equal(Subject, e.Subject);
        Assert.Contains("TODOS los tenants", e.Message, StringComparison.Ordinal);
        Assert.Contains("09-multitenancy.md", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void StrictConTenantLaSuscripcionProcede()
    {
        Assert.Equal("acme", TenantFilterPolicy.Require(Subject, TenantIsolation.Strict, null, "acme"));
        Assert.Equal("globex", TenantFilterPolicy.Require(Subject, TenantIsolation.Strict, "globex", null));
    }

    [Fact]
    public void SystemNoSatisfaceElRequisitoDeFiltro()
    {
        // "system" se reserva para eventos de plataforma SIN tenant. No debe usarse como
        // comodín ni como valor por defecto cuando el tenant real se desconoce: si no se
        // sabe de quién es un evento, el bug está aguas arriba — §5.
        //
        // Si "system" contara como filtro, el modo estricto daría por bueno exactamente el
        // caso que existe para cazar, y además el consumidor descartaría en silencio todos
        // los eventos de tenants reales.
        var e = Assert.Throws<TenantIsolationException>(() =>
            TenantFilterPolicy.Require(Subject, TenantIsolation.Strict, null, "system"));

        Assert.Contains("\"system\"", e.Message, StringComparison.Ordinal);
        Assert.Contains("NO es un tenant", e.Message, StringComparison.Ordinal);

        Assert.Throws<TenantIsolationException>(() =>
            TenantFilterPolicy.Require(Subject, TenantIsolation.Strict, "system", "acme"));
    }

    [Fact]
    public void UnaCadenaVaciaNoEsUnTenant()
    {
        // Ausente ≠ vacío, también aquí: un TenantId = "" no es un tenant llamado "" — es
        // una configuración a medio rellenar (01-envelope.md §3.3).
        Assert.Null(TenantFilterPolicy.Resolve(string.Empty, string.Empty));
        Assert.Throws<TenantIsolationException>(() =>
            TenantFilterPolicy.Require(Subject, TenantIsolation.Strict, string.Empty, string.Empty));
    }

    [Fact]
    public void ElEventoAjenoSeDescartaYElPropioPasa()
    {
        // La misma decisión que toma FluxBus.DispatchAsync antes de invocar al handler. El
        // evento de otro tenant se ACK-ea: no es un fallo y no es para nosotros. Nakearlo lo
        // reentregaría seis veces y acabaría en la DLQ, convirtiendo el aislamiento en una
        // fábrica de ruido — §3, punto 2.
        Assert.True(EsParaNosotros("acme", "acme"));
        Assert.False(EsParaNosotros("acme", "globex"));
        Assert.False(EsParaNosotros("acme", "system"));
        Assert.True(EsParaNosotros(null, "globex"));
    }

    private static bool EsParaNosotros(string? tenantFilter, string tenantIdDelEvento) =>
        tenantFilter is null || string.Equals(tenantFilter, tenantIdDelEvento, StringComparison.Ordinal);
}
