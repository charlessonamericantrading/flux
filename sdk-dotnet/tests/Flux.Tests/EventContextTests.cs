// Propagación de contexto entre eventos — 01-envelope.md §5.

using System.Diagnostics;
using Xunit;

namespace Flux.Tests;

public class EventContextTests
{
    private static FluxEvent Entrante() => Envelope.BuildEvent(new Envelope.BuildEventInput
    {
        Subject = "pedidos.pedido.v1.creado",
        Data = new { pedidoId = "ped-123" },
        Id = "evento-hijo",
        Source = "/produccion/pedidos-api",
        ProducerVersion = "3.4.1",
        TenantId = "acme",
        DataClassification = DataClassification.Internal,
        DataSchema = "https://s/x.json",
        CorrelationId = "flujo-raiz",
        CausationId = "evento-abuelo",
        TraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    });

    [Fact]
    public void FromEventEncadenaLaCausalidad()
    {
        var contexto = FluxContext.FromEvent(Entrante());

        // correlationid se propaga SIN MODIFICAR: identifica el flujo de negocio completo.
        Assert.Equal("flujo-raiz", contexto.CorrelationId);

        // causationid toma el `id` del evento, NO su causationid: la causa de lo que se
        // publique ahora es ESTE evento, no el que lo causó a él — 01-envelope.md §3.2.
        Assert.Equal("evento-hijo", contexto.CausationId);
        Assert.NotEqual("evento-abuelo", contexto.CausationId);

        // El tenant del evento gana sobre el default del servicio: un evento derivado
        // pertenece al tenant del que lo causó.
        Assert.Equal("acme", contexto.TenantId);
        Assert.Equal("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", contexto.TraceParent);
    }

    [Fact]
    public void SinContextoActivoNoHayNadaQueHeredar()
    {
        // El caso normal de un publish desde una ruta HTTP o un cron: el evento nace de cero
        // y PublishAsync inicializará su correlationid con su propio id.
        Assert.Null(FluxContext.CurrentContext);
    }

    [Fact]
    public async Task ElContextoFluyeATravesDeAwaitYDeTareasHijas()
    {
        // Esto es lo que en Go NO se puede hacer: allí el contexto viaja explícito en el
        // context.Context y pasar el equivocado rompe la cadena de correlación en silencio.
        // AsyncLocal<T> es el equivalente del AsyncLocalStorage de Node.
        var contexto = FluxContext.FromEvent(Entrante());

        using (FluxContext.Push(contexto))
        {
            await Task.Yield();
            Assert.Equal("flujo-raiz", FluxContext.CurrentContext?.CorrelationId);

            await Task.Run(() =>
            {
                Assert.Equal("flujo-raiz", FluxContext.CurrentContext?.CorrelationId);
            });

            await ProfundoAsync(4);
        }

        Assert.Null(FluxContext.CurrentContext);
    }

    [Fact]
    public void LosScopesAnidadosSeRestauranEnOrden()
    {
        var externo = new EventContext { CorrelationId = "externo" };
        var interno = new EventContext { CorrelationId = "interno" };

        using (FluxContext.Push(externo))
        {
            Assert.Equal("externo", FluxContext.CurrentContext?.CorrelationId);

            using (FluxContext.Push(interno))
            {
                Assert.Equal("interno", FluxContext.CurrentContext?.CorrelationId);
            }

            Assert.Equal("externo", FluxContext.CurrentContext?.CorrelationId);
        }

        Assert.Null(FluxContext.CurrentContext);
    }

    [Fact]
    public void ElTraceparentSaleDeActivityCurrent()
    {
        // Ventaja de .NET: el trace context vive en el BCL, así que no hace falta ni el
        // import() dinámico de @opentelemetry/api de Node ni la inyección explícita de Go.
        Activity.DefaultIdFormat = ActivityIdFormat.W3C;
        using var actividad = new Activity("test").Start();

        var traceparent = FluxContext.ActiveTraceparent();

        Assert.NotNull(traceparent);
        Assert.Matches("^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$", traceparent!);
    }

    [Fact]
    public void SinSpanActivoElTraceparentSeOmite()
    {
        // Devolver "" o un traceparent inventado produciría un atributo sintácticamente
        // inválido, que es peor que omitirlo — 01-envelope.md §3.3.
        Assert.Null(new Activity("sin-arrancar").Id);
    }

    private static async Task ProfundoAsync(int niveles)
    {
        if (niveles == 0)
        {
            Assert.Equal("flujo-raiz", FluxContext.CurrentContext?.CorrelationId);
            return;
        }

        await Task.Delay(1).ConfigureAwait(false);
        await ProfundoAsync(niveles - 1).ConfigureAwait(false);
    }
}
