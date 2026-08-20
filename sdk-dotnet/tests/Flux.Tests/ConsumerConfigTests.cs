// Verificación de la config efectiva del consumidor — 03-delivery.md §2.1, requisito L2.

using Xunit;

namespace Flux.Tests;

public class ConsumerConfigTests
{
    [Fact]
    public void AceptaLaConfigCanonica()
    {
        var canonica = ConsumerConfigSnapshot.Canonical();

        ConsumerConfigVerifier.AssertHonored("svc__pedidos_pedido_v1_creado", canonica, canonica);
    }

    [Fact]
    public void DetectaLaSobrescrituraSilenciosaDeAckWait()
    {
        // La trampa más cara de JetStream: pides ack_wait 30 s con un backoff que empieza en
        // 1 s y el servidor te devuelve ack_wait 1 s, sin error. Cualquier handler que toque
        // una base de datos se ejecuta entonces en concurrencia consigo mismo.
        var solicitada = ConsumerConfigSnapshot.Canonical();
        var efectiva = solicitada with
        {
            AckWait = TimeSpan.FromSeconds(1),
            Backoff = new[]
            {
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(5),
                TimeSpan.FromSeconds(30),
                TimeSpan.FromMinutes(2),
                TimeSpan.FromMinutes(10),
            },
        };

        var e = Assert.Throws<ConsumerConfigMismatchException>(
            () => ConsumerConfigVerifier.AssertHonored("svc__x", solicitada, efectiva));

        Assert.Contains(e.Differences, d => d.Field == "ack_wait");
        Assert.Contains(e.Differences, d => d.Field == "backoff");
        Assert.Contains("backoff[0]", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DetectaOtrosCamposAlterados()
    {
        var solicitada = ConsumerConfigSnapshot.Canonical();

        foreach (var efectiva in new[]
                 {
                     solicitada with { MaxDeliver = 3 },
                     solicitada with { MaxAckPending = 1 },
                     solicitada with { AckPolicy = "all" },
                 })
        {
            Assert.Throws<ConsumerConfigMismatchException>(
                () => ConsumerConfigVerifier.AssertHonored("svc__x", solicitada, efectiva));
        }
    }

    [Fact]
    public void ValidaLaInvarianteSobreLaConfigEfectiva()
    {
        // Aunque el servidor devuelva EXACTAMENTE lo que se le pidió, ack_wait y backoff[0]
        // tienen que coincidir. Si alguien cambia el backoff canónico y olvida
        // DefaultAckWait, se caza aquí y no en producción a las 3 de la mañana.
        var rota = ConsumerConfigSnapshot.Canonical() with
        {
            AckWait = TimeSpan.FromSeconds(5),
            Backoff = new[] { TimeSpan.FromSeconds(30) },
        };

        var e = Assert.Throws<ConsumerConfigMismatchException>(
            () => ConsumerConfigVerifier.AssertHonored("svc__x", rota, rota));

        Assert.Contains(e.Differences, d => d.Field == "ack_wait == backoff[0]");
    }

    [Fact]
    public void ElMensajeExplicaQueHacer()
    {
        var solicitada = ConsumerConfigSnapshot.Canonical();
        var efectiva = solicitada with { MaxDeliver = 3 };

        var e = Assert.Throws<ConsumerConfigMismatchException>(
            () => ConsumerConfigVerifier.AssertHonored("facturacion-api__pedidos_pedido_v1_creado", solicitada, efectiva));

        Assert.Contains("facturacion-api__pedidos_pedido_v1_creado", e.Message, StringComparison.Ordinal);
        Assert.Contains("max_deliver: solicitado 6, efectivo 3", e.Message, StringComparison.Ordinal);
        Assert.Contains("03-delivery.md §2.1", e.Message, StringComparison.Ordinal);
    }
}
