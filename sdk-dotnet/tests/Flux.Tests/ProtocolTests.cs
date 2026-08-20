// Naming y constantes — 02-naming.md, 03-delivery.md.

using System.Globalization;
using Xunit;

namespace Flux.Tests;

public class ProtocolTests
{
    [Theory]
    [InlineData("pedidos.pedido.v1.creado", "pedidos", "pedido", 1, "creado")]
    [InlineData("logistica.envio.v1.entrega-fallida", "logistica", "envio", 1, "entrega-fallida")]
    [InlineData("facturacion.factura.v2.emitida", "facturacion", "factura", 2, "emitida")]
    [InlineData("mi-dominio.mi-agregado.v12.algo-paso", "mi-dominio", "mi-agregado", 12, "algo-paso")]
    public void ParseSubjectValido(string subject, string domain, string aggregate, int major, string evento)
    {
        var parsed = Protocol.ParseSubject(subject);

        Assert.Equal(domain, parsed.Domain);
        Assert.Equal(aggregate, parsed.Aggregate);
        Assert.Equal(major, parsed.Major);
        Assert.Equal(evento, parsed.Event);

        // La transformación es biyectiva.
        Assert.Equal(subject, parsed.ToSubject());
    }

    [Fact]
    public void ParseSubjectRechazaMayusculas()
    {
        // NATS es case-sensitive: "Pedidos." != "pedidos." crea un subject fantasma al que
        // nadie está suscrito y no produce ningún error — 02-naming.md §1.1.
        var e = Assert.Throws<InvalidSubjectException>(
            () => Protocol.ParseSubject("Pedidos.Pedido.V1.Creado"));

        Assert.Contains("minúsculas", e.Message, StringComparison.Ordinal);
        Assert.Contains("nadie está suscrito", e.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("pedidos.crear-pedido", 2)]
    [InlineData("pedidos.pedido.v1.creado.retry", 5)]
    [InlineData("pedidos", 1)]
    public void ParseSubjectRechazaTokensIncorrectos(string subject, int tokens)
    {
        var e = Assert.Throws<InvalidSubjectException>(() => Protocol.ParseSubject(subject));

        Assert.Contains("exactamente 4 tokens", e.Message, StringComparison.Ordinal);
        Assert.Contains(tokens.ToString(CultureInfo.InvariantCulture), e.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("pedidos.pedido.1.creado")]      // falta la `v`
    [InlineData("pedidos.pedido.v0.creado")]     // major debe ser >= 1
    [InlineData("pedidos.pedido_x.v1.creado")]   // guion bajo prohibido
    [InlineData("pedidos..v1.creado")]           // token vacío
    [InlineData("pedidos.pedido.v1.-creado")]    // kebab mal formado
    public void ParseSubjectRechazaFormatosInvalidos(string subject)
    {
        Assert.Throws<InvalidSubjectException>(() => Protocol.ParseSubject(subject));
        Assert.False(Protocol.IsValidSubject(subject));
    }

    [Fact]
    public void SubjectToTypeYVuelta()
    {
        Assert.Equal(
            "com.flux.pedidos.pedido.creado.v1",
            Protocol.SubjectToType("pedidos.pedido.v1.creado"));

        // La transformación es mecánica y biyectiva: el subject enruta (versión en posición
        // fija para los wildcards), el type identifica el contrato — 02-naming.md §2.
        var back = Protocol.ParseType("com.flux.pedidos.pedido.creado.v1");
        Assert.Equal("pedidos.pedido.v1.creado", back.ToSubject());
    }

    [Fact]
    public void StreamNameNoLlevaPuntos()
    {
        // NATS no admite `.` en nombres de stream — 02-naming.md §3.
        Assert.Equal("EVT_PEDIDOS", Protocol.StreamName("pedidos"));
        Assert.Equal("DLQ_PEDIDOS", Protocol.DlqStreamName("pedidos"));
        Assert.Equal("EVT_MI_DOMINIO", Protocol.StreamName("mi-dominio"));
        Assert.False(Protocol.StreamName("mi-dominio").Contains('.', StringComparison.Ordinal));
    }

    [Fact]
    public void DurableNameEsReversible()
    {
        var durable = Protocol.DurableName("facturacion-api", "pedidos.pedido.v1.creado");

        Assert.Equal("facturacion-api__pedidos_pedido_v1_creado", durable);

        // Partiendo por `__` se recuperan servicio y subject exactos — 02-naming.md §4.
        var parts = durable.Split("__");
        Assert.Equal("facturacion-api", parts[0]);
        Assert.Equal("pedidos_pedido_v1_creado", parts[1]);
    }

    [Theory]
    [InlineData("FacturacionAPI")]
    [InlineData("facturacion_api")]
    [InlineData("facturacion api")]
    [InlineData("-facturacion")]
    [InlineData("")]
    public void DurableNameValidaTambienElNombreDeServicio(string service)
    {
        // NATS aceptaría `FacturacionAPI__pedidos_…` sin error, y el incumplimiento solo se
        // descubriría al parsear nombres de consumidor en una herramienta
        // — protocol.json naming.service.
        Assert.Throws<InvalidServiceNameException>(
            () => Protocol.DurableName(service, "pedidos.pedido.v1.creado"));
    }

    [Fact]
    public void DlqSubjectEsPrefijoNoSufijo()
    {
        var dlq = Protocol.DlqSubject("pedidos.pedido.v1.creado");

        Assert.Equal("dlq.pedidos.pedido.v1.creado", dlq);
        Assert.True(Protocol.IsDlqSubject(dlq));

        // Un sufijo encajaría con `pedidos.>` y el stream principal capturaría sus propios
        // muertos — 02-naming.md §3.1.
        Assert.False(dlq.StartsWith("pedidos.", StringComparison.Ordinal));
    }

    [Fact]
    public void SourceUriIdentificaEntornoYServicio()
    {
        Assert.Equal("/produccion/pedidos-api", Protocol.SourceUri("produccion", "pedidos-api"));
    }

    [Fact]
    public void InvarianteAckWaitIgualBackoffCero()
    {
        // La invariante más cara del protocolo: JetStream SOBRESCRIBE ack_wait con
        // backoff[0] sin avisar — 03-delivery.md §2.1.
        Assert.Equal(Protocol.DefaultAckWait, Protocol.CanonicalBackoff[0]);

        // max_deliver = 1 entrega inicial + una por entrada de backoff. Si no cuadran, la
        // última entrada no se aplicaría nunca y la config mentiría sobre sí misma.
        Assert.Equal(Protocol.DefaultMaxDeliver - 1, Protocol.CanonicalBackoff.Count);
    }

    [Fact]
    public void BackoffCanonicoYTiempoHastaLaDlq()
    {
        Assert.Equal(
            new[]
            {
                TimeSpan.FromSeconds(30),
                TimeSpan.FromMinutes(1),
                TimeSpan.FromMinutes(5),
                TimeSpan.FromMinutes(15),
                TimeSpan.FromMinutes(30),
            },
            Protocol.CanonicalBackoff);

        // protocol.json: totalTimeToDlqSeconds = 3090.
        Assert.Equal(3090, (int)Protocol.TotalTimeToDlq().TotalSeconds);
    }

    [Fact]
    public void WipSeEmiteCadaMedioAckWait()
    {
        Assert.Equal(TimeSpan.FromSeconds(15), Protocol.WorkInProgressInterval);
    }

    [Fact]
    public void ElBackoffCanonicoNoEsMutableDesdeFuera()
    {
        // Una entrada [0] alterada cambiaría en silencio el ack_wait efectivo de todo
        // consumidor creado después. ReadOnlyCollection encapsula el array, así que no hay
        // forma de recuperarlo con un cast.
        Assert.IsNotType<TimeSpan[]>(Protocol.CanonicalBackoff);
        Assert.True(((IList<TimeSpan>)Protocol.CanonicalBackoff).IsReadOnly);
    }

    [Fact]
    public void NewEventIdEsUnUuidV7()
    {
        var id = Protocol.NewEventId();
        var text = id.ToString();

        // Formato canónico en minúsculas, 36 caracteres.
        Assert.Equal(36, text.Length);
        Assert.Equal(text.ToLowerInvariant(), text);

        // Versión 7 en el primer dígito del tercer grupo y variante 10b en el cuarto.
        var groups = text.Split('-');
        Assert.Equal('7', groups[2][0]);
        Assert.True("89ab".Contains(groups[3][0], StringComparison.Ordinal));

        // Los 48 bits altos son el timestamp Unix en ms: el id debe situarse en el presente.
        var millis = long.Parse(groups[0] + groups[1], NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        var instant = DateTimeOffset.FromUnixTimeMilliseconds(millis);
        Assert.InRange(
            instant,
            DateTimeOffset.UtcNow.AddMinutes(-1),
            DateTimeOffset.UtcNow.AddMinutes(1));
    }

    [Fact]
    public void NewEventIdEsMonotonicoComoTexto()
    {
        // El protocolo se apoya en que ordenar por `id` dentro de un mismo `source` equivale
        // a ordenar por instante de generación — 01-envelope.md §2.4. En una ráfaga dentro
        // del mismo milisegundo eso solo se cumple si rand_a se usa como contador.
        var ids = new List<string>();
        for (var i = 0; i < 2000; i++)
        {
            ids.Add(Protocol.NewEventId().ToString());
        }

        var ordenados = ids.OrderBy(x => x, StringComparer.Ordinal).ToList();
        Assert.Equal(ordenados, ids);
        Assert.Equal(ids.Count, ids.Distinct(StringComparer.Ordinal).Count());
    }
}
