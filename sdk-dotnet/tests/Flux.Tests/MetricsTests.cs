// Métricas del SDK — 08-observability.md.

using System.Text.RegularExpressions;
using Xunit;

namespace Flux.Tests;

public class MetricsTests
{
    /// <summary>Una línea de exposición válida: nombre, etiquetas opcionales y un número.</summary>
    private static readonly Regex LineaPrometheus = new(@"^[a-z_]+(\{[^}]*\})? -?[0-9.]+$", RegexOptions.None);

    // ─── buckets ─────────────────────────────────────────────────────────────

    [Fact]
    public void ElUltimoBucketEsElAckWait()
    {
        // 08-observability.md §3: un handler en el bucket superior está a punto de que su
        // mensaje se reentregue MIENTRAS aún se ejecuta. Si el bucket no coincide con el
        // plazo real, mide algo que no le importa a nadie — y el bucket DEBE moverse si
        // alguien cambia ack_wait. Este test es lo que lo obliga.
        var ultimo = InMemoryMetrics.DurationBuckets[^1];
        Assert.Equal(Protocol.DefaultAckWait.TotalSeconds, ultimo);
    }

    [Fact]
    public void SonExactamenteLosDoceDeLaSpecEnOrdenAscendente()
    {
        Assert.Equal(
            new[] { 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0 },
            InMemoryMetrics.DurationBuckets);

        for (var i = 1; i < InMemoryMetrics.DurationBuckets.Count; i++)
        {
            Assert.True(InMemoryMetrics.DurationBuckets[i] > InMemoryMetrics.DurationBuckets[i - 1]);
        }
    }

    // ─── nombres y etiquetas del contrato ────────────────────────────────────

    [Fact]
    public void LasSieteMetricasUsanLosNombresYEtiquetasDeLaSpec()
    {
        // Son un CONTRATO con los dashboards y las alertas, no una decisión de este SDK: si
        // .NET y Go nombran distinto la tasa de DLQ, no se pueden sumar y un panel del
        // ecosistema es imposible — §1.
        var salida = TodasLasMetricas().Render();

        Assert.Contains(
            "flux_events_published_total{outcome=\"ok\",subject=\"pedidos.pedido.v1.creado\"} 1",
            salida,
            StringComparison.Ordinal);
        Assert.Contains(
            "flux_events_consumed_total{consumer=\"facturacion-api__x\",outcome=\"ok\"," +
            "subject=\"pedidos.pedido.v1.creado\"} 1",
            salida,
            StringComparison.Ordinal);
        Assert.Contains(
            "flux_events_dlq_total{code=\"HTTP_404\",consumer=\"facturacion-api__x\"," +
            "reason=\"permanent\",subject=\"pedidos.pedido.v1.creado\"} 1",
            salida,
            StringComparison.Ordinal);
        Assert.Contains(
            "flux_events_retried_total{attempt=\"3\",consumer=\"facturacion-api__x\"," +
            "subject=\"pedidos.pedido.v1.creado\"} 1",
            salida,
            StringComparison.Ordinal);
        Assert.Contains(
            "flux_consumer_pending{consumer=\"facturacion-api__x\"," +
            "subject=\"pedidos.pedido.v1.creado\"} 42",
            salida,
            StringComparison.Ordinal);
        Assert.Contains("flux_connection_state 1", salida, StringComparison.Ordinal);
        Assert.Contains("flux_event_handler_duration_seconds_bucket{", salida, StringComparison.Ordinal);
    }

    [Fact]
    public void NingunaEtiquetaEsTenantIdNiIdNiCorrelationId()
    {
        // §2.2: un tenant nuevo NO debe crear series temporales nuevas. El fallo no avisa
        // —funciona con tres tenants en desarrollo y muere con diez mil en producción— y se
        // manifiesta como "Prometheus se ha quedado sin memoria", no como "alguien etiquetó
        // por tenant". Por eso IMetricsSink no tiene un diccionario genérico de etiquetas:
        // no hay por dónde colarlo.
        var salida = TodasLasMetricas().Render();

        foreach (var prohibida in new[] { "tenantid=", "id=", "correlationid=" })
        {
            Assert.DoesNotContain(prohibida, salida, StringComparison.Ordinal);
        }
    }

    // ─── recolector ──────────────────────────────────────────────────────────

    [Fact]
    public void CuentaPublicacionesPorSubjectYResultado()
    {
        var m = new InMemoryMetrics();
        m.EventPublished("pedidos.pedido.v1.creado", PublishOutcome.Ok);
        m.EventPublished("pedidos.pedido.v1.creado", PublishOutcome.Ok);
        m.EventPublished("pedidos.pedido.v1.creado", PublishOutcome.Error);

        var counters = m.Counters();
        Assert.Equal(
            2L,
            counters["flux_events_published_total{outcome=\"ok\",subject=\"pedidos.pedido.v1.creado\"}"]);
        Assert.Equal(
            1L,
            counters["flux_events_published_total{outcome=\"error\",subject=\"pedidos.pedido.v1.creado\"}"]);
    }

    [Fact]
    public void LasEtiquetasSeOrdenanParaQueLaClaveSeaEstable()
    {
        // Sin orden estable, la MISMA serie temporal aparecería con dos claves según el
        // orden en que se construyeran las etiquetas, y el contador se repartiría entre
        // ambas sin que nada avisara.
        var a = new InMemoryMetrics();
        var b = new InMemoryMetrics();
        a.EventDlq("s", "c", DlqReason.Permanent, "X");
        b.EventDlq("s", "c", DlqReason.Permanent, "X");

        Assert.Equal(a.Counters().Keys, b.Counters().Keys);
    }

    [Fact]
    public void ElHistogramaAcumulaEnTodosLosBucketsQueSuperanElValor()
    {
        var m = new InMemoryMetrics();
        m.HandlerDuration("s", "c", 0.03); // cae por encima de 0.025
        var salida = m.Render();

        Assert.Contains("le=\"0.025\"} 0", salida, StringComparison.Ordinal);
        Assert.Contains("le=\"0.05\"} 1", salida, StringComparison.Ordinal);
        Assert.Contains("le=\"+Inf\"} 1", salida, StringComparison.Ordinal);
        Assert.Contains("_count{consumer=\"c\",subject=\"s\"} 1", salida, StringComparison.Ordinal);
    }

    [Fact]
    public void LosBucketsSeEmitenSinCerosFinalesComoEnNode()
    {
        // `le="30.0"` y `le="30"` son etiquetas DISTINTAS: al agregar el histograma de un
        // servicio .NET con el de uno de Node saldrían dos series donde hay una.
        var m = new InMemoryMetrics();
        m.HandlerDuration("s", "c", 0.4);
        var salida = m.Render();

        Assert.Contains("le=\"30\"}", salida, StringComparison.Ordinal);
        Assert.Contains("le=\"1\"}", salida, StringComparison.Ordinal);
        Assert.DoesNotContain("le=\"30.0\"}", salida, StringComparison.Ordinal);
        Assert.DoesNotContain("le=\"1.0\"}", salida, StringComparison.Ordinal);
    }

    [Fact]
    public void UnGaugeSinEtiquetasNoDejaLlavesVaciasEnLaSalida()
    {
        var m = new InMemoryMetrics();
        m.ConnectionState(Flux.ConnectionState.Connected);

        Assert.Contains("\nflux_connection_state 1\n", m.Render(), StringComparison.Ordinal);
        Assert.DoesNotContain("flux_connection_state{}", m.Render(), StringComparison.Ordinal);
    }

    [Fact]
    public void TodasLasLineasTienenFormaValidaDePrometheus()
    {
        foreach (var linea in TodasLasMetricas().Render().Split('\n'))
        {
            if (linea.Length == 0 || linea.StartsWith('#'))
            {
                continue;
            }

            Assert.True(LineaPrometheus.IsMatch(linea), $"línea no válida para Prometheus: {linea}");
        }
    }

    [Fact]
    public void EscapaLasComillasDeLosValoresDeEtiqueta()
    {
        // Un `code` con comillas rompe el formato y Prometheus descarta el SCRAPE ENTERO, no
        // solo esa línea: un mensaje de error mal formado de un servicio apagaría las
        // métricas de todo el proceso.
        var m = new InMemoryMetrics();
        m.EventDlq("s", "c", DlqReason.Permanent, "con \"comillas\" y \\barra");

        foreach (var linea in m.Render().Split('\n'))
        {
            if (!linea.StartsWith("flux_events_dlq_total", StringComparison.Ordinal))
            {
                continue;
            }

            Assert.Equal(0, linea.Count(c => c == '"') % 2);
            Assert.DoesNotContain("\\", linea, StringComparison.Ordinal);
        }
    }

    // ─── outcome de los fallos de firma ──────────────────────────────────────

    [Fact]
    public void LosTresCodigosDeFirmaProducenOutcomeInvalidSignature()
    {
        // La firma inválida se separa del POISON común aunque su `reason` siga siendo
        // `poison`: son dos incidentes distintos —basura frente a suplantación— con dos
        // respuestas distintas, y §2.1 declara la etiqueta justo para eso. Es además lo que
        // hacen Go, Rust y PHP, así que un panel del ecosistema espera este valor.
        foreach (var code in new[]
                 {
                     EventSigning.MissingSignature,
                     EventSigning.InvalidSignature,
                     EventSigning.UnknownSigningKey,
                 })
        {
            Assert.Equal(
                ConsumeOutcome.InvalidSignature,
                MetricLabels.ConsumeOutcomeFor(DlqReason.Poison, code));
        }
    }

    [Fact]
    public void ElReasonDeLaDlqNoCambiaConUnFalloDeFirma()
    {
        // El outcome tiene seis valores; dlqreason tiene tres y es el enum CERRADO del
        // envelope (04-errors.md §1). Mezclarlos metería "invalid_signature" en el atributo
        // `dlqreason` de un evento, que ningún otro SDK sabría leer.
        var m = new InMemoryMetrics();
        m.EventDlq("s", "c", DlqReason.Poison, EventSigning.InvalidSignature);

        Assert.Contains("reason=\"poison\"", m.Render(), StringComparison.Ordinal);
        Assert.DoesNotContain("reason=\"invalid_signature\"", m.Render(), StringComparison.Ordinal);
    }

    [Fact]
    public void CualquierOtroCodigoConservaElOutcomeDeSuReason()
    {
        Assert.Equal(
            ConsumeOutcome.Poison,
            MetricLabels.ConsumeOutcomeFor(DlqReason.Poison, "MALFORMED_JSON"));
        Assert.Equal(
            ConsumeOutcome.Permanent,
            MetricLabels.ConsumeOutcomeFor(DlqReason.Permanent, "HTTP_404"));
        Assert.Equal(
            ConsumeOutcome.Retryable,
            MetricLabels.ConsumeOutcomeFor(DlqReason.Retryable, "ECONNRESET"));
        Assert.Equal(
            ConsumeOutcome.Poison,
            MetricLabels.ConsumeOutcomeFor(DlqReason.Poison, null));
    }

    // ─── default ─────────────────────────────────────────────────────────────

    [Fact]
    public void NoMetricsNoLanzaYNoGuardaNada()
    {
        // Un SDK de protocolo no debe imponer un backend de métricas a quien solo quiere
        // publicar un evento.
        var sink = (IMetricsSink)NoMetrics.Instance;
        sink.EventPublished("s", PublishOutcome.Ok);
        sink.EventConsumed("s", "c", ConsumeOutcome.Ok);
        sink.HandlerDuration("s", "c", 1.0);
        sink.EventDlq("s", "c", DlqReason.Poison, "X");
        sink.EventRetried("s", "c", 1);
        sink.ConsumerPending("s", "c", 0);
        sink.ConnectionState(Flux.ConnectionState.Disconnected);
    }

    [Fact]
    public void LosLiteralesDeConnectionStateSonLosDeLaSpec()
    {
        // §2.1: 1 conectado, 0 desconectado, 2 reconectando. Son los valores que leen las
        // alertas, así que el enum los fija explícitamente en vez de heredar el orden de
        // declaración.
        Assert.Equal(0, (int)Flux.ConnectionState.Disconnected);
        Assert.Equal(1, (int)Flux.ConnectionState.Connected);
        Assert.Equal(2, (int)Flux.ConnectionState.Reconnecting);
    }

    private static InMemoryMetrics TodasLasMetricas()
    {
        const string subject = "pedidos.pedido.v1.creado";
        const string consumer = "facturacion-api__x";

        var m = new InMemoryMetrics();
        m.EventPublished(subject, PublishOutcome.Ok);
        m.EventConsumed(subject, consumer, ConsumeOutcome.Ok);
        m.EventDlq(subject, consumer, DlqReason.Permanent, "HTTP_404");
        m.EventRetried(subject, consumer, 3);
        m.ConsumerPending(subject, consumer, 42);
        m.ConnectionState(Flux.ConnectionState.Connected);
        m.HandlerDuration(subject, consumer, 0.4);
        return m;
    }
}
