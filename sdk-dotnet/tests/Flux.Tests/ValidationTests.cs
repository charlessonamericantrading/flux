// Validación L3 contra el JSON Schema del evento — 00-protocol.md §5.
//
// Los casos son los mismos que fija sdk-node/test/validation.test.ts, que es la
// implementación de referencia. Que un payload sea rechazado en Node y aceptado en .NET
// sería peor que no validar: convertiría el nivel de conformidad en una propiedad del
// lenguaje del productor.

using System.Text.Json;
using Xunit;

namespace Flux.Tests;

public class ValidationTests
{
    private const string Subject = "pedidos.pedido.v1.creado";

    private static readonly SchemaBundle Bundle = SchemaBundle.FromFile(
        Path.Combine(RepoRoot(), "schemas", "bundle.json"));

    private static readonly string Uri = Bundle.SchemaUriFor(Subject)!;

    /// <summary>
    /// La raíz se busca por un marcador y no contando <c>..</c>: los tests corren desde
    /// <c>bin/Debug/net8.0</c>, y cualquier ruta relativa fija sería correcta ahí y
    /// silenciosamente incorrecta desde un IDE o desde la raíz del repositorio.
    /// </summary>
    private static string RepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "protocol.json")))
            {
                return dir.FullName;
            }

            dir = dir.Parent;
        }

        throw new InvalidOperationException("no se encontró la raíz del repo (protocol.json)");
    }

    /// <summary>El payload de ejemplo de AGENTS.md §2, que cumple el esquema.</summary>
    private static object Valido() => new
    {
        pedidoId = "ped-123",
        clienteId = "cli-987",
        aggregateVersion = 1,
        totalCents = 9990,
        moneda = "EUR",
        lineas = new[] { new { sku = "ABC-1", cantidad = 2, precioUnitarioCents = 4995 } },
    };

    private static FluxEvent Evento(object data, string? dataSchema = null) =>
        Envelope.BuildEvent(new Envelope.BuildEventInput
        {
            Subject = Subject,
            Data = data,
            Id = "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
            Source = "/produccion/pedidos-api",
            ProducerVersion = "3.4.1",
            TenantId = "acme",
            DataClassification = DataClassification.Internal,
            DataSchema = dataSchema ?? Uri,
            CorrelationId = "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
        });

    private static IEventValidator Validador(ValidationMode mode, IFluxLogger? logger = null) =>
        SchemaValidator.Create(new ValidationOptions
        {
            Mode = mode,
            Bundle = Bundle,
            Logger = logger,
        })!;

    /// <summary>Recoge los avisos del modo <see cref="ValidationMode.Warn"/>.</summary>
    private sealed class LoggerDePrueba : IFluxLogger
    {
        public List<string> Avisos { get; } = new();

        public List<string> Errores { get; } = new();

        public void Warn(string message) => Avisos.Add(message);

        public void Error(string message) => Errores.Add(message);
    }

    // ─── El bundle ───────────────────────────────────────────────────────────

    [Fact]
    public void ElBundleIndexaElSubjectHaciaSuUri()
    {
        Assert.NotNull(Uri);
        Assert.Matches(
            @"^https://schemas\.internal/pedidos/pedido/creado/\d+\.\d+\.\d+\.json$", Uri);
    }

    [Fact]
    public void ElIdDelEsquemaCoincideConLaClaveDelBundle()
    {
        using var documento = JsonDocument.Parse(Bundle.Schemas[Uri]);

        Assert.Equal(Uri, documento.RootElement.GetProperty("$id").GetString());
    }

    [Fact]
    public void UnBundleQueNoEsJsonFallaConUnMensajeAccionable()
    {
        var e = Assert.Throws<ArgumentException>(() => SchemaBundle.FromJson("{no es json"));

        Assert.Contains("bundle-schemas.mjs", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void UnBundleSinEsquemasNoSeAcepta()
    {
        // Con Mode = Strict haría que TODO evento fallara con SchemaNotFoundException, y el
        // operador buscaría el problema en el evento en vez de en el fichero que no se generó.
        Assert.Throws<ArgumentException>(
            () => SchemaBundle.FromJson("""{"subjects":{},"schemas":{}}"""));
    }

    // ─── STRICT ──────────────────────────────────────────────────────────────

    [Fact]
    public void UnPayloadValidoPasa()
    {
        var validador = Validador(ValidationMode.Strict);

        validador.Check(Evento(Valido()), Subject);
    }

    [Fact]
    public void FaltaUnCampoRequeridoLanza()
    {
        var validador = Validador(ValidationMode.Strict);
        var sinTotal = new
        {
            pedidoId = "ped-123",
            clienteId = "cli-987",
            aggregateVersion = 1,
            moneda = "EUR",
            lineas = new[] { new { sku = "ABC-1", cantidad = 2, precioUnitarioCents = 4995 } },
        };

        Assert.Throws<SchemaValidationException>(() => validador.Check(Evento(sinTotal), Subject));
    }

    [Fact]
    public void UnTipoIncorrectoLanza()
    {
        // El caso que la spec llama el más peligroso: "9990" en vez de 9990. El importe
        // sigue "estando", y un consumidor descuidado lo concatena en vez de sumarlo.
        var validador = Validador(ValidationMode.Strict);
        var data = new
        {
            pedidoId = "ped-123",
            clienteId = "cli-987",
            aggregateVersion = 1,
            totalCents = "9990",
            moneda = "EUR",
            lineas = new[] { new { sku = "ABC-1", cantidad = 2, precioUnitarioCents = 4995 } },
        };

        Assert.Throws<SchemaValidationException>(() => validador.Check(Evento(data), Subject));
    }

    [Fact]
    public void UnCampoDesconocidoLanza()
    {
        // additionalProperties: false. Un campo mal escrito debe fallar, no colarse en
        // silencio: `totalCemts` sin esta regla se publicaría y el consumidor leería 0.
        var validador = Validador(ValidationMode.Strict);
        var data = new
        {
            pedidoId = "ped-123",
            clienteId = "cli-987",
            aggregateVersion = 1,
            totalCents = 9990,
            totalCemts = 9990,
            moneda = "EUR",
            lineas = new[] { new { sku = "ABC-1", cantidad = 2, precioUnitarioCents = 4995 } },
        };

        Assert.Throws<SchemaValidationException>(() => validador.Check(Evento(data), Subject));
    }

    [Fact]
    public void UnPatronIncumplidoLanza()
    {
        var validador = Validador(ValidationMode.Strict);
        var data = new
        {
            pedidoId = "ped-123",
            clienteId = "cli-987",
            aggregateVersion = 1,
            totalCents = 9990,
            moneda = "euros",
            lineas = new[] { new { sku = "ABC-1", cantidad = 2, precioUnitarioCents = 4995 } },
        };

        Assert.Throws<SchemaValidationException>(() => validador.Check(Evento(data), Subject));
    }

    [Fact]
    public void ReportaTodosLosErroresNoSoloElPrimero()
    {
        // Requisito explícito de L3 (00-protocol.md §5): de uno en uno, arreglar un payload
        // con tres campos mal cuesta tres despliegues.
        var validador = Validador(ValidationMode.Strict);
        var data = new
        {
            pedidoId = "ped-123",
            clienteId = "cli-987",
            aggregateVersion = 1,
            totalCents = "x",
            moneda = "euros",
            cantidad = 1,
            lineas = new[] { new { sku = "ABC-1", cantidad = 2, precioUnitarioCents = 4995 } },
        };

        var e = Assert.Throws<SchemaValidationException>(() => validador.Check(Evento(data), Subject));

        Assert.True(e.Errors.Count >= 2, $"esperaba >= 2 errores, hubo {e.Errors.Count}");
        Assert.Equal(Subject, e.Subject);
        Assert.Equal(Uri, e.DataSchema);
    }

    [Fact]
    public void UnEsquemaAusenteDelBundleLanzaSchemaNotFound()
    {
        var validador = Validador(ValidationMode.Strict);

        var e = Assert.Throws<SchemaNotFoundException>(() => validador.Check(
            Evento(Valido(), "https://schemas.internal/no/existe/1.0.0.json"), Subject));

        Assert.Contains("bundle-schemas.mjs", e.Message, StringComparison.Ordinal);
    }

    // ─── WARN y OFF ──────────────────────────────────────────────────────────

    [Fact]
    public void WarnRegistraPeroNoLanza()
    {
        var logger = new LoggerDePrueba();
        var validador = Validador(ValidationMode.Warn, logger);
        var data = new
        {
            pedidoId = "ped-123",
            clienteId = "cli-987",
            aggregateVersion = 1,
            totalCents = "x",
            moneda = "EUR",
            lineas = new[] { new { sku = "ABC-1", cantidad = 2, precioUnitarioCents = 4995 } },
        };

        validador.Check(Evento(data), Subject);

        Assert.Single(logger.Avisos);
        Assert.Contains("no cumple su esquema", logger.Avisos[0], StringComparison.Ordinal);
    }

    [Fact]
    public void WarnTambienAvisaSinLanzarCuandoFaltaElEsquema()
    {
        var logger = new LoggerDePrueba();
        var validador = Validador(ValidationMode.Warn, logger);

        validador.Check(Evento(Valido(), "https://schemas.internal/no/existe/1.0.0.json"), Subject);

        Assert.Single(logger.Avisos);
    }

    [Fact]
    public void OffNoCompilaNada()
    {
        // L2 no paga el coste de L3: sin modo, no hay validador que construir.
        Assert.Null(SchemaValidator.Create(new ValidationOptions()));
        Assert.Null(SchemaValidator.Create(new ValidationOptions { Mode = ValidationMode.Off }));
    }

    [Fact]
    public void StrictSinBundleFallaConUnMensajeAccionable()
    {
        var e = Assert.Throws<ArgumentException>(
            () => SchemaValidator.Create(new ValidationOptions { Mode = ValidationMode.Strict }));

        Assert.Contains("bundle-schemas.mjs", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WithSchemaValidatorDejaLasOpcionesListas()
    {
        var options = new ValidationOptions
        {
            Mode = ValidationMode.Strict,
            Bundle = Bundle,
            OnConsume = true,
        }.WithSchemaValidator();

        Assert.NotNull(options.Validator);
        Assert.True(options.OnConsume);
        Assert.Equal(ValidationMode.Strict, options.Mode);
    }

    // ─── Al consumir ─────────────────────────────────────────────────────────

    [Fact]
    public void UnFalloDeEsquemaSeClasificaPermanentNoRetryable()
    {
        // El evento es sintácticamente correcto —ha llegado a parsearse, así que no es
        // POISON— pero incumple su contrato: reintentarlo seis veces da exactamente el mismo
        // resultado y bloquea la cola 51 minutos para nada — 04-errors.md §1.2.
        var c = new Classifier().Classify(
            new SchemaValidationException(Subject, Uri, new[] { "$.totalCents: …" }));

        Assert.Equal(ErrorClass.Permanent, c.Class);
        Assert.Equal(SchemaValidationException.CodeValue, c.Code);
    }

    [Fact]
    public void UnEsquemaAusenteTambienEsPermanent()
    {
        var c = new Classifier().Classify(new SchemaNotFoundException(Subject, Uri));

        Assert.Equal(ErrorClass.Permanent, c.Class);
        Assert.Equal(SchemaNotFoundException.CodeValue, c.Code);
    }

    [Fact]
    public void LaMetricaLoEtiquetaInvalidSchemaNoPermanent()
    {
        // "un productor incumple su esquema" y "mi lógica rechaza este evento" son dos
        // preguntas distintas con dos respuestas distintas — 08-observability.md §2.1. El
        // dlqreason sigue siendo `permanent`: ése es el enum cerrado de 04-errors.md §1.
        Assert.Equal(
            ConsumeOutcome.InvalidSchema,
            MetricLabels.ConsumeOutcomeFor(DlqReason.Permanent, SchemaValidationException.CodeValue));

        Assert.Equal(
            ConsumeOutcome.InvalidSchema,
            MetricLabels.ConsumeOutcomeFor(DlqReason.Permanent, SchemaNotFoundException.CodeValue));

        Assert.Equal(
            ConsumeOutcome.Permanent,
            MetricLabels.ConsumeOutcomeFor(DlqReason.Permanent, "PEDIDO_YA_CANCELADO"));
    }

    [Fact]
    public void ElMensajeEnumeraTodosLosFallosUnoPorLinea()
    {
        var e = new SchemaValidationException(Subject, Uri, new[]
        {
            "$.totalCents [type] string found, integer expected",
            "$.moneda [pattern] does not match",
        });

        Assert.Contains(Subject, e.Message, StringComparison.Ordinal);
        Assert.Contains(Uri, e.Message, StringComparison.Ordinal);
        Assert.Equal(2, e.Message.Split("\n  · ").Length - 1);
    }

    // ─── Sondeo de flux_consumer_pending — 08-observability.md §2.3 ───────────

    [Fact]
    public void ElSondeoDePendingSeConfiguraYSePuedeDesactivar()
    {
        Assert.Equal(TimeSpan.FromSeconds(15), FluxBus.DefaultPendingPollInterval);

        var porDefecto = new ConnectOptions
        {
            Servers = "nats://localhost:4222",
            Service = "pedidos-api",
            Environment = "produccion",
            Version = "1.0.0",
        };

        Assert.Equal(FluxBus.DefaultPendingPollInterval, porDefecto.PendingPollInterval);
        Assert.True(FluxBus.PendingPollEnabled(porDefecto.PendingPollInterval, new InMemoryMetrics()));

        // `0` lo desactiva — 08-observability.md §2.3 lo permite explícitamente.
        Assert.False(FluxBus.PendingPollEnabled(TimeSpan.Zero, new InMemoryMetrics()));

        // Sin sink no hay dónde escribir el gauge: no se crea la tarea.
        Assert.False(FluxBus.PendingPollEnabled(TimeSpan.FromSeconds(15), NoMetrics.Instance));
    }

    [Fact]
    public void LaValidacionEstaApagadaPorDefecto()
    {
        // Sin configurarla, el SDK se comporta exactamente como en L2: ni compila esquemas
        // ni exige el paquete Flux.Validation.
        var options = new ConnectOptions
        {
            Servers = "nats://localhost:4222",
            Service = "pedidos-api",
            Environment = "produccion",
            Version = "1.0.0",
        };

        Assert.Null(options.Validation);
    }
}
