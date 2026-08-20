// Envelope CloudEvents 1.0 — 01-envelope.md.

using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Xunit;

namespace Flux.Tests;

public class EnvelopeTests
{
    /// <summary>
    /// El fixture canónico de <c>conformance/cases/cross-sdk-envelope.json</c>. Todo SDK
    /// DEBE producir exactamente el mismo JSON a partir de él.
    /// </summary>
    private const long FixtureMillis = 1755685539410L;

    private const string FixtureId = "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55";

    private static Envelope.BuildEventInput FixtureInput() => new()
    {
        Subject = "pedidos.pedido.v1.creado",
        Data = new { pedidoId = "ped-123", aggregateVersion = 1, totalCents = 9990, moneda = "EUR" },
        Id = FixtureId,
        Source = "/produccion/pedidos-api",
        ProducerVersion = "3.4.1",
        TenantId = "acme",
        DataClassification = DataClassification.Confidential,
        DataSchema = "https://schemas.internal/pedidos/pedido/creado/1.0.0.json",
        CorrelationId = FixtureId,
        Time = DateTimeOffset.FromUnixTimeMilliseconds(FixtureMillis),
        AggregateId = "ped-123",
    };

    private static string Json(FluxEvent evento) => Encoding.UTF8.GetString(Envelope.Serialize(evento));

    // ─── time ────────────────────────────────────────────────────────────────

    [Fact]
    public void FormatTimeSiempreTresDecimalesYSufijoZ()
    {
        // 01-envelope.md §2.2: exactamente 3 decimales. Los ceros finales NO se recortan
        // (el fallo de Go con RFC3339Nano) y no hay microsegundos ni offset (el de Python).
        Assert.Equal(
            "2025-08-20T10:25:39.410Z",
            Envelope.FormatTime(DateTimeOffset.FromUnixTimeMilliseconds(FixtureMillis)));

        Assert.Equal(
            "2025-08-20T10:25:39.400Z",
            Envelope.FormatTime(DateTimeOffset.FromUnixTimeMilliseconds(FixtureMillis - 10)));

        Assert.Equal(
            "2025-08-20T10:25:39.000Z",
            Envelope.FormatTime(DateTimeOffset.FromUnixTimeMilliseconds(FixtureMillis - 410)));
    }

    [Fact]
    public void FormatTimeTruncaLaParteSubmilisegundo()
    {
        // .NET guarda ticks de 100 ns. `.fff` TRUNCA, no redondea — que es lo que hacen los
        // otros cuatro SDKs. Si redondease, un evento a .4109999 saldría como .411 en .NET y
        // .410 en Node, y el envelope dejaría de ser reproducible.
        var casi = DateTimeOffset.FromUnixTimeMilliseconds(FixtureMillis).AddTicks(9_999);

        Assert.Equal("2025-08-20T10:25:39.410Z", Envelope.FormatTime(casi));
    }

    [Fact]
    public void FormatTimeNormalizaAUtc()
    {
        var conOffset = new DateTimeOffset(2025, 8, 20, 12, 25, 39, 410, TimeSpan.FromHours(2));

        Assert.Equal("2025-08-20T10:25:39.410Z", Envelope.FormatTime(conOffset));
    }

    [Fact]
    public void ElFormateadorPorDefectoDeDotnetNoValdria()
    {
        // Deja constancia del motivo por el que `time` es string y no DateTimeOffset:
        // ToString("O") emite 7 decimales, que es RFC 3339 válido pero no byte a byte igual
        // al de los demás SDKs — 01-envelope.md §2.2.
        var instante = DateTimeOffset.FromUnixTimeMilliseconds(FixtureMillis);

        Assert.NotEqual(Envelope.FormatTime(instante), instante.ToString("O"));
    }

    // ─── construcción ────────────────────────────────────────────────────────

    [Fact]
    public void BuildEventRellenaLoDerivable()
    {
        var evento = Envelope.BuildEvent(FixtureInput());

        Assert.Equal("1.0", evento.SpecVersion);
        Assert.Equal("com.flux.pedidos.pedido.creado.v1", evento.Type);
        Assert.Equal("application/json", evento.DataContentType);
        Assert.Equal("2025-08-20T10:25:39.410Z", evento.Time);

        // El aggregateId va al atributo `subject` de CloudEvents — 01-envelope.md §2.1.
        Assert.Equal("ped-123", evento.AggregateId);

        // Por convención partitionkey = aggregateId — 01-envelope.md §3.2.
        Assert.Equal("ped-123", evento.PartitionKey);
    }

    [Fact]
    public void BuildEventExigeUnObjetoJsonEnLaRaizDeData()
    {
        foreach (object data in new object[] { new[] { 1, 2, 3 }, "texto", 42, true })
        {
            var e = Assert.Throws<EnvelopeException>(
                () => Envelope.BuildEvent(FixtureInput() with { Data = data }));

            Assert.Contains("objeto JSON en la raíz", e.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void BuildEventExigeLasExtensionesObligatorias()
    {
        // Una cadena vacía es un valor legal de string, así que el compilador no puede
        // distinguirla de "presente": la comprobación es en ejecución, igual que en Go.
        Assert.Throws<EnvelopeException>(() => Envelope.BuildEvent(FixtureInput() with { TenantId = "" }));
        Assert.Throws<EnvelopeException>(() => Envelope.BuildEvent(FixtureInput() with { CorrelationId = "" }));
        Assert.Throws<EnvelopeException>(() => Envelope.BuildEvent(FixtureInput() with { ProducerVersion = "" }));
        Assert.Throws<EnvelopeException>(() => Envelope.BuildEvent(FixtureInput() with { DataSchema = "" }));
    }

    [Fact]
    public void BuildEventRechazaUnSubjectInvalido()
    {
        Assert.Throws<InvalidSubjectException>(
            () => Envelope.BuildEvent(FixtureInput() with { Subject = "Pedidos.pedido.v1.creado" }));
    }

    // ─── serialización ───────────────────────────────────────────────────────

    [Fact]
    public void SerializeProduceElFixtureCanonicoByteAByte()
    {
        // conformance/cases/cross-sdk-envelope.json: Node, Python y Go producen exactamente
        // esto. Si este test se rompe, el envelope de .NET ha dejado de ser interoperable.
        const string esperado =
            """
            {"specversion":"1.0","id":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55","source":"/produccion/pedidos-api","type":"com.flux.pedidos.pedido.creado.v1","time":"2025-08-20T10:25:39.410Z","datacontenttype":"application/json","dataschema":"https://schemas.internal/pedidos/pedido/creado/1.0.0.json","subject":"ped-123","correlationid":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55","tenantid":"acme","producerversion":"3.4.1","dataclassification":"confidential","partitionkey":"ped-123","data":{"pedidoId":"ped-123","aggregateVersion":1,"totalCents":9990,"moneda":"EUR"}}
            """;

        Assert.Equal(esperado, Json(Envelope.BuildEvent(FixtureInput())));
    }

    [Fact]
    public void SerializeUsaLosNombresDeExtensionDeCloudEvents()
    {
        var json = Json(Envelope.BuildEvent(FixtureInput()));

        // CloudEvents restringe los nombres de extensión a [a-z0-9] sin separadores: es la
        // especificación, no una convención de estilo — 01-envelope.md §3.
        foreach (var atributo in new[]
                 {
                     "\"correlationid\":", "\"tenantid\":", "\"producerversion\":",
                     "\"dataclassification\":", "\"partitionkey\":",
                 })
        {
            Assert.Contains(atributo, json, StringComparison.Ordinal);
        }

        // Y NO usa los nombres de .NET.
        Assert.DoesNotContain("CorrelationId", json, StringComparison.Ordinal);
        Assert.DoesNotContain("AggregateId", json, StringComparison.Ordinal);
    }

    [Fact]
    public void SerializeNoEscapaNiHtmlNiAcentos()
    {
        // El encoder por defecto de System.Text.Json escaparía < > & y todo lo no-ASCII,
        // produciendo un JSON distinto byte a byte del de Node y Go para el mismo evento.
        var evento = Envelope.BuildEvent(FixtureInput() with
        {
            Data = new { nota = "<b>café & té</b>" },
        });

        var json = Json(evento);

        Assert.Contains("<b>café & té</b>", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\\u003C", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("\\u00E9", json, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SerializeOmiteLosOpcionalesAusentes()
    {
        // Ausente != vacío: un opcional que no aplica se OMITE, no viaja como "" — 01-envelope.md §3.3.
        var json = Json(Envelope.BuildEvent(FixtureInput() with { AggregateId = null }));

        Assert.DoesNotContain("\"subject\"", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"partitionkey\"", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"causationid\"", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"traceparent\"", json, StringComparison.Ordinal);
        Assert.DoesNotContain("null", json, StringComparison.Ordinal);
    }

    [Fact]
    public void UnDlqAttemptsDeCeroNoDesaparece()
    {
        // ESTE es el test que justifica `int?` en vez de `int`. Con `int` y una condición de
        // omisión por valor por defecto, el atributo se evaporaría y reaparecería como
        // "ausente" al parsear en otro SDK. Hoy el mínimo legal es 1, así que el bug sería
        // invisible — el envelope dependería de una coincidencia, no de una regla
        // — 01-envelope.md §3.3.
        var construido = Envelope.BuildEvent(FixtureInput());
        var evento = construido with { DlqAttempts = 0 };

        Assert.Contains("\"dlqattempts\":0", Json(evento), StringComparison.Ordinal);
    }

    [Fact]
    public void SerializeRechazaMasDeUnMiB()
    {
        var enorme = new { relleno = new string('x', Protocol.MaxMessageBytes) };

        var e = Assert.Throws<EnvelopeException>(
            () => Envelope.Serialize(Envelope.BuildEvent(FixtureInput() with { Data = enorme })));

        Assert.Contains("claim-check", e.Message, StringComparison.Ordinal);
    }

    // ─── parseo ──────────────────────────────────────────────────────────────

    [Fact]
    public void RoundTripParseSerializeEsIdentico()
    {
        var original = Envelope.BuildEvent(FixtureInput());
        var bytes = Envelope.Serialize(original);

        var parseado = Envelope.ParseEvent(bytes);

        Assert.Equal(original, parseado);
        Assert.Equal(bytes, Envelope.Serialize(parseado));
    }

    [Fact]
    public void ParseEventPreservaElPayloadSinInterpretar()
    {
        // Un entero mayor que 2^53 sobreviviría a `double`. Se guarda como JsonElement justo
        // para que el replay desde la DLQ sea verbatim.
        const string cuerpo =
            """
            {"specversion":"1.0","id":"i","source":"/e/s","type":"com.flux.pedidos.pedido.creado.v1","time":"2025-08-20T10:25:39.410Z","datacontenttype":"application/json","dataschema":"https://s/x.json","correlationid":"i","tenantid":"t","producerversion":"1.0.0","dataclassification":"internal","data":{"grande":9007199254740993,"orden":1,"antes":2}}
            """;

        var evento = Envelope.ParseEvent(Encoding.UTF8.GetBytes(cuerpo));

        Assert.Equal("9007199254740993", evento.Data.GetProperty("grande").GetRawText());

        // El orden de las claves del payload se conserva.
        Assert.Equal(
            new[] { "grande", "orden", "antes" },
            evento.Data.EnumerateObject().Select(p => p.Name).ToArray());
    }

    [Fact]
    public void DataAsTipaElPayload()
    {
        var evento = Envelope.BuildEvent(FixtureInput());

        var pedido = evento.DataAs<PedidoCreado>();

        Assert.Equal("ped-123", pedido.PedidoId);
        Assert.Equal(9990, pedido.TotalCents);
    }

    [Fact]
    public void DataAsQueNoEncajaEsPermanentNoPoison()
    {
        // El envelope era correcto y el mensaje llegó al handler: es un desajuste de
        // contrato entre productor y consumidor, es decir PERMANENT — 04-errors.md §1.2.
        var evento = Envelope.BuildEvent(FixtureInput() with { Data = new { totalCents = "no soy un número" } });

        var e = Assert.Throws<PermanentException>(() => evento.DataAs<PedidoCreado>());

        Assert.Equal("DATA_SCHEMA_MISMATCH", e.FluxCode);
        Assert.Equal(ErrorClass.Permanent, e.FluxClass);
    }

    [Theory]
    [InlineData("{no soy json", "MALFORMED_JSON")]
    [InlineData("[1,2,3]", "NOT_AN_OBJECT")]
    [InlineData("""{"specversion":"0.3"}""", "UNSUPPORTED_SPECVERSION")]
    [InlineData("""{"specversion":"1.0"}""", "MISSING_REQUIRED_ATTRIBUTE")]
    public void ParseEventDetectaPoison(string cuerpo, string codigo)
    {
        var e = Assert.Throws<PoisonException>(() => Envelope.ParseEvent(Encoding.UTF8.GetBytes(cuerpo)));

        Assert.Equal(codigo, e.FluxCode);
        Assert.Equal(ErrorClass.Poison, e.FluxClass);
    }

    [Fact]
    public void ParseEventRechazaUnContentTypeNoSoportado()
    {
        var cuerpo = Valido().Replace(
            "\"datacontenttype\":\"application/json\"",
            "\"datacontenttype\":\"application/xml\"",
            StringComparison.Ordinal);

        var e = Assert.Throws<PoisonException>(() => Envelope.ParseEvent(Encoding.UTF8.GetBytes(cuerpo)));

        Assert.Equal("UNSUPPORTED_CONTENT_TYPE", e.FluxCode);
    }

    [Fact]
    public void ParseEventRechazaAtributosRaizDesconocidos()
    {
        // Lista CERRADA: las extensiones de CloudEvents solo admiten escalares, y colgar
        // metadatos de la raíz acaba en JSON dentro de un string — 01-envelope.md §3.3.
        var cuerpo = Valido().Replace(
            "\"data\":", "\"metadatos\":{\"a\":1},\"otro\":2,\"data\":", StringComparison.Ordinal);

        var e = Assert.Throws<PoisonException>(() => Envelope.ParseEvent(Encoding.UTF8.GetBytes(cuerpo)));

        Assert.Equal("UNKNOWN_ROOT_ATTRIBUTE", e.FluxCode);

        // Se enumeran TODOS los desconocidos, en orden estable, no solo el primero.
        Assert.Contains("metadatos, otro", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void LaComparacionDeNombresDeAtributoEsCaseSensitive()
    {
        // 01-envelope.md §2.3: {"ID": …} NO es `id`. Es un atributo raíz desconocido y por
        // tanto POISON. Es el mismo fantasma que la spec combate en los subjects de NATS, y
        // el que `encoding/json` de Go tiene por defecto.
        var cuerpo = Valido().Replace("\"data\":", "\"ID\":\"otro\",\"data\":", StringComparison.Ordinal);

        var e = Assert.Throws<PoisonException>(() => Envelope.ParseEvent(Encoding.UTF8.GetBytes(cuerpo)));

        Assert.Equal("UNKNOWN_ROOT_ATTRIBUTE", e.FluxCode);
        Assert.Contains("ID", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void LasOpcionesDeJsonSonCaseSensitivePorContrato()
    {
        // Es el default de JsonSerializerDefaults.General, pero se afirma aquí porque
        // JsonSerializerDefaults.Web —lo que inyecta ASP.NET Core— lo pone en true y
        // resucitaría el atributo fantasma.
        Assert.False(Envelope.JsonOptions.PropertyNameCaseInsensitive);
        Assert.Equal(JsonIgnoreCondition.WhenWritingNull, Envelope.JsonOptions.DefaultIgnoreCondition);
        Assert.Equal(JsonNumberHandling.Strict, Envelope.JsonOptions.NumberHandling);
    }

    [Fact]
    public void ParseEventRechazaUnAtributoConTipoIncompatible()
    {
        var cuerpo = Valido().Replace("\"data\":", "\"dlqattempts\":\"seis\",\"data\":", StringComparison.Ordinal);

        var e = Assert.Throws<PoisonException>(() => Envelope.ParseEvent(Encoding.UTF8.GetBytes(cuerpo)));

        Assert.Equal("INVALID_ATTRIBUTE_TYPE", e.FluxCode);
    }

    [Fact]
    public void ParseEventRechazaUnaClasificacionFueraDelEnum()
    {
        var cuerpo = Valido().Replace(
            "\"dataclassification\":\"internal\"",
            "\"dataclassification\":\"Confidential\"",
            StringComparison.Ordinal);

        // "Confidential" con mayúscula no es un valor del protocolo: la comparación es
        // case-sensitive — 01-envelope.md §2.3.
        Assert.Throws<PoisonException>(() => Envelope.ParseEvent(Encoding.UTF8.GetBytes(cuerpo)));
    }

    // ─── DLQ ─────────────────────────────────────────────────────────────────

    [Fact]
    public void ToDlqEventConservaElOriginalIntegro()
    {
        var original = Envelope.BuildEvent(FixtureInput());

        var enDlq = Envelope.ToDlqEvent(
            original, new Envelope.DlqInfo(DlqReason.Permanent, 1, "facturacion-api__x", "PEDIDO_YA_CANCELADO"));

        // Sin wrapper: el CloudEvent original íntegro más las extensiones dlq*
        // — 04-errors.md §3.
        Assert.Equal(original.Id, enDlq.Id);
        Assert.Equal(original.Time, enDlq.Time);
        Assert.Equal(original.Data.GetRawText(), enDlq.Data.GetRawText());
        Assert.Equal(DlqReason.Permanent, enDlq.DlqReason);
        Assert.Equal(1, enDlq.DlqAttempts);
        Assert.Matches(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$", enDlq.DlqTime!);

        // El original no se muta.
        Assert.Null(original.DlqReason);
    }

    [Fact]
    public void StripDlqExtensionsDevuelveElEventoReproducible()
    {
        var original = Envelope.BuildEvent(FixtureInput());
        var enDlq = Envelope.ToDlqEvent(
            original, new Envelope.DlqInfo(DlqReason.Retryable, 6, "c", "boom"));

        var reproducible = Envelope.StripDlqExtensions(enDlq);

        // El `id` original se CONSERVA: regenerarlo rompe la idempotencia de todos los
        // consumidores aguas abajo — 04-errors.md §4.1.
        Assert.Equal(original.Id, reproducible.Id);
        Assert.Equal(original, reproducible);
        Assert.Equal(Json(original), Json(reproducible));
    }

    [Fact]
    public void ElDlqErrorSeRecortaSinPartirCaracteres()
    {
        var largo = new string('á', 5_000);

        var enDlq = Envelope.ToDlqEvent(
            Envelope.BuildEvent(FixtureInput()), new Envelope.DlqInfo(DlqReason.Poison, 1, "c", largo));

        Assert.Equal(Envelope.MaxDlqErrorChars, enDlq.DlqError!.Length);

        // Un par sustituto no se parte por la mitad: partirlo produciría U+FFFD en el JSON.
        var emoji = string.Concat(Enumerable.Repeat("🚚", 1_000));
        Assert.True(Envelope.Truncate(emoji, 1_001).Length % 2 == 0);
    }

    // ─── igualdad ────────────────────────────────────────────────────────────

    [Fact]
    public void DosEventosConElMismoPayloadSonIguales()
    {
        // JsonElement es un struct cuya igualdad por defecto compara la referencia al
        // documento: sin el Equals escrito a mano, esto fallaría. Ver FluxEvent.Equals.
        var a = Envelope.BuildEvent(FixtureInput());
        var b = Envelope.BuildEvent(FixtureInput());

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, a with { AggregateId = "otro" });
        Assert.NotEqual(a, a with { Data = JsonSerializer.SerializeToElement(new { distinto = true }) });
    }

    // ─── utilidades ──────────────────────────────────────────────────────────

    private static string Valido() =>
        """
        {"specversion":"1.0","id":"i","source":"/e/s","type":"com.flux.pedidos.pedido.creado.v1","time":"2025-08-20T10:25:39.410Z","datacontenttype":"application/json","dataschema":"https://s/x.json","correlationid":"i","tenantid":"t","producerversion":"1.0.0","dataclassification":"internal","data":{"a":1}}
        """;

    private sealed record PedidoCreado
    {
        [JsonPropertyName("pedidoId")]
        public string PedidoId { get; init; } = "";

        [JsonPropertyName("totalCents")]
        public long TotalCents { get; init; }
    }
}
