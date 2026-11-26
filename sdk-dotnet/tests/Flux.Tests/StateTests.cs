// Estado replicado — lo que se comprueba SIN broker: la parte pura de 10-state.md.
//
// Lo que depende del broker (la secuencia esperada, el DIRECT.GET, el `watch` con su durable) lo cubren Integration/StateIntegrationTests.cs
// e Integration/StateSemanticsTests.cs. Aquí: los vectores de conformance/harness/vectors.json ejecutados EN PROCESO (los mismos que el
// arnés cruzado compara con Node y Rust, con su SHA-256 de la spec), el orden de validación de §3.6 y las reglas de clave, tenant y versión.

using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using Flux.Tests.Integration;
using Xunit;

namespace Flux.Tests;

public class StateTests
{
    private static readonly JsonArray Vectores = CargaVectores();

    private static JsonArray CargaVectores()
    {
        var ruta = Path.Combine(Prereq.RepoRoot, "conformance", "harness", "vectors.json");
        return JsonNode.Parse(File.ReadAllText(ruta))!["stateVectors"]!.AsArray();
    }

    public static IEnumerable<object[]> LosVectores() =>
        Vectores.Select(v => new object[] { v!["id"]!.GetValue<string>() });

    private static JsonElement Elemento(JsonNode? n)
    {
        using var doc = JsonDocument.Parse(n is null ? "null" : n.ToJsonString());
        return doc.RootElement.Clone();
    }

    private static string? Texto(JsonObject o, string nombre) => o[nombre]?.GetValue<string>();

    private static DataClassification? Clasificacion(string? t) =>
        t switch
        {
            null => null,
            "public" => DataClassification.Public,
            "internal" => DataClassification.Internal,
            "confidential" => DataClassification.Confidential,
            "restricted" => DataClassification.Restricted,
            _ => (DataClassification)(-1),
        };

    private static StateProtocol.EventInput Entrada(JsonObject e) =>
        new()
        {
            Contract = Texto(e, "contract")!,
            TenantId = Texto(e, "tenantId")!,
            Key = Texto(e, "key")!,
            Id = Texto(e, "id")!,
            Source = Texto(e, "source")!,
            Time = Texto(e, "time")!,
            CorrelationId = Texto(e, "correlationid")!,
            ProducerVersion = Texto(e, "producerversion")!,
            Classification = Clasificacion(Texto(e, "classification")),
            Data = Elemento(e["data"]),
            CausationId = Texto(e, "causationid"),
            TraceParent = Texto(e, "traceparent"),
            TraceState = Texto(e, "tracestate"),
        };

    /// <summary>Ejecuta un vector como el arnés: los bytes serializados o el subject; un rechazo lanza.</summary>
    private static (byte[]? Bytes, string? Subject) Ejecuta(string op, JsonObject i)
    {
        switch (op)
        {
            case "state-build":
                return (Envelope.Serialize(StateProtocol.BuildEvent(Entrada(i))), null);
            case "state-tombstone":
                return (Envelope.Serialize(StateProtocol.BuildTombstone(new StateProtocol.TombstoneInput
                {
                    Contract = Texto(i, "contract")!,
                    TenantId = Texto(i, "tenantId")!,
                    Key = Texto(i, "key")!,
                    Id = Texto(i, "id")!,
                    Source = Texto(i, "source")!,
                    Time = Texto(i, "time")!,
                    CorrelationId = Texto(i, "correlationid")!,
                    ProducerVersion = Texto(i, "producerversion")!,
                    AggregateVersion = Elemento(i["aggregateVersion"]),
                    CausationId = Texto(i, "causationid"),
                    TraceParent = Texto(i, "traceparent"),
                    TraceState = Texto(i, "tracestate"),
                })), null);
            case "state-subject":
                return (null, StateProtocol.Subject(Texto(i, "contract")!, Texto(i, "tenantId")!, Texto(i, "key")!));
            case "state-dlq":
            {
                var original = Entrada(i["original"]!.AsObject());
                var d = i["dlq"]!.AsObject();
                var ev = StateProtocol.ToDlqEvent(StateProtocol.BuildEvent(original), new StateProtocol.DlqInfo(
                    original.Key,
                    i["revision"]!.GetValue<long>(),
                    JsonSerializer.Deserialize<DlqReason>($"\"{Texto(d, "reason")}\"", Envelope.JsonOptions),
                    d["attempts"]!.GetValue<int>(),
                    Texto(d, "consumer")!,
                    Texto(d, "error")!,
                    Texto(d, "dlqtime")!));
                return (Envelope.Serialize(ev), null);
            }

            default:
                throw new InvalidOperationException(op);
        }
    }

    private static string Hex(byte[] b) => Convert.ToHexString(SHA256.HashData(b)).ToLowerInvariant();

    /// <summary>
    /// TODOS los vectores de estado, en proceso. Los que llevan `sha256` son los bytes DE LA SPEC (10-state.md §2.6, §6): que coincidan con
    /// Node no prueba que Node acierte, y estos sí lo prueban. Los de `expect` fijan el CÓDIGO y su ORDEN (§3.6).
    /// </summary>
    [Theory]
    [MemberData(nameof(LosVectores))]
    public void ElVectorDaLoQueDiceLaSpec(string id)
    {
        var v = Vectores.Single(x => x!["id"]!.GetValue<string>() == id)!.AsObject();
        var op = v["op"]!.GetValue<string>();
        var entrada = v["input"]!.AsObject();

        if (v["expect"]?.GetValue<string>() is { } codigo)
        {
            var e = Assert.IsAssignableFrom<Exception>(Record.Exception(() => Ejecuta(op, entrada)));
            var obtenido = e switch
            {
                ClassifiedException c => c.FluxCode,
                EnvelopeInputException i => i.Code,
                _ => e.GetType().Name,
            };
            Assert.Equal(codigo, obtenido);
            return;
        }

        var (bytes, subject) = Ejecuta(op, entrada);
        if (v["expectSubject"]?.GetValue<string>() is { } esperado)
        {
            Assert.Equal(esperado, subject);
        }

        if (v["sha256"]?.GetValue<string>() is { } sha)
        {
            Assert.Equal(sha, Hex(bytes!));
            if (v["length"] is { } largo)
            {
                Assert.Equal(largo.GetValue<int>(), bytes!.Length);
            }
        }
    }

    [Fact]
    public void HayVectoresDeLosCuatroTiposDeOperacion()
    {
        var ops = Vectores.Select(v => v!["op"]!.GetValue<string>()).ToHashSet();
        Assert.Equal(new[] { "state-build", "state-dlq", "state-subject", "state-tombstone" }, ops.OrderBy(o => o).ToArray());
        Assert.True(Vectores.Count >= 80, "los vectores de estado se han reducido");
    }

    // ─── la forma de lo que se construye ─────────────────────────────────────

    private static StateProtocol.EventInput Base(string data = "{\"aggregateVersion\":3}") =>
        new()
        {
            Contract = "clientes.cliente.v1",
            TenantId = "acme",
            Key = "cli-1",
            Id = "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
            Source = "/produccion/crm-api",
            Time = "2026-08-20T10:25:39.412Z",
            CorrelationId = "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
            ProducerVersion = "2.1.0",
            Data = JsonDocument.Parse(data).RootElement.Clone(),
        };

    [Fact]
    public void LaEntradaLlevaLaClaveComoSubjectYComoPartitionkeyYElTenantDelSubject()
    {
        var json = System.Text.Encoding.UTF8.GetString(Envelope.Serialize(StateProtocol.BuildEvent(Base())));
        Assert.Contains("\"subject\":\"cli-1\"", json, StringComparison.Ordinal);
        Assert.Contains("\"partitionkey\":\"cli-1\"", json, StringComparison.Ordinal);
        Assert.Contains("\"tenantid\":\"acme\"", json, StringComparison.Ordinal);
        Assert.Contains("\"type\":\"com.flux.state.clientes.cliente.v1\"", json, StringComparison.Ordinal);
        // Sin clasificación, `confidential`, NO `internal`: es un almacén que no caduca (§7).
        Assert.Contains("\"dataclassification\":\"confidential\"", json, StringComparison.Ordinal);
        // `data` es el ÚLTIMO atributo.
        Assert.EndsWith("\"data\":{\"aggregateVersion\":3}}", json, StringComparison.Ordinal);
    }

    [Fact]
    public void LaLapidaLlevaTipoPropioYSoloLaVersionEnData()
    {
        var ev = StateProtocol.BuildTombstone(new StateProtocol.TombstoneInput
        {
            Contract = "clientes.cliente.v1",
            TenantId = "acme",
            Key = "cli-1",
            Id = "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
            Source = "/produccion/crm-api",
            Time = "2026-08-20T10:25:39.412Z",
            CorrelationId = "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
            ProducerVersion = "2.1.0",
            AggregateVersion = JsonDocument.Parse("4").RootElement.Clone(),
        });
        var json = System.Text.Encoding.UTF8.GetString(Envelope.Serialize(ev));
        Assert.Contains("\"type\":\"com.flux.state.clientes.cliente.deleted.v1\"", json, StringComparison.Ordinal);
        Assert.EndsWith("\"data\":{\"aggregateVersion\":4}}", json, StringComparison.Ordinal);
    }

    [Fact]
    public void LaDlqDeEstadoNoConservaElDatoNiLaFirmaYDataEsElUltimo()
    {
        var original = StateProtocol.BuildEvent(Base("{\"aggregateVersion\":3,\"email\":\"ana@example.com\"}"));
        // Con firma: la DLQ la quita, porque firma un `data` que ya no está.
        var firmado = original with { SignKeyId = "k1", Signature = "AAAA" };
        var dlq = StateProtocol.ToDlqEvent(firmado, new StateProtocol.DlqInfo(
            "cli-1", 41, DlqReason.Permanent, 1, "tienda-web__state_clientes_cliente_v1__acme", "PAIS_NO_SOPORTADO", "2026-08-20T10:25:40.117Z"));
        var json = System.Text.Encoding.UTF8.GetString(Envelope.Serialize(dlq));
        Assert.DoesNotContain("ana@example.com", json, StringComparison.Ordinal);
        Assert.DoesNotContain("signature", json, StringComparison.Ordinal);
        Assert.DoesNotContain("signkeyid", json, StringComparison.Ordinal);
        Assert.EndsWith("\"data\":{\"key\":\"cli-1\",\"revision\":41}}", json, StringComparison.Ordinal);
    }

    // ─── clave, tenant y versión ─────────────────────────────────────────────

    [Theory]
    [InlineData("cli-1")]
    [InlineData("A_b-9")]
    [InlineData("0")]
    public void UnaClaveDeUnTokenEsValida(string key) => Assert.Equal(key, StateProtocol.AssertKey(key));

    [Theory]
    [InlineData("")]
    [InlineData("a.b")]
    [InlineData("a b")]
    [InlineData("a*")]
    [InlineData("a>")]
    [InlineData("ñandú")]
    [InlineData("cli-1\n")] // `$` de .NET casa antes de un salto final: la coincidencia es COMPLETA
    public void UnaClaveQueNoEsUnTokenSeRechaza(string key)
    {
        var e = Assert.Throws<InvalidStateKeyException>(() => StateProtocol.AssertKey(key));
        Assert.Equal("INVALID_STATE_KEY", e.FluxCode);
    }

    [Fact]
    public void LaClaveNoPuedeSerNulaNiPasarDelMaximo()
    {
        Assert.Throws<InvalidStateKeyException>(() => StateProtocol.AssertKey(null));
        Assert.Equal(new string('k', 128), StateProtocol.AssertKey(new string('k', 128)));
        Assert.Throws<InvalidStateKeyException>(() => StateProtocol.AssertKey(new string('k', 129)));
    }

    [Theory]
    [InlineData("acme")]
    [InlineData("acme-corp")]
    [InlineData("t1")]
    public void UnTenantDeEstadoValido(string t) => Assert.Equal(t, StateProtocol.AssertTenant(t));

    [Theory]
    [InlineData("")]
    [InlineData("ACME")]
    [InlineData("acme_corp")]
    [InlineData("acme.corp")]
    [InlineData("acme\n")]
    [InlineData("-acme")]
    public void UnTenantQueNoEsUnTokenMinusculaSeRechaza(string t)
    {
        var e = Assert.Throws<InvalidTenantForStateException>(() => StateProtocol.AssertTenant(t));
        Assert.Equal("INVALID_TENANT_FOR_STATE", e.FluxCode);
    }

    [Fact]
    public void UnTenantDeMasDe64CaracteresSeRechaza() =>
        Assert.Throws<InvalidTenantForStateException>(() => StateProtocol.AssertTenant(new string('a', 65)));

    [Theory]
    [InlineData("{\"aggregateVersion\":1}", 1)]
    [InlineData("{\"aggregateVersion\":9007199254740991}", 9007199254740991L)]
    public void UnaVersionEnteraDeUnoAlMaximoSeAcepta(string data, long esperado) =>
        Assert.Equal(esperado, StateProtocol.AggregateVersionOf(JsonDocument.Parse(data).RootElement));

    [Theory]
    [InlineData("{}")]
    [InlineData("{\"aggregateVersion\":null}")]
    [InlineData("{\"aggregateVersion\":0}")]
    [InlineData("{\"aggregateVersion\":-3}")]
    [InlineData("{\"aggregateVersion\":\"7\"}")]
    [InlineData("{\"aggregateVersion\":7.5}")]
    [InlineData("{\"aggregateVersion\":7.0}")] // §3.4: un parser que separa entero y flotante DEBE rechazarlo
    [InlineData("{\"aggregateVersion\":1e0}")]
    [InlineData("{\"aggregateVersion\":9007199254740992}")]
    [InlineData("{\"aggregateVersion\":true}")]
    [InlineData("[1]")]
    public void UnaVersionQueNoEsUnEnteroDeUnoAlMaximoSeRechaza(string data)
    {
        var e = Assert.Throws<MissingAggregateVersionException>(() => StateProtocol.AggregateVersionOf(JsonDocument.Parse(data).RootElement));
        Assert.Equal("MISSING_AGGREGATE_VERSION", e.FluxCode);
    }

    [Fact]
    public void ConUnaClaveRepetidaGanaLaUltimaComoEnLaFormaCanonica() =>
        Assert.Equal(5, StateProtocol.AggregateVersionOf(JsonDocument.Parse("{\"aggregateVersion\":9,\"aggregateVersion\":5}").RootElement));

    // ─── el orden de §3.6: el primero que falla gana ─────────────────────────

    [Fact]
    public void ElDominioReservadoGanaALaFormaDelContrato()
    {
        // `state.cliente` rompe las dos cosas: no es un contrato de tres tokens y su dominio es reservado.
        Assert.Throws<ReservedDomainException>(() => StateProtocol.ParseContract("state.cliente"));
        Assert.Throws<InvalidStateContractException>(() => StateProtocol.ParseContract("clientes.cliente"));
        Assert.Throws<InvalidStateContractException>(() => StateProtocol.ParseContract("clientes.cliente.v10000"));
        Assert.Equal(9999, StateProtocol.ParseContract("clientes.cliente.v9999").Major);
    }

    [Fact]
    public void LaClaveGanaAlTenantYElTenantALaVersion()
    {
        var malo = Base("{}") with { Key = "a.b", TenantId = "ACME" };
        Assert.Throws<InvalidStateKeyException>(() => StateProtocol.BuildEvent(malo));
        Assert.Throws<InvalidTenantForStateException>(() => StateProtocol.BuildEvent(malo with { Key = "cli-1" }));
        Assert.Throws<MissingAggregateVersionException>(() => StateProtocol.BuildEvent(malo with { Key = "cli-1", TenantId = "acme" }));
        Assert.Throws<InvalidDataClassificationException>(
            () => StateProtocol.BuildEvent(Base() with { Classification = DataClassification.Restricted }));
    }

    // ─── nombres derivados ───────────────────────────────────────────────────

    [Fact]
    public void LosNombresDeStreamDurableYSubjectNoLlevanPuntosDondeNatsNoLosAdmite()
    {
        Assert.Equal("STATE_CLIENTES", StateProtocol.StreamName("clientes"));
        Assert.Equal("STATE_MI_DOMINIO", StateProtocol.StreamName("mi-dominio"));
        Assert.Equal("state.clientes.cliente.v1.acme.cli-1", StateProtocol.Subject("clientes.cliente.v1", "acme", "cli-1"));
        Assert.Equal("tienda-web__state_clientes_cliente_v1__acme", StateProtocol.DurableName("tienda-web", "clientes.cliente.v1", "acme"));
        Assert.Equal("dlq.state.clientes.cliente.v1", StateProtocol.DlqSubject("clientes.cliente.v1"));
        Assert.DoesNotContain('.', StateProtocol.DurableName("tienda-web", "clientes.cliente.v1", "acme"));
    }

    [Fact]
    public void ElSubjectDeUnContratoReservadoSeRechazaAntesDeConstruirNada() =>
        Assert.Throws<ReservedDomainException>(() => StateProtocol.Subject("state.cliente.v1", "acme", "k"));
}
