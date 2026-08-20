// Arnés de conformidad cruzada — SDK de .NET.
// Contrato: conformance/harness/README.md
//
// Lee UNA operación por stdin, escribe UN resultado por stdout, sale con 0 SIEMPRE.
// Deliberadamente delgado: toda lógica aquí es lógica que no está en el SDK y que el
// runner, por tanto, no verifica.
//
// Se invoca como dice conformance/harnesses.json:
//
//   cd sdk-dotnet && dotnet run -v quiet --project tools/Flux.ConformanceHarness/Flux.ConformanceHarness.csproj

using System.Text.Encodings.Web;
using System.Text.Json;

namespace Flux.Conformance;

/// <summary>Punto de entrada del arnés. No forma parte de la API del SDK.</summary>
internal static class Program
{
    /// <summary>
    /// Lo que el arnés responde. <c>Bytes</c> y <c>Code</c> son excluyentes; <c>Detail</c>
    /// es informativo y el runner no lo compara.
    /// </summary>
    private sealed record Resultado(bool Ok, string? Bytes = null, string? Code = null, string? Detail = null);

    private static int Main()
    {
        Resultado resultado;
        try
        {
            using var entrada = LeerEntrada();
            resultado = Ejecutar(entrada.RootElement);
        }
        catch (Exception e)
        {
            // Un fallo de la operación se REPORTA, no se propaga: exit != 0 significaría
            // que el arnés está roto, no que el caso falló.
            resultado = new Resultado(false, Code: CodigoDe(e), Detail: e.Message);
        }

        Escribir(resultado);
        return 0;
    }

    private static JsonDocument LeerEntrada()
    {
        using var stdin = Console.OpenStandardInput();
        using var buffer = new MemoryStream();
        stdin.CopyTo(buffer);
        return JsonDocument.Parse(buffer.ToArray());
    }

    private static Resultado Ejecutar(JsonElement entrada)
    {
        var op = Texto(entrada, "op");
        switch (op)
        {
            case "build":
                return Ok(Envelope.Serialize(Construir(entrada.GetProperty("event"))));

            case "dlq":
            {
                var evento = Construir(entrada.GetProperty("event"));
                if (entrada.TryGetProperty("signFirst", out var primero)
                    && primero.ValueKind == JsonValueKind.True)
                {
                    evento = Firmante(entrada.GetProperty("signing")).Sign(evento);
                }

                var d = entrada.GetProperty("dlq");
                var conDlq = Envelope.ToDlqEvent(evento, new Envelope.DlqInfo(
                    d.GetProperty("reason").Deserialize<DlqReason>(Envelope.JsonOptions),
                    d.GetProperty("attempts").GetInt32(),
                    Texto(d, "consumer")!,
                    Texto(d, "error")!));

                // `dlqtime` lo fija el vector: si lo pusiera el SDK —y lo pone, ToDlqEvent
                // sella DateTimeOffset.UtcNow— los bytes no serían comparables entre
                // ejecuciones, y mucho menos entre lenguajes. Se sobrescribe solo ese
                // atributo: el orden de claves, el recorte de `dlqerror` y todo lo demás
                // siguen siendo los del SDK.
                return Ok(Envelope.Serialize(conDlq with { DlqTime = Texto(d, "dlqtime") }));
            }

            case "sign":
                return Ok(Envelope.Serialize(
                    Firmante(entrada.GetProperty("signing")).Sign(Construir(entrada.GetProperty("event")))));

            case "verify":
            {
                var evento = Envelope.ParseEvent(Convert.FromBase64String(Texto(entrada, "bytes")!));

                var claves = new Dictionary<string, string>(StringComparer.Ordinal);
                foreach (var clave in entrada.GetProperty("publicKeys").EnumerateObject())
                {
                    claves[clave.Name] = clave.Value.GetString()!;
                }

                var modo = entrada.TryGetProperty("mode", out var m) && m.ValueKind == JsonValueKind.String
                    ? Enum.Parse<VerificationMode>(m.GetString()!, ignoreCase: true)
                    : VerificationMode.Require;

                var verificador = Ed25519Signing.CreateVerifier(new SigningOptions
                {
                    PublicKeys = claves,
                    Verify = modo,
                })!;

                try
                {
                    verificador.Check(evento);
                    return new Resultado(true);
                }
                catch (ClassifiedException e)
                {
                    return new Resultado(false, Code: e.FluxCode);
                }
            }

            case "parse":
                Envelope.ParseEvent(Convert.FromBase64String(Texto(entrada, "bytes")!));
                return new Resultado(true);

            default:
                return new Resultado(false, Code: "UNSUPPORTED_OP", Detail: op);
        }
    }

    /// <summary>
    /// Construye el evento con los atributos TAL CUAL vienen del vector.
    /// </summary>
    /// <remarks>
    /// El arnés NO rellena nada: <c>id</c>, <c>time</c> y las extensiones vienen dados, o
    /// los bytes de dos SDKs no serían comparables.
    /// </remarks>
    private static FluxEvent Construir(JsonElement e) =>
        Envelope.BuildEvent(new Envelope.BuildEventInput
        {
            Subject = Texto(e, "subject")!,
            Data = e.GetProperty("data"),
            Id = Texto(e, "id")!,
            Source = Texto(e, "source")!,
            DataSchema = Texto(e, "dataschema")!,
            CorrelationId = Texto(e, "correlationid")!,
            TenantId = Texto(e, "tenantid")!,
            ProducerVersion = Texto(e, "producerversion")!,
            // Vía el convertidor del SDK y no con un switch propio: el mapeo del literal
            // del protocolo al enum es del SDK, y duplicarlo aquí sería lógica que el
            // runner no verifica.
            DataClassification = e.GetProperty("dataclassification")
                .Deserialize<DataClassification>(Envelope.JsonOptions),
            Time = Texto(e, "time") is { } t ? (DateTimeOffset?)Envelope.ParseTime(t) : null,
            AggregateId = Texto(e, "aggregateId"),
            CausationId = Texto(e, "causationid"),
            PartitionKey = Texto(e, "partitionkey"),
            TraceParent = Texto(e, "traceparent"),
            TraceState = Texto(e, "tracestate"),
        });

    private static IEventSigner Firmante(JsonElement signing) =>
        Ed25519Signing.CreateSigner(new SigningOptions
        {
            PrivateKeyPem = Texto(signing, "privateKeyPem"),
            KeyId = Texto(signing, "keyId"),
        })!;

    // ─── Entrada y salida ────────────────────────────────────────────────────

    /// <summary>El atributo como cadena, o <see langword="null"/> si falta o es <c>null</c>.</summary>
    private static string? Texto(JsonElement objeto, string nombre) =>
        objeto.TryGetProperty(nombre, out var valor) && valor.ValueKind == JsonValueKind.String
            ? valor.GetString()
            : null;

    /// <summary><c>bytes</c> va en base64 ESTÁNDAR (con relleno), como el <c>Buffer</c> de Node.</summary>
    private static Resultado Ok(byte[] evento) => new(true, Bytes: Convert.ToBase64String(evento));

    /// <summary>
    /// El <c>code</c> del error, que es lo que el runner compara entre los siete SDKs.
    /// </summary>
    /// <remarks>
    /// Sin código propio se cae al nombre del tipo: un arnés que devolviera siempre un
    /// código genérico haría pasar por idénticos SDKs que clasifican distinto.
    /// </remarks>
    private static string CodigoDe(Exception e) =>
        e is ClassifiedException c ? c.FluxCode : e.GetType().Name;

    /// <summary>
    /// Escribe el resultado como UN objeto JSON en stdout, y nada más.
    /// </summary>
    /// <remarks>
    /// Se escriben BYTES sobre el stream estándar en vez de usar
    /// <see cref="Console.Out"/>: en Windows la consola recodificaría el UTF-8 a su
    /// codepage, que es justo lo que este arnés existe para comprobar.
    /// </remarks>
    private static void Escribir(Resultado resultado)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(
            buffer,
            new JsonWriterOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping }))
        {
            writer.WriteStartObject();
            writer.WriteBoolean("ok", resultado.Ok);
            if (resultado.Bytes is not null)
            {
                writer.WriteString("bytes", resultado.Bytes);
            }

            if (resultado.Code is not null)
            {
                writer.WriteString("code", resultado.Code);
            }

            if (resultado.Detail is not null)
            {
                writer.WriteString("detail", resultado.Detail);
            }

            writer.WriteEndObject();
        }

        var bytes = buffer.ToArray();
        using var stdout = Console.OpenStandardOutput();
        stdout.Write(bytes, 0, bytes.Length);
        stdout.Flush();
    }
}
