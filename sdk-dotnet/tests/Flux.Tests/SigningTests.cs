// Firma Ed25519 de eventos — 07-signing.md.
//
// Los casos son los mismos que los del SDK de Node (test/signing.test.ts). Que la lista
// coincida no es burocracia: la firma es lo único del protocolo que solo funciona si dos
// lenguajes producen exactamente los mismos bytes, así que un caso que un SDK no cubra es
// un caso donde la interoperabilidad no está comprobada.

using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Xunit;

namespace Flux.Tests;

public class SigningTests
{
    private const string KeyId = "pedidos-api-1";

    private const string FixtureId = "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55";

    private static readonly (string PrivateKeyPem, string PublicKeyPem) Par =
        Ed25519Signing.GenerateKeyPair();

    private static readonly IEventSigner Signer = Ed25519Signing.CreateSigner(new SigningOptions
    {
        PrivateKeyPem = Par.PrivateKeyPem,
        KeyId = KeyId,
    })!;

    private static readonly IEventVerifier Verifier = Ed25519Signing.CreateVerifier(new SigningOptions
    {
        PublicKeys = new Dictionary<string, string>(StringComparer.Ordinal) { [KeyId] = Par.PublicKeyPem },
        Verify = VerificationMode.Require,
    })!;

    /// <summary>El mismo evento de referencia que usa el SDK de Node en su suite de firma.</summary>
    private static FluxEvent Evento() => Envelope.BuildEvent(new Envelope.BuildEventInput
    {
        Subject = "pedidos.pedido.v1.creado",
        Data = new { pedidoId = "ped-123" },
        Id = FixtureId,
        Source = "/produccion/pedidos-api",
        ProducerVersion = "3.4.1",
        TenantId = "acme",
        DataClassification = DataClassification.Internal,
        DataSchema = "https://schemas.internal/pedidos/pedido/creado/1.0.0.json",
        CorrelationId = FixtureId,
        Time = DateTimeOffset.FromUnixTimeMilliseconds(1_755_685_539_410L),
    });

    private static string Json(FluxEvent evento) => Encoding.UTF8.GetString(Envelope.Serialize(evento));

    /// <summary>Reserializa el evento cambiando un atributo raíz, como haría un atacante.</summary>
    private static FluxEvent Manipular(FluxEvent firmado, string atributo, string valor)
    {
        var json = Json(firmado);
        using var documento = JsonDocument.Parse(json);
        var buffer = new System.IO.MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions
               {
                   Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
               }))
        {
            writer.WriteStartObject();
            foreach (var propiedad in documento.RootElement.EnumerateObject())
            {
                if (string.Equals(propiedad.Name, atributo, StringComparison.Ordinal))
                {
                    writer.WriteString(atributo, valor);
                }
                else
                {
                    propiedad.WriteTo(writer);
                }
            }

            writer.WriteEndObject();
        }

        return Envelope.ParseEvent(buffer.ToArray());
    }

    private static string CodigoDe(Action accion)
    {
        var e = Assert.Throws<PoisonException>(accion);
        return e.FluxCode;
    }

    // ─── firma ───────────────────────────────────────────────────────────────

    [Fact]
    public void AnadeSignKeyIdYSignatureAmbosAntesDeData()
    {
        var firmado = Signer.Sign(Evento());

        Assert.Equal(KeyId, firmado.SignKeyId);
        Assert.NotNull(firmado.Signature);

        // base64url SIN padding — 07-signing.md §4.
        Assert.Matches("^[A-Za-z0-9_-]+$", firmado.Signature!);

        var json = Json(firmado);
        Assert.True(json.IndexOf("\"signkeyid\"", StringComparison.Ordinal)
                    < json.IndexOf("\"signature\"", StringComparison.Ordinal));
        Assert.True(json.IndexOf("\"signature\"", StringComparison.Ordinal)
                    < json.IndexOf("\"data\"", StringComparison.Ordinal));
        Assert.EndsWith("\"data\":{\"pedidoId\":\"ped-123\"}}", json, StringComparison.Ordinal);
    }

    [Fact]
    public void UnaFirmaValidaVerifica() => Verifier.Check(Signer.Sign(Evento()));

    [Fact]
    public void LaFirmaEsDeterminista()
    {
        // Solo es cierto porque 01-envelope.md §1.1, §2.2 y §6 fijan una única
        // representación en bytes. Sin ellas firmar sería imposible entre lenguajes: el
        // mismo evento daría dos secuencias de bytes y dos firmas distintas.
        Assert.Equal(Signer.Sign(Evento()).Signature, Signer.Sign(Evento()).Signature);
    }

    [Fact]
    public void SobreviveAUnRoundTripDeSerializacion()
    {
        var ida = Envelope.ParseEvent(Envelope.Serialize(Signer.Sign(Evento())));
        Verifier.Check(ida);
    }

    [Fact]
    public void LoFirmadoEsElSerializeDelProductor()
    {
        // No hay canonicalización aparte para firmar (07-signing.md §2). Este test lo fija:
        // si alguien introdujera una forma canónica propia, las firmas de .NET dejarían de
        // verificar en Node sin que ningún otro test se enterase.
        var firmado = Signer.Sign(Evento());
        var payload = Encoding.UTF8.GetString(EventSigning.SignablePayload(firmado));

        Assert.Contains($"\"signkeyid\":\"{KeyId}\"", payload, StringComparison.Ordinal);
        Assert.DoesNotContain("\"signature\"", payload, StringComparison.Ordinal);
        Assert.EndsWith("\"data\":{\"pedidoId\":\"ped-123\"}}", payload, StringComparison.Ordinal);
    }

    // ─── detección de manipulación ───────────────────────────────────────────

    [Fact]
    public void AlterarDataInvalidaLaFirma()
    {
        var firmado = Signer.Sign(Evento());
        var manipulado = firmado with { Data = JsonSerializer.SerializeToElement(new { pedidoId = "ped-999" }) };

        Assert.Equal(EventSigning.InvalidSignature, CodigoDe(() => Verifier.Check(manipulado)));
    }

    [Fact]
    public void AlterarElTenantIdInvalidaLaFirma()
    {
        // El caso que la ACL del broker NO cubre: un evento sacado del stream, editado y
        // reinyectado. La ACL dice quién puede escribir en el dominio, no quién escribió
        // este evento concreto — 07-signing.md §1 y 09-multitenancy.md §4.
        var falso = Manipular(Signer.Sign(Evento()), "tenantid", "otro");
        Assert.Equal(EventSigning.InvalidSignature, CodigoDe(() => Verifier.Check(falso)));
    }

    [Fact]
    public void CambiarSignKeyIdNoPermiteEludirLaVerificacion()
    {
        // signkeyid va DENTRO de lo firmado justo para esto: si quedara fuera, un atacante
        // lo cambiaría por el id de una clave suya — 07-signing.md §5.
        var falso = Manipular(Signer.Sign(Evento()), "signkeyid", "otro-1");
        Assert.Equal(EventSigning.UnknownSigningKey, CodigoDe(() => Verifier.Check(falso)));
    }

    [Fact]
    public void UnaFirmaDeOtraClaveNoVerificaAunqueElSignKeyIdCoincida()
    {
        var otra = Ed25519Signing.GenerateKeyPair();
        var impostor = Ed25519Signing.CreateSigner(new SigningOptions
        {
            PrivateKeyPem = otra.PrivateKeyPem,
            KeyId = KeyId,
        })!;

        Assert.Equal(
            EventSigning.InvalidSignature,
            CodigoDe(() => Verifier.Check(impostor.Sign(Evento()))));
    }

    [Fact]
    public void UnaFirmaQueNoEsBase64UrlEsInvalidSignatureYNoUnaExcepcionCruda()
    {
        var falso = Manipular(Signer.Sign(Evento()), "signature", "no-es-base64!!");
        Assert.Equal(EventSigning.InvalidSignature, CodigoDe(() => Verifier.Check(falso)));
    }

    // ─── DLQ y replay ────────────────────────────────────────────────────────

    [Fact]
    public void LaFirmaSigueVerificandoTrasPasarPorLaDlq()
    {
        // Las extensiones dlq* se añaden DESPUÉS de firmar y no están cubiertas. Si la
        // verificación no las ignorara, TODO evento de la DLQ parecería manipulado y la
        // firma sería incompatible con el mecanismo de errores del propio protocolo.
        var enDlq = Envelope.ToDlqEvent(
            Signer.Sign(Evento()),
            new Envelope.DlqInfo(
                DlqReason.Permanent, 1, "facturacion-api__pedidos_pedido_v1_creado", "PEDIDO_YA_CANCELADO"));

        Verifier.Check(enDlq);
    }

    [Fact]
    public void UnEventoReproducidoDesdeLaDlqConservaSuFirmaValida()
    {
        // El replay redistribuye un hecho ya emitido, no crea uno nuevo — §5.1.
        var enDlq = Envelope.ToDlqEvent(
            Signer.Sign(Evento()),
            new Envelope.DlqInfo(DlqReason.Retryable, 6, "c", "x"));

        var reproducido = Envelope.StripDlqExtensions(Envelope.ParseEvent(Envelope.Serialize(enDlq)));
        Verifier.Check(reproducido);
    }

    [Fact]
    public void EnLaDlqLaFirmaVaAntesDeLasExtensionesDlq()
    {
        // Byte a byte con Node: allí toDlqEvent expande el resto del evento —que ya lleva la
        // firma— y añade las dlq* después. Si .NET las intercalara al revés, el mismo evento
        // de DLQ tendría dos representaciones y el replay verbatim dejaría de serlo
        // (01-envelope.md §6).
        var json = Json(Envelope.ToDlqEvent(
            Signer.Sign(Evento()),
            new Envelope.DlqInfo(DlqReason.Poison, 1, "c", "x")));

        Assert.True(json.IndexOf("\"signature\"", StringComparison.Ordinal)
                    < json.IndexOf("\"dlqreason\"", StringComparison.Ordinal));
        Assert.True(json.IndexOf("\"dlqtime\"", StringComparison.Ordinal)
                    < json.IndexOf("\"data\"", StringComparison.Ordinal));
    }

    // ─── modos de verificación ───────────────────────────────────────────────

    [Fact]
    public void RequireUnEventoSinFirmaEsPoison()
    {
        Assert.Equal(EventSigning.MissingSignature, CodigoDe(() => Verifier.Check(Evento())));
    }

    [Fact]
    public void WarnRegistraYAcepta()
    {
        // `warn` existe porque adoptar la firma en un ecosistema en marcha exige un periodo
        // en el que unos productores firman y otros no. Pasar directo a `require` convierte
        // en POISON todo evento de un servicio aún no migrado.
        var avisos = new List<string>();
        var verificador = Ed25519Signing.CreateVerifier(new SigningOptions
        {
            PublicKeys = new Dictionary<string, string>(StringComparer.Ordinal) { [KeyId] = Par.PublicKeyPem },
            Verify = VerificationMode.Warn,
            OnWarn = avisos.Add,
        })!;

        verificador.Check(Evento());
        Assert.Single(avisos);
        Assert.Contains(EventSigning.MissingSignature, avisos[0], StringComparison.Ordinal);

        verificador.Check(Manipular(Signer.Sign(Evento()), "tenantid", "otro"));
        Assert.Equal(2, avisos.Count);
        Assert.Contains(EventSigning.InvalidSignature, avisos[1], StringComparison.Ordinal);
    }

    [Fact]
    public void OffNoConstruyeVerificador()
    {
        // No se paga lo que no se usa: sin verificador, el despacho no tiene ni una rama de
        // más. Es además el DEFAULT (07-signing.md §7).
        Assert.Null(Ed25519Signing.CreateVerifier(new SigningOptions()));
        Assert.Null(Ed25519Signing.CreateVerifier(new SigningOptions { Verify = VerificationMode.Off }));
        Assert.Null(Ed25519Signing.CreateVerifier(null));
    }

    [Fact]
    public void SinClavePrivadaNoHayFirmante()
    {
        Assert.Null(Ed25519Signing.CreateSigner(new SigningOptions()));
        Assert.Null(Ed25519Signing.CreateSigner(null));
    }

    // ─── gestión de claves ───────────────────────────────────────────────────

    [Fact]
    public void FirmarSinKeyIdFallaConUnMensajeAccionable()
    {
        var e = Assert.Throws<SigningKeyException>(() =>
            Ed25519Signing.CreateSigner(new SigningOptions { PrivateKeyPem = Par.PrivateKeyPem }));

        Assert.Contains("KeyId", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RechazaUnaClaveQueNoSeaEd25519()
    {
        // El protocolo NO negocia algoritmo: los formatos con algoritmo negociable acumulan
        // una familia de vulnerabilidades que solo existe porque hay algo que negociar
        // (07-signing.md §3). Una RSA se rechaza al construir, no al firmar.
        using var rsa = RSA.Create(2048);
        var pem = new StringBuilder()
            .Append("-----BEGIN PRIVATE KEY-----\n")
            .Append(Convert.ToBase64String(rsa.ExportPkcs8PrivateKey()))
            .Append("\n-----END PRIVATE KEY-----\n")
            .ToString();

        var e = Assert.Throws<SigningKeyException>(() =>
            Ed25519Signing.CreateSigner(new SigningOptions { PrivateKeyPem = pem, KeyId = "x-1" }));

        Assert.Contains("Ed25519", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void VerificarSinClavesPublicasFallaExplicandoLaRetencion()
    {
        var e = Assert.Throws<SigningKeyException>(() =>
            Ed25519Signing.CreateVerifier(new SigningOptions { Verify = VerificationMode.Require }));

        Assert.Contains("RETIRADAS", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void UnaClaveRetiradaSigueVerificandoSiSeConservaLaPublica()
    {
        // Es la regla que más se equivoca. Retirar una clave impide EMITIR con ella, no
        // VERIFICAR lo ya emitido: un evento es un hecho del pasado y su firma era válida
        // cuando se emitió. Tratarla como inválida convierte una rotación rutinaria en la
        // invalidación retroactiva de todo el historial — §6.
        var vieja = Ed25519Signing.GenerateKeyPair();
        var nueva = Ed25519Signing.GenerateKeyPair();

        var firmadoConLaVieja = Ed25519Signing.CreateSigner(new SigningOptions
        {
            PrivateKeyPem = vieja.PrivateKeyPem,
            KeyId = "pedidos-api-1",
        })!.Sign(Evento());

        var verificador = Ed25519Signing.CreateVerifier(new SigningOptions
        {
            PublicKeys = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["pedidos-api-1"] = vieja.PublicKeyPem, // retirada, conservada
                ["pedidos-api-2"] = nueva.PublicKeyPem, // activa
            },
            Verify = VerificationMode.Require,
        })!;

        verificador.Check(firmadoConLaVieja);
    }

    [Fact]
    public void ElParGeneradoEsPkcs8YSpkiEstandar()
    {
        // Si el DER no fuese el canónico de RFC 8410, una clave generada aquí no la podrían
        // leer ni OpenSSL, ni Node, ni Java — y el ecosistema es polyglot por definición.
        var par = Ed25519Signing.GenerateKeyPair();

        Assert.StartsWith("-----BEGIN PRIVATE KEY-----", par.PrivateKeyPem, StringComparison.Ordinal);
        Assert.StartsWith("-----BEGIN PUBLIC KEY-----", par.PublicKeyPem, StringComparison.Ordinal);

        var firmante = Ed25519Signing.CreateSigner(new SigningOptions
        {
            PrivateKeyPem = par.PrivateKeyPem,
            KeyId = "k-1",
        })!;
        var verificador = Ed25519Signing.CreateVerifier(new SigningOptions
        {
            PublicKeys = new Dictionary<string, string>(StringComparer.Ordinal) { ["k-1"] = par.PublicKeyPem },
            Verify = VerificationMode.Require,
        })!;

        verificador.Check(firmante.Sign(Evento()));
    }

    [Fact]
    public void LaFirmaCabeEnLoQueDiceLaSpec()
    {
        // 64 bytes de firma en base64url sin padding = 86 caracteres. Que sea fijo es lo que
        // permite a un consumidor descartar basura antes de tocar la criptografía.
        var firma = Signer.Sign(Evento()).Signature!;
        Assert.Equal(86, firma.Length);
    }
}
