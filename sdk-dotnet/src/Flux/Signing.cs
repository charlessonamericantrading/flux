// Firma de eventos: contrato y política. La criptografía vive en el paquete Flux.Signing.
// Contrato normativo: specification/07-signing.md
//
// ⚠️ Ed25519 NO está en la BCL de .NET 8. `System.Security.Cryptography` trae RSA, ECDsa y
// ECDiffieHellman, pero no EdDSA: no hay `Ed25519` ni una curva `ed25519` en `ECCurve`
// (ECCurve modela curvas de Weierstrass y Ed25519 es de Edwards retorcida). Es la única
// diferencia de plataforma real de esta fase: en Node está en `node:crypto`, en Java en
// `java.security` desde el JDK 15, en Go en `crypto/ed25519`, y aquí hace falta un paquete.
//
// Por eso este fichero contiene TODO menos la primitiva: los tipos del contrato, la
// política de verificación y los bytes canónicos. El paquete `Flux.Signing` —que sí
// arrastra BouncyCastle— implementa las dos interfaces de abajo. Así el paquete `Flux`, que
// es el que instala todo servicio que solo quiere publicar un evento, sigue dependiendo
// únicamente de NATS.Net y System.Text.Json. Ver sdk-dotnet/README.md §"Firma".

namespace Flux;

/// <summary>Qué hacer ante un evento sin firma o con una firma inválida — 07-signing.md §7.</summary>
/// <remarks>
/// <see cref="Warn"/> existe porque adoptar la firma en un ecosistema en marcha exige un
/// periodo en el que unos productores firman y otros no. Pasar directo a
/// <see cref="Require"/> convierte en POISON todo evento de un servicio aún no migrado.
/// </remarks>
public enum VerificationMode
{
    /// <summary>Default. No se mira la firma, y no se construye verificador.</summary>
    Off,

    /// <summary>Se registra y se acepta. El modo de la migración.</summary>
    Warn,

    /// <summary>POISON. Solo cuando TODOS los productores del subject firman.</summary>
    Require,
}

/// <summary>Añade <c>signkeyid</c> y <c>signature</c> a un evento antes de publicarlo.</summary>
/// <remarks>
/// La implementación Ed25519 vive en el paquete <c>Flux.Signing</c>. Esta interfaz está
/// aquí para que <see cref="ConnectOptions"/> pueda referirla sin que el paquete base
/// dependa de ninguna librería de criptografía.
/// </remarks>
public interface IEventSigner
{
    /// <summary>Devuelve una copia firmada. El evento original no se toca.</summary>
    /// <param name="evento">Evento recién construido, aún sin publicar.</param>
    FluxEvent Sign(FluxEvent evento);
}

/// <summary>Aplica la política de <see cref="VerificationMode"/> a un evento entrante.</summary>
public interface IEventVerifier
{
    /// <summary>Comprueba la firma según el modo configurado.</summary>
    /// <param name="evento">El evento tal y como llegó del stream.</param>
    /// <exception cref="PoisonException">
    /// En modo <see cref="VerificationMode.Require"/>, con el código de
    /// <see cref="EventSigning"/> que corresponda.
    /// </exception>
    void Check(FluxEvent evento);
}

/// <summary>
/// Una clave mal formada, de otro algoritmo, o una configuración de firma incoherente.
/// </summary>
/// <remarks>
/// Es un bug de configuración LOCAL, no un mensaje ajeno corrupto: por eso no es una
/// <see cref="PoisonException"/>. Se detecta al construir el firmante o el verificador
/// —es decir, al arrancar el servicio— y no en la ruta caliente.
/// </remarks>
public sealed class SigningKeyException : Exception
{
    /// <summary>Construye la excepción.</summary>
    /// <param name="message">Qué está mal en la configuración de firma.</param>
    public SigningKeyException(string message)
        : base(message)
    {
    }

    /// <summary>Construye la excepción encadenando la causa.</summary>
    /// <param name="message">Qué está mal en la configuración de firma.</param>
    /// <param name="innerException">El fallo original de la librería de criptografía.</param>
    public SigningKeyException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Los bytes que se firman y los códigos POISON de la extensión — 07-signing.md §5 y §7.
/// </summary>
public static class EventSigning
{
    /// <summary>El único algoritmo. Sin negociación, sin parámetros — 07-signing.md §3.</summary>
    /// <remarks>
    /// La razón es de superficie de error, no de rendimiento: los formatos de firma con
    /// algoritmo negociable acumulan una familia de vulnerabilidades bien conocida —desde
    /// <c>alg: none</c> hasta la confusión entre HMAC y RSA— que solo existe porque hay algo
    /// que negociar. Aquí no lo hay.
    /// </remarks>
    public const string Algorithm = "Ed25519";

    /// <summary>Falta <c>signature</c> en modo <see cref="VerificationMode.Require"/>.</summary>
    public const string MissingSignature = "MISSING_SIGNATURE";

    /// <summary>La firma no verifica.</summary>
    public const string InvalidSignature = "INVALID_SIGNATURE";

    /// <summary>El <c>signkeyid</c> no está entre las claves públicas conocidas.</summary>
    public const string UnknownSigningKey = "UNKNOWN_SIGNING_KEY";

    /// <summary>
    /// Los bytes canónicos que se firman: el evento SIN <c>signature</c> y SIN las
    /// extensiones <c>dlq*</c> — 07-signing.md §5.
    /// </summary>
    /// <remarks>
    /// <b>No hay una canonicalización aparte para firmar</b>: es el mismo
    /// <see cref="Envelope.Serialize"/> que usa el productor. Funciona porque
    /// 01-envelope.md §1.1 (UTF-8 literal, sin escapes), §2.2 (<c>time</c> con exactamente
    /// 3 decimales) y §6 (orden de claves, <c>data</c> al final) fijan una única
    /// representación en bytes. Las tres se escribieron para otros problemas —replay
    /// verbatim, fixtures compartidos, interoperabilidad— y juntas resultaron ser
    /// exactamente la canonicalización que la firma necesitaba.
    /// <para>
    /// <c>signkeyid</c> SÍ entra. Si quedara fuera, un atacante podría cambiarlo para que la
    /// verificación buscara otra clave —una suya— y la firma seguiría "verificando".
    /// </para>
    /// <para>
    /// Las <c>dlq*</c> se añaden DESPUÉS de firmar, así que no están cubiertas. Que se
    /// quiten aquí es lo que hace que <b>un evento que ha pasado por la DLQ conserve su
    /// firma válida</b>, y eso es lo correcto: el replay redistribuye un hecho ya emitido,
    /// no crea uno nuevo (§5.1).
    /// </para>
    /// </remarks>
    /// <param name="evento">El evento a firmar o a verificar.</param>
    /// <returns>Los bytes UTF-8 sobre los que opera Ed25519.</returns>
    public static byte[] SignablePayload(FluxEvent evento)
    {
        ArgumentNullException.ThrowIfNull(evento);
        return Envelope.Serialize(Envelope.StripDlqExtensions(evento) with { Signature = null });
    }
}
