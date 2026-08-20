// Firma Ed25519 de eventos, sobre BouncyCastle.
// Contrato normativo: specification/07-signing.md
//
// Extensión OPCIONAL de v1: el default es `off` y un evento SIN firma sigue siendo válido.
//
// Este fichero es lo único del SDK que conoce criptografía. Todo lo demás —los tipos del
// contrato, la política de verificación y los bytes canónicos— vive en el paquete `Flux`,
// que no depende de BouncyCastle. Ver Flux.Signing.csproj para por qué BouncyCastle y no
// NSec.

using System.Security.Cryptography;
using System.Text;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Crypto.Signers;
using Org.BouncyCastle.Security;

namespace Flux;

/// <summary>Configuración de la extensión de firma — 07-signing.md.</summary>
public sealed record SigningOptions
{
    /// <summary>
    /// Clave privada Ed25519 en PEM (PKCS#8) para firmar al publicar.
    /// <see langword="null"/> para solo verificar.
    /// </summary>
    /// <remarks><b>NUNCA se versiona</b> — 06-security.md §2.</remarks>
    public string? PrivateKeyPem { get; init; }

    /// <summary>
    /// Id de la clave, formato <c>&lt;servicio&gt;-&lt;n&gt;</c>. Obligatorio si se firma.
    /// </summary>
    /// <remarks>
    /// DEBE cambiar en cada rotación. Reutilizar un id con una clave distinta convierte la
    /// verificación de eventos históricos en un juego de azar — 07-signing.md §6.
    /// </remarks>
    public string? KeyId { get; init; }

    /// <summary>Claves públicas conocidas: <c>signkeyid</c> → PEM (SPKI).</summary>
    /// <remarks>
    /// <b>Incluye aquí las claves RETIRADAS mientras exista algún evento firmado con
    /// ellas.</b> El mínimo son 90 días —la retención de la DLQ— más el margen de los
    /// backups. Retirar una clave impide EMITIR con ella, no VERIFICAR lo ya emitido:
    /// tratar una clave retirada como inválida convierte una rotación rutinaria en la
    /// invalidación retroactiva de todo el historial — 07-signing.md §6.
    /// </remarks>
    public IReadOnlyDictionary<string, string>? PublicKeys { get; init; }

    /// <summary>Política de verificación. Default <see cref="VerificationMode.Off"/>.</summary>
    public VerificationMode Verify { get; init; } = VerificationMode.Off;

    /// <summary>
    /// Dónde va el aviso del modo <see cref="VerificationMode.Warn"/>.
    /// <see langword="null"/> escribe en <see cref="Console.Error"/>.
    /// </summary>
    /// <remarks>
    /// No se reutiliza <c>ConnectOptions.Logger</c> porque el verificador se construye
    /// también suelto —en tests y en herramientas de verificación offline— donde no hay un
    /// bus del que colgarlo.
    /// </remarks>
    public Action<string>? OnWarn { get; init; }
}

/// <summary>
/// Construye firmantes y verificadores Ed25519 a partir de claves en PEM.
/// </summary>
/// <example>
/// <code>
/// var (signer, verifier) = Ed25519Signing.Create(new SigningOptions
/// {
///     PrivateKeyPem = File.ReadAllText("pedidos-api-1.key"),
///     KeyId         = "pedidos-api-1",
///     PublicKeys    = new Dictionary&lt;string, string&gt; { ["pedidos-api-1"] = pem },
///     Verify        = VerificationMode.Require,
/// });
///
/// await using var bus = await FluxBus.ConnectAsync(new ConnectOptions
/// {
///     // …
///     Signer   = signer,
///     Verifier = verifier,
/// });
/// </code>
/// </example>
public static class Ed25519Signing
{
    /// <summary>Longitud de una firma Ed25519, en bytes — 07-signing.md §3.</summary>
    private const int SignatureBytes = 64;

    /// <summary>Longitud de una clave Ed25519 (pública o semilla privada), en bytes.</summary>
    private const int KeyBytes = 32;

    /// <summary>Construye a la vez el firmante y el verificador de unas opciones.</summary>
    /// <param name="options">Configuración de firma.</param>
    /// <returns>
    /// El firmante (<see langword="null"/> si no hay clave privada) y el verificador
    /// (<see langword="null"/> si el modo es <see cref="VerificationMode.Off"/>).
    /// </returns>
    /// <exception cref="SigningKeyException">La configuración de claves es incoherente.</exception>
    public static (IEventSigner? Signer, IEventVerifier? Verifier) Create(SigningOptions options) =>
        (CreateSigner(options), CreateVerifier(options));

    /// <summary>
    /// Construye el firmante, o <see langword="null"/> si no hay clave privada configurada.
    /// </summary>
    /// <remarks>
    /// <see langword="null"/> y no un firmante que no hace nada: un servicio que solo
    /// verifica no debe pagar ni una rama en la ruta de publicación.
    /// </remarks>
    /// <param name="options">Configuración de firma.</param>
    /// <returns>El firmante, o <see langword="null"/>.</returns>
    /// <exception cref="SigningKeyException">
    /// Hay clave privada pero no <see cref="SigningOptions.KeyId"/>, o el PEM no es una
    /// Ed25519 en PKCS#8.
    /// </exception>
    public static IEventSigner? CreateSigner(SigningOptions? options)
    {
        if (options is null || string.IsNullOrEmpty(options.PrivateKeyPem))
        {
            return null;
        }

        if (string.IsNullOrEmpty(options.KeyId))
        {
            throw new SigningKeyException(
                "SigningOptions.PrivateKeyPem requiere SigningOptions.KeyId: sin él, un verificador " +
                "no sabe qué clave pública usar (07-signing.md §4)");
        }

        return new Ed25519EventSigner(DecodePrivateKey(options.PrivateKeyPem), options.KeyId);
    }

    /// <summary>
    /// Construye el verificador, o <see langword="null"/> si el modo es
    /// <see cref="VerificationMode.Off"/>.
    /// </summary>
    /// <param name="options">Configuración de firma.</param>
    /// <returns>El verificador, o <see langword="null"/>.</returns>
    /// <exception cref="SigningKeyException">
    /// Se pide verificar sin claves públicas, o alguna no es una Ed25519 en SPKI.
    /// </exception>
    public static IEventVerifier? CreateVerifier(SigningOptions? options)
    {
        var mode = options?.Verify ?? VerificationMode.Off;
        if (options is null || mode == VerificationMode.Off)
        {
            return null;
        }

        var keys = new Dictionary<string, Ed25519PublicKeyParameters>(StringComparer.Ordinal);
        foreach (var (id, pem) in options.PublicKeys ?? new Dictionary<string, string>(StringComparer.Ordinal))
        {
            keys[id] = DecodePublicKey(id, pem);
        }

        if (keys.Count == 0)
        {
            throw new SigningKeyException(
                $"SigningOptions.Verify = {mode} requiere al menos una clave en " +
                "SigningOptions.PublicKeys. Incluye también las claves RETIRADAS mientras existan " +
                "eventos firmados con ellas: el mínimo son 90 días, la retención de la DLQ " +
                "(07-signing.md §6)");
        }

        return new Ed25519EventVerifier(keys, mode, options.OnWarn);
    }

    /// <summary>Genera un par Ed25519 en PEM. Comodidad para pruebas y para <c>flux keygen</c>.</summary>
    /// <returns>La privada en PKCS#8 y la pública en SPKI, ambas en PEM.</returns>
    public static (string PrivateKeyPem, string PublicKeyPem) GenerateKeyPair()
    {
        // La semilla sale del CSPRNG del sistema vía la BCL, no del SecureRandom de
        // BouncyCastle: menos superficie que revisar, y es la misma fuente que usan RSA y
        // ECDsa del propio framework.
        var seed = RandomNumberGenerator.GetBytes(KeyBytes);
        var priv = new Ed25519PrivateKeyParameters(seed, 0);
        var pub = priv.GeneratePublicKey();

        return (
            Pem("PRIVATE KEY", Pkcs8(seed)),
            Pem("PUBLIC KEY", Spki(pub.GetEncoded())));
    }

    // ─── Implementaciones ────────────────────────────────────────────────────

    private sealed class Ed25519EventSigner : IEventSigner
    {
        private readonly Ed25519PrivateKeyParameters _key;
        private readonly string _keyId;

        internal Ed25519EventSigner(Ed25519PrivateKeyParameters key, string keyId)
        {
            _key = key;
            _keyId = keyId;
        }

        public FluxEvent Sign(FluxEvent evento)
        {
            ArgumentNullException.ThrowIfNull(evento);

            // `signkeyid` va DENTRO de lo firmado: si quedara fuera, un atacante podría
            // cambiarlo por el id de una clave suya — 07-signing.md §5.
            var conKeyId = evento with { SignKeyId = _keyId, Signature = null };
            var payload = EventSigning.SignablePayload(conKeyId);

            var signer = new Ed25519Signer();
            signer.Init(true, _key); // true = firmar
            signer.BlockUpdate(payload, 0, payload.Length);

            // base64url SIN padding — 07-signing.md §4.
            return conKeyId with { Signature = ToBase64Url(signer.GenerateSignature()) };
        }
    }

    private sealed class Ed25519EventVerifier : IEventVerifier
    {
        private readonly IReadOnlyDictionary<string, Ed25519PublicKeyParameters> _keys;
        private readonly VerificationMode _mode;
        private readonly Action<string>? _onWarn;

        internal Ed25519EventVerifier(
            IReadOnlyDictionary<string, Ed25519PublicKeyParameters> keys,
            VerificationMode mode,
            Action<string>? onWarn)
        {
            _keys = keys;
            _mode = mode;
            _onWarn = onWarn;
        }

        public void Check(FluxEvent evento)
        {
            ArgumentNullException.ThrowIfNull(evento);

            if (string.IsNullOrEmpty(evento.Signature))
            {
                Fail(EventSigning.MissingSignature, $"evento {evento.Id} sin firma");
                return;
            }

            if (evento.SignKeyId is null || !_keys.TryGetValue(evento.SignKeyId, out var key))
            {
                // Una clave DESCONOCIDA no es lo mismo que una RETIRADA: si el id no está en
                // el mapa, el operador olvidó conservar la pública al rotar, o el evento
                // viene de fuera del ecosistema.
                Fail(
                    EventSigning.UnknownSigningKey,
                    $"evento {evento.Id} firmado con signkeyid=\"{evento.SignKeyId}\", que no está " +
                    "entre las claves conocidas. ¿Se retiró sin conservar la pública? Una clave " +
                    "retirada DEBE seguir verificando (07-signing.md §6)");
                return;
            }

            if (!Verify(key, EventSigning.SignablePayload(evento), FromBase64Url(evento.Signature)))
            {
                Fail(EventSigning.InvalidSignature, $"la firma del evento {evento.Id} no verifica");
            }
        }

        /// <summary>
        /// En <c>Require</c> lanza; en <c>Warn</c> registra y deja pasar.
        /// </summary>
        /// <remarks>
        /// <see cref="PoisonException"/> y no PERMANENT: un evento cuya firma no cuadra no
        /// es un evento que el handler deba ver, igual que uno cuyo JSON no parsea. Y es de
        /// los pocos casos que deben despertar a alguien — 04-errors.md §1.3.
        /// </remarks>
        private void Fail(string code, string message)
        {
            if (_mode == VerificationMode.Require)
            {
                throw new PoisonException(message, code: code);
            }

            var aviso = $"[flux] {code}: {message}";
            if (_onWarn is not null)
            {
                _onWarn(aviso);
            }
            else
            {
                Console.Error.WriteLine(aviso);
            }
        }

        private static bool Verify(Ed25519PublicKeyParameters key, byte[] payload, byte[] signature)
        {
            // BouncyCastle lanza si la firma no mide 64 bytes. Para el protocolo eso es el
            // mismo caso que una firma que no cuadra: la firma no verifica.
            if (signature.Length != SignatureBytes)
            {
                return false;
            }

            var verifier = new Ed25519Signer();
            verifier.Init(false, key); // false = verificar
            verifier.BlockUpdate(payload, 0, payload.Length);
            return verifier.VerifySignature(signature);
        }
    }

    // ─── Claves y codificaciones ─────────────────────────────────────────────

    private static Ed25519PrivateKeyParameters DecodePrivateKey(string pem)
    {
        AsymmetricKeyParameter key;
        try
        {
            key = PrivateKeyFactory.CreateKey(DecodePem(pem));
        }
        catch (Exception e) when (e is not SigningKeyException)
        {
            throw new SigningKeyException(
                "la clave privada no es un PEM PKCS#8 válido: " + e.Message, e);
        }

        // El protocolo NO negocia algoritmo a propósito: los formatos con algoritmo
        // negociable acumulan una familia de vulnerabilidades —desde `alg: none` hasta la
        // confusión HMAC/RSA— que solo existe porque hay algo que negociar (07-signing.md
        // §3). Una RSA se rechaza aquí, al arrancar, y no en la primera publicación.
        return key as Ed25519PrivateKeyParameters
               ?? throw new SigningKeyException(
                   $"la clave privada es {key.GetType().Name}, se esperaba una Ed25519. El protocolo " +
                   "no negocia algoritmo (07-signing.md §3)");
    }

    private static Ed25519PublicKeyParameters DecodePublicKey(string signKeyId, string pem)
    {
        AsymmetricKeyParameter key;
        try
        {
            key = PublicKeyFactory.CreateKey(DecodePem(pem));
        }
        catch (Exception e) when (e is not SigningKeyException)
        {
            throw new SigningKeyException(
                $"la clave pública \"{signKeyId}\" no es un PEM SPKI válido: " + e.Message, e);
        }

        return key as Ed25519PublicKeyParameters
               ?? throw new SigningKeyException(
                   $"la clave pública \"{signKeyId}\" es {key.GetType().Name}, se esperaba una Ed25519");
    }

    /// <summary>Quita la armadura PEM y decodifica el DER.</summary>
    private static byte[] DecodePem(string pem)
    {
        var base64 = new StringBuilder();
        foreach (var line in pem.Split('\n'))
        {
            var trimmed = line.Trim();
            if (trimmed.Length == 0 || trimmed.StartsWith("-----", StringComparison.Ordinal))
            {
                continue;
            }

            base64.Append(trimmed);
        }

        if (base64.Length == 0)
        {
            throw new SigningKeyException("el PEM no contiene ningún bloque base64");
        }

        return Convert.FromBase64String(base64.ToString());
    }

    private static string Pem(string label, byte[] der)
    {
        var body = Convert.ToBase64String(der);
        var sb = new StringBuilder();
        sb.Append("-----BEGIN ").Append(label).Append("-----\n");

        // Lineas de 64 caracteres: es lo que emiten OpenSSL, Node y Java, y lo que espera
        // cualquier herramienta que lea el PEM con una expresion regular por linea.
        for (var i = 0; i < body.Length; i += 64)
        {
            sb.Append(body, i, Math.Min(64, body.Length - i)).Append('\n');
        }

        sb.Append("-----END ").Append(label).Append("-----\n");
        return sb.ToString();
    }

    /// <summary>
    /// Envuelve una semilla Ed25519 de 32 bytes en un <c>PrivateKeyInfo</c> de PKCS#8.
    /// </summary>
    /// <remarks>
    /// El DER se escribe con el prefijo fijo de RFC 8410 §7 en vez de con las factorías de
    /// BouncyCastle. Para Ed25519 sin atributos la codificación es de longitud constante y
    /// no tiene ninguna variante, así que el prefijo ES la especificación:
    /// <code>
    /// 30 2e             SEQUENCE (46 bytes)          PrivateKeyInfo
    ///    02 01 00       INTEGER 0                    version v1
    ///    30 05          SEQUENCE (5)                 AlgorithmIdentifier
    ///       06 03 2b 65 70   OID 1.3.101.112         id-Ed25519
    ///    04 22          OCTET STRING (34)            privateKey
    ///       04 20 …     OCTET STRING (32)            CurvePrivateKey (la semilla)
    /// </code>
    /// Escribirlo aquí mantiene la superficie de API de BouncyCastle en dos funciones
    /// (<c>PrivateKeyFactory</c> y <c>PublicKeyFactory</c>) y hace el formato auditable
    /// leyendo el comentario. Lo que produce es idéntico byte a byte a lo que emiten
    /// <c>openssl genpkey -algorithm ed25519</c>, Node y Java.
    /// </remarks>
    private static byte[] Pkcs8(byte[] seed)
    {
        var der = new byte[]
        {
            0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70,
            0x04, 0x22, 0x04, 0x20,
        };
        return Concat(der, seed);
    }

    /// <summary>
    /// Envuelve una clave pública Ed25519 de 32 bytes en un <c>SubjectPublicKeyInfo</c>.
    /// </summary>
    /// <remarks>
    /// Mismo razonamiento que <see cref="Pkcs8"/>. RFC 8410 §4:
    /// <code>
    /// 30 2a             SEQUENCE (42 bytes)          SubjectPublicKeyInfo
    ///    30 05          SEQUENCE (5)                 AlgorithmIdentifier
    ///       06 03 2b 65 70   OID 1.3.101.112         id-Ed25519
    ///    03 21 00 …     BIT STRING (33, 0 bits sin usar) subjectPublicKey
    /// </code>
    /// </remarks>
    private static byte[] Spki(byte[] publicKey)
    {
        var der = new byte[]
        {
            0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
        };
        return Concat(der, publicKey);
    }

    private static byte[] Concat(byte[] prefix, byte[] tail)
    {
        var result = new byte[prefix.Length + tail.Length];
        Buffer.BlockCopy(prefix, 0, result, 0, prefix.Length);
        Buffer.BlockCopy(tail, 0, result, prefix.Length, tail.Length);
        return result;
    }

    /// <summary>base64url SIN padding — 07-signing.md §4.</summary>
    /// <remarks>
    /// A mano porque <c>System.Buffers.Text.Base64Url</c> llega en .NET 9 y el TFM de este
    /// SDK es net8.0, la LTS vigente.
    /// </remarks>
    private static string ToBase64Url(byte[] bytes) =>
        Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');

    /// <summary>
    /// Decodifica base64url, tolerando el padding aunque el protocolo no lo emita.
    /// </summary>
    /// <remarks>
    /// Emitir es estricto, leer es tolerante: rechazar un <c>=</c> de más convertiría en
    /// POISON un evento perfectamente verificable escrito por un productor con una librería
    /// menos cuidadosa. Un valor que no sea base64 en absoluto devuelve un array vacío, que
    /// el verificador trata como "la firma no verifica" en vez de propagar una excepción
    /// cruda del decodificador.
    /// </remarks>
    private static byte[] FromBase64Url(string value)
    {
        var normalized = value.TrimEnd('=').Replace('-', '+').Replace('_', '/');
        var padded = normalized.PadRight(normalized.Length + ((4 - (normalized.Length % 4)) % 4), '=');
        try
        {
            return Convert.FromBase64String(padded);
        }
        catch (FormatException)
        {
            return Array.Empty<byte>();
        }
    }
}
