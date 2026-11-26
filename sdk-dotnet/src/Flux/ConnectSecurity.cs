// Traducción de las opciones de seguridad a NATS.Net, y reconocimiento de lo que falla.
// Contrato normativo: specification/06-security.md §2 y §3, 04-errors.md §5.3, 03-delivery.md §6.1
//
// Todo lo que se puede comprobar SIN red se comprueba aquí, ANTES de abrir ningún socket: un
// valor mal formado debe fallar con su nombre y no degradarse a «sin credenciales» o a «sin
// TLS». Las dos cosas se midieron contra nats-server 2.14.5 con NATS.Net 2.5.16 (A-211):
//
//   · una ruta de CA o de `.creds` inexistente NO falla rápido en la librería: un `.creds`
//     ausente lanza un FileNotFoundException suelto, y una CA ausente queda envuelta en el
//     mismo «can not start to connect nats server» que cualquier otro fallo del handshake,
//     indistinguible de un servidor caído;
//   · una CA que no es PEM no da ningún error: no carga ningún certificado y el handshake
//     falla después como «certificado rechazado», que apunta a otro sitio.

using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using NATS.Client.Core;

namespace Flux;

/// <summary>Opciones de seguridad ya validadas y traducidas a lo que entiende NATS.Net.</summary>
internal static class ConnectSecurity
{
    /// <summary>
    /// <c>_INBOX.&lt;servicio&gt;</c>. Las respuestas a las peticiones de JetStream —y las entregas
    /// de un pull consumer— viajan por el inbox. Con el <c>_INBOX.&gt;</c> común, un servicio que
    /// se suscribe ahí lee los eventos de dominios que no tiene concedidos (A-2, A-22).
    /// </summary>
    /// <remarks>
    /// No es una opción: lo deriva el SDK del nombre del servicio. Si fuera configurable, volver
    /// al <c>_INBOX.&gt;</c> común sería un valor por defecto olvidado — 06-security.md §3.3.
    /// </remarks>
    public static string InboxPrefixFor(string service)
    {
        // El servicio pasa a ser un token de subject, no solo parte de un nombre de durable.
        Protocol.ValidateService(service);
        return "_INBOX." + service;
    }

    // ─── Servidores ──────────────────────────────────────────────────────────

    /// <summary>
    /// Los servidores sin credenciales embebidas en la URL, para los mensajes de error: una
    /// URL <c>nats://usuario:clave@host</c> acabaría en un log.
    /// </summary>
    public static string RedactServers(string servers)
    {
        var partes = servers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        for (var i = 0; i < partes.Length; i++)
        {
            var arroba = partes[i].LastIndexOf('@');
            if (arroba >= 0)
            {
                var esquema = partes[i].IndexOf("://", StringComparison.Ordinal);
                partes[i] = (esquema >= 0 ? partes[i][..(esquema + 3)] : string.Empty) + partes[i][(arroba + 1)..];
            }
        }

        return string.Join(", ", partes);
    }

    // ─── Credenciales ────────────────────────────────────────────────────────

    /// <summary>
    /// Traduce <paramref name="credentials"/> a las opciones de autenticación de NATS.Net, o
    /// <see cref="NatsAuthOpts.Default"/> si no hay.
    /// </summary>
    /// <remarks>
    /// Falla en alto ante cualquier valor que la librería ignoraría o no podrá usar: un valor
    /// vacío (casi siempre una variable de entorno sin definir), <c>User</c> sin
    /// <c>Password</c>, más de un método a la vez (ambiguo: el servidor aceptaría uno y el otro
    /// sería ruido), un <c>.creds</c> ilegible o sin JWT y semilla, una semilla que no es de
    /// usuario (<c>SU…</c>).
    /// </remarks>
    /// <exception cref="InvalidCredentialsException">Alguna opción está mal formada.</exception>
    public static NatsAuthOpts BuildAuth(NatsCredentials? credentials)
    {
        if (credentials is null)
        {
            return NatsAuthOpts.Default;
        }

        var dadas = new List<string>();
        void Anota(string nombre, string? valor)
        {
            if (valor is null)
            {
                return;
            }

            if (string.IsNullOrWhiteSpace(valor))
            {
                // El valor vacío es casi siempre una variable de entorno sin definir. Aceptarlo
                // degradaría a «sin credenciales», que es justo lo que el desarrollador no quería.
                throw new InvalidCredentialsException($"`Credentials.{nombre}` debe ser un texto no vacío");
            }

            dadas.Add(nombre);
        }

        Anota(nameof(NatsCredentials.CredsFile), credentials.CredsFile);
        Anota(nameof(NatsCredentials.CredsContent), credentials.CredsContent);
        Anota(nameof(NatsCredentials.NkeySeed), credentials.NkeySeed);
        Anota(nameof(NatsCredentials.Token), credentials.Token);
        Anota(nameof(NatsCredentials.User), credentials.User);
        Anota(nameof(NatsCredentials.Password), credentials.Password);

        if (dadas.Count == 0)
        {
            return NatsAuthOpts.Default;
        }

        if ((credentials.User is not null) != (credentials.Password is not null))
        {
            throw new InvalidCredentialsException("`User` y `Password` van juntos");
        }

        var metodos = new List<string>();
        if (credentials.CredsFile is not null) { metodos.Add("CredsFile"); }
        if (credentials.CredsContent is not null) { metodos.Add("CredsContent"); }
        if (credentials.NkeySeed is not null) { metodos.Add("NkeySeed"); }
        if (credentials.Token is not null) { metodos.Add("Token"); }
        if (credentials.User is not null) { metodos.Add("User+Password"); }

        if (metodos.Count > 1)
        {
            // Combinarlos es ambiguo: el servidor aceptaría uno y el otro quedaría como ruido.
            throw new InvalidCredentialsException($"elige UN método y no {string.Join(" + ", metodos)}");
        }

        if (credentials.CredsFile is { } ruta)
        {
            string texto;
            try
            {
                texto = File.ReadAllText(ruta);
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException)
            {
                throw new InvalidCredentialsException(
                    $"`CredsFile` apunta a \"{ruta}\", que no se puede leer ({e.GetType().Name})", e);
            }

            var (jwt, semilla) = ParseCreds($"el fichero \"{ruta}\"", texto);
            return new NatsAuthOpts { Jwt = jwt, Seed = semilla, NKey = NKeys.PublicKeyFromSeed(semilla) };
        }

        if (credentials.CredsContent is { } contenido)
        {
            var (jwt, semilla) = ParseCreds("`CredsContent`", contenido);
            return new NatsAuthOpts { Jwt = jwt, Seed = semilla, NKey = NKeys.PublicKeyFromSeed(semilla) };
        }

        if (credentials.NkeySeed is { } nkey)
        {
            ValidateUserSeed("`NkeySeed`", nkey.Trim());
            return new NatsAuthOpts { Seed = nkey.Trim(), NKey = NKeys.PublicKeyFromSeed(nkey.Trim()) };
        }

        if (credentials.Token is { } token)
        {
            return new NatsAuthOpts { Token = token };
        }

        return new NatsAuthOpts { Username = credentials.User, Password = credentials.Password };
    }

    /// <summary>Extrae el JWT y la semilla de un <c>.creds</c> y comprueba que se pueden usar.</summary>
    private static (string Jwt, string Seed) ParseCreds(string origen, string texto)
    {
        string? jwt = null;
        string? semilla = null;
        var lineas = texto.Split('\n');
        for (var i = 0; i < lineas.Length - 1; i++)
        {
            var linea = lineas[i].Trim();
            if (linea.StartsWith("-----BEGIN NATS USER JWT-----", StringComparison.Ordinal))
            {
                jwt = lineas[i + 1].Trim();
            }
            else if (linea.StartsWith("-----BEGIN USER NKEY SEED-----", StringComparison.Ordinal))
            {
                semilla = lineas[i + 1].Trim();
            }
        }

        if (string.IsNullOrEmpty(jwt))
        {
            throw new InvalidCredentialsException($"{origen} no contiene un JWT de usuario (bloque «NATS USER JWT»)");
        }

        if (string.IsNullOrEmpty(semilla))
        {
            throw new InvalidCredentialsException($"{origen} no contiene la semilla de usuario (bloque «USER NKEY SEED»)");
        }

        ValidateUserSeed(origen, semilla);
        return (jwt, semilla);
    }

    /// <summary>
    /// Comprueba que es una semilla nkey <b>de usuario</b> (<c>SU…</c>) bien formada. Nunca
    /// incluye la semilla en el mensaje: acaba en un log.
    /// </summary>
    private static void ValidateUserSeed(string origen, string semilla)
    {
        if (!semilla.StartsWith("SU", StringComparison.Ordinal))
        {
            // Una semilla de cuenta u operador (`SA…`, `SO…`) se parsea bien y el broker la
            // rechazaría después con un `authorization violation` sin explicación.
            throw new InvalidCredentialsException(
                $"{origen}: la semilla debe ser la de una nkey de USUARIO (empieza por «SU»)");
        }

        try
        {
            using var par = NKeys.FromSeed(semilla);
            if (!NKeys.PublicKeyFromSeed(semilla).StartsWith('U'))
            {
                throw new InvalidCredentialsException($"{origen}: la semilla no es de usuario");
            }
        }
        catch (Exception e) when (e is not InvalidCredentialsException)
        {
            throw new InvalidCredentialsException(
                $"{origen}: la semilla nkey no es válida (se omite el valor a propósito: acabaría en un log)", e);
        }
    }

    // ─── TLS ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Traduce <paramref name="tls"/> a las opciones TLS de NATS.Net, o <see langword="null"/> si
    /// no se pidió TLS. Con un <see cref="TlsOptions"/> el modo es <see cref="TlsMode.Require"/>:
    /// NO baja a texto plano si el servidor no ofrece TLS.
    /// </summary>
    /// <exception cref="InvalidTlsOptionsException">Alguna opción está mal formada.</exception>
    public static NatsTlsOpts? BuildTls(TlsOptions? tls)
    {
        if (tls is null)
        {
            return null;
        }

        Vacia(nameof(TlsOptions.CaFile), tls.CaFile);
        Vacia(nameof(TlsOptions.CertFile), tls.CertFile);
        Vacia(nameof(TlsOptions.KeyFile), tls.KeyFile);

        if ((tls.CertFile is null) != (tls.KeyFile is null))
        {
            throw new InvalidTlsOptionsException(
                "`CertFile` y `KeyFile` van juntos (certificado de cliente para mTLS): con uno solo no hay " +
                "certificado que presentar y se ignoraría sin decir nada");
        }

        if (tls.CaFile is { } ca)
        {
            var texto = Lee("CaFile", ca);
            try
            {
                var certificados = new X509Certificate2Collection();
                certificados.ImportFromPem(texto);
                if (certificados.Count == 0)
                {
                    throw new InvalidTlsOptionsException(
                        $"`CaFile` \"{ca}\" no contiene ningún certificado PEM (`-----BEGIN CERTIFICATE-----`). " +
                        "Un fichero equivocado —o una CA en DER— haría fallar el handshake con un error que no lo dice");
                }
            }
            catch (CryptographicException e)
            {
                throw new InvalidTlsOptionsException($"`CaFile` \"{ca}\" no es un PEM legible", e);
            }
        }

        if (tls.CertFile is { } cert && tls.KeyFile is { } clave)
        {
            var textoCert = Lee("CertFile", cert);
            var textoClave = Lee("KeyFile", clave);
            if (!textoClave.Contains("PRIVATE KEY-----", StringComparison.Ordinal))
            {
                throw new InvalidTlsOptionsException(
                    $"`KeyFile` \"{clave}\" no parece una clave privada PEM (no contiene `PRIVATE KEY-----`)");
            }

            try
            {
                using var par = X509Certificate2.CreateFromPem(textoCert, textoClave);
            }
            catch (CryptographicException e)
            {
                throw new InvalidTlsOptionsException(
                    $"`CertFile` \"{cert}\" y `KeyFile` \"{clave}\" no forman un par PEM legible " +
                    "(¿la clave es de otro certificado, o está cifrada?)", e);
            }
        }

        return new NatsTlsOpts
        {
            Mode = TlsMode.Require,
            CaFile = tls.CaFile,
            CertFile = tls.CertFile,
            KeyFile = tls.KeyFile,
        };

        static void Vacia(string nombre, string? valor)
        {
            if (valor is not null && string.IsNullOrWhiteSpace(valor))
            {
                throw new InvalidTlsOptionsException($"`{nombre}` debe ser una ruta no vacía");
            }
        }

        static string Lee(string nombre, string ruta)
        {
            try
            {
                return File.ReadAllText(ruta);
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException)
            {
                throw new InvalidTlsOptionsException(
                    $"`{nombre}` apunta a \"{ruta}\", que no se puede leer ({e.GetType().Name})", e);
            }
        }
    }

    // ─── Qué falla en la primera conexión ────────────────────────────────────

    /// <summary>
    /// Un fallo de la PRIMERA conexión que esperar no arregla, ya explicado; o
    /// <see langword="null"/> si es transitorio (servidor caído, DNS, timeout) y merece otro
    /// intento — 03-delivery.md §6.1.
    /// </summary>
    /// <remarks>
    /// NATS.Net 2.5.16 NO se cuelga ni se queda mudo en la primera conexión (A-211): no reintenta
    /// nada por su cuenta y falla en milisegundos, con un <see cref="NatsException"/> que lleva la
    /// causa dentro. Lo que no hace es distinguir «el broker aún no está» de «esto no cambia por
    /// esperar», y esa distinción es de quien escribe el bucle de espera. Observado:
    /// <code>
    /// broker sin arrancar          NatsException("can not connect uris: …")                    sin causa
    /// credencial rechazada         NatsException ← NatsServerException(IsAuthError)
    /// CA ausente o equivocada      NatsException ← NatsException("TLS authentication failed") ← AuthenticationException
    /// host fuera de los SAN        la misma cadena
    /// TLS exigido, servidor sin    NatsException("Server … does not support TLS but TlsMode is set to Require")
    /// mTLS sin certificado         NatsException ← SocketClosedException ← IOException ← SslException ← «alert certificate required»
    /// </code>
    /// </remarks>
    public static ConnectionRejectedException? PermanentConnectError(Exception error, string servers)
    {
        var broker = RedactServers(servers);

        for (var e = error; e is not null; e = e.InnerException)
        {
            // La credencial: el servidor dijo `Authorization Violation` (o la autenticación
            // caducó o se revocó). Reintentar da la misma respuesta.
            if (e is NatsServerException { IsAuthError: true } auth)
            {
                return new ConnectionRejectedException(
                    RejectionReason.Auth,
                    $"el broker {broker} rechazó las credenciales ({auth.Error}). Reintentar no lo arregla: revisa " +
                    "`Credentials` y que el usuario exista en la account del servicio (06-security.md §2).",
                    error);
            }
        }

        // Se pidió TLS y el servidor no lo ofrece. La librería NO degrada a texto plano (bien),
        // pero lo comunica como un NatsException más.
        for (var e = error; e is not null; e = e.InnerException)
        {
            if (e is NatsException && e.Message.Contains("does not support TLS", StringComparison.Ordinal))
            {
                return new ConnectionRejectedException(
                    RejectionReason.Tls,
                    $"se pidió TLS y el broker {broker} no lo ofrece. Reintentar no lo arregla, y el SDK no baja a " +
                    "texto plano: habilita TLS en el listener del broker (06-security.md §2) o quita `Tls`.",
                    error);
            }
        }

        for (var e = error; e is not null; e = e.InnerException)
        {
            // El handshake TLS: el certificado no verifica (CA ausente o equivocada, host fuera de los
            // SAN, caducado), o —en mTLS— el broker corta porque no se presentó certificado de cliente.
            if (e is AuthenticationException || EsErrorDeTls(e))
            {
                return new ConnectionRejectedException(
                    RejectionReason.Tls,
                    $"el handshake TLS con el broker {broker} falló ({Causa(e)}). Reintentar no lo arregla. Causas " +
                    "posibles: el certificado del servidor no verifica (si usa una CA propia, indícala en " +
                    "`Tls.CaFile`), el certificado no cubre el host al que te conectas (revisa sus SAN), el " +
                    "broker no ofrece TLS, o exige certificado de cliente (`Tls.CertFile` y `Tls.KeyFile`) " +
                    "(06-security.md §2).",
                    error);
            }
        }

        return null;
    }

    /// <summary>
    /// Un error de la capa TLS que no es un <see cref="AuthenticationException"/>: es lo que llega
    /// cuando el servidor corta tras el handshake (mTLS sin certificado de cliente en TLS 1.3).
    /// </summary>
    private static bool EsErrorDeTls(Exception e) =>
        e is CryptographicException ||
        e.GetType().Name is "SslException" or "OpenSslCryptographicException" ||
        (e is System.IO.IOException && e.Message.Contains("decryption operation failed", StringComparison.OrdinalIgnoreCase));

    /// <summary>La causa más específica de la cadena, para el mensaje. No lleva credenciales.</summary>
    private static string Causa(Exception e)
    {
        var interna = e;
        while (interna.InnerException is not null)
        {
            interna = interna.InnerException;
        }

        return interna.Message.Replace('\n', ' ').Trim();
    }
}
