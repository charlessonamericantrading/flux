// Seguridad de la conexión — lo que se comprueba SIN broker.
//
// Contrato normativo: specification/06-security.md §2 y §3, 04-errors.md §5.
// Lo que depende del broker (permisos exactos, TLS, credenciales aceptadas o rechazadas) lo cubre
// Integration/SecurityIntegrationTests.cs; aquí, la parte pura: validación de opciones ANTES de abrir
// ningún socket, interpretación de los `-ERR` del servidor y clasificación de lo que falla al conectar.

using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using Xunit;

namespace Flux.Tests;

public class SecurityTests
{
    // ─── inbox privado ───────────────────────────────────────────────────────

    [Fact]
    public void ElInboxEsPrivadoYSeDerivaDelNombreDelServicio()
    {
        Assert.Equal("_INBOX.facturacion-api", ConnectSecurity.InboxPrefixFor("facturacion-api"));
        // Nunca el `_INBOX.>` común, ni un prefijo de otro servicio.
        Assert.NotEqual("_INBOX", ConnectSecurity.InboxPrefixFor("a"));
        Assert.DoesNotContain('>', ConnectSecurity.InboxPrefixFor("a"));
    }

    [Theory]
    [InlineData("Facturacion")]
    [InlineData("fact.api")]
    [InlineData("")]
    [InlineData("a b")]
    public void UnNombreDeServicioQueNoEsUnTokenNoSePuedeUsarComoInbox(string servicio)
    {
        // Un punto haría que `_INBOX.a.b` fuera el inbox de `a` y a la vez un subject bajo `_INBOX.a.>`.
        Assert.Throws<InvalidServiceNameException>(() => ConnectSecurity.InboxPrefixFor(servicio));
    }

    // ─── denegaciones ────────────────────────────────────────────────────────

    [Fact]
    public void SeInterpretanLasDenegacionesQueEmiteElServidor()
    {
        // Cadenas literales observadas en nats-server 2.14.5 (A-210).
        Assert.Equal(
            new Denial(DeniedAction.Publish, "$JS.API.STREAM.INFO.EVT_PEDIDOS"),
            Denial.Parse("Permissions Violation for Publish to \"$JS.API.STREAM.INFO.EVT_PEDIDOS\""));
        Assert.Equal(
            new Denial(DeniedAction.Subscribe, "_INBOX.otro.>"),
            Denial.Parse("Permissions Violation for Subscription to \"_INBOX.otro.>\""));
        // Con comillas simples, tal como las manda el servidor en el protocolo.
        Assert.Equal(
            new Denial(DeniedAction.Publish, "x.y"),
            Denial.Parse("'Permissions Violation for Publish to \"x.y\"'"));
        // Con grupo de cola (una suscripción de quien responde).
        Assert.Equal(
            new Denial(DeniedAction.Subscribe, "rpc.clientes.cliente.v1.obtener"),
            Denial.Parse("Permissions Violation for Subscription to \"rpc.clientes.cliente.v1.obtener\" using queue \"crm-api\""));
    }

    [Theory]
    [InlineData("Authorization Violation")]
    [InlineData("Unknown Protocol Operation")]
    [InlineData("Maximum Connections Exceeded")]
    [InlineData("Permissions Violation for Other to \"x\"")]
    [InlineData("Permissions Violation for Publish to x")]
    [InlineData("")]
    [InlineData(null)]
    public void LoQueNoEsUnaDenegacionNoSeInterpretaComoTal(string? texto)
    {
        // Un `-ERR` que no es una denegación de permisos no puede convertirse en un PERMISSION_DENIED.
        Assert.Null(Denial.Parse(texto));
    }

    [Fact]
    public void ElSubjectDeUnAckYElDeUnaRespuestaSeNormalizanParaAvisarUnaVez()
    {
        Assert.Equal(
            "$JS.ACK.EVT_PEDIDOS.fact__x.>",
            new Denial(DeniedAction.Publish, "$JS.ACK.EVT_PEDIDOS.fact__x.1.1.1.1759000000.0").Normalized());
        Assert.Equal(
            "_INBOX.crm-api.>",
            new Denial(DeniedAction.Publish, "_INBOX.crm-api.AbCdEf.GhIjKl").Normalized());
        // Lo demás, tal cual.
        Assert.Equal(
            "$JS.API.CONSUMER.MSG.NEXT.EVT_X.d",
            new Denial(DeniedAction.Publish, "$JS.API.CONSUMER.MSG.NEXT.EVT_X.d").Normalized());
    }

    [Fact]
    public async Task UnaVigilanciaSeCompletaConLaPrimeraDenegacionRelevanteYSoloConEsa()
    {
        var log = new DenialLog { InboxPrefix = "_INBOX.svc" };
        using var vigila = log.StartWatch(d => d.Action == DeniedAction.Publish && d.Subject == "a.b");

        log.Record(new Denial(DeniedAction.Publish, "otro.subject"));
        log.Record(new Denial(DeniedAction.Subscribe, "a.b")); // el mismo subject pero suscribir: no
        Assert.False(vigila.Denied.IsCompleted);

        log.Record(new Denial(DeniedAction.Publish, "a.b"));
        Assert.Equal("a.b", (await vigila.Denied.WaitAsync(TimeSpan.FromSeconds(2))).Subject);
    }

    [Fact]
    public async Task UnaDenegacionAnteriorALaVigilanciaNoSeLeAchacaAEstaOperacion()
    {
        var log = new DenialLog { InboxPrefix = "_INBOX.svc" };
        log.Record(new Denial(DeniedAction.Publish, "a.b")); // antes de vigilar

        using var vigila = log.StartWatch(d => d.Subject == "a.b");
        await Task.Delay(50);
        Assert.False(vigila.Denied.IsCompleted);
    }

    [Fact]
    public void LaDenegacionDeLaSuscripcionAlInboxQuedaAnotadaParaSiempre()
    {
        var log = new DenialLog { InboxPrefix = "_INBOX.svc" };
        Assert.Null(log.InboxDenial);

        // La de un subject ajeno no es la del inbox.
        log.Record(new Denial(DeniedAction.Subscribe, "ajeno.>"));
        Assert.Null(log.InboxDenial);

        log.Record(new Denial(DeniedAction.Subscribe, "_INBOX.svc.NUID.*"));
        Assert.NotNull(log.InboxDenial);
        // Una publicación a un subject del inbox NO es la suscripción.
        var otro = new DenialLog { InboxPrefix = "_INBOX.svc" };
        otro.Record(new Denial(DeniedAction.Publish, "_INBOX.svc.NUID.x"));
        Assert.Null(otro.InboxDenial);
    }

    [Fact]
    public void ElAvisoSeDaUnaVezPorSubjectNormalizado()
    {
        var log = new DenialLog();
        Assert.True(log.FirstTime(new Denial(DeniedAction.Publish, "$JS.ACK.S.d.1.1.1.10.0")));
        Assert.False(log.FirstTime(new Denial(DeniedAction.Publish, "$JS.ACK.S.d.2.2.2.20.0")));
        Assert.True(log.FirstTime(new Denial(DeniedAction.Publish, "$JS.ACK.S.otro.1.1.1.10.0")));
        log.Forget("$JS.ACK.S.d.>");
        Assert.True(log.FirstTime(new Denial(DeniedAction.Publish, "$JS.ACK.S.d.3.3.3.30.0")));
    }

    /// <summary>
    /// El mecanismo entero depende de que NATS.Net escriba el `-ERR` en su ILogger con el valor `Error`
    /// (A-210). Este test fija la forma en que el SDK lo lee —con el valor estructurado y también con el
    /// texto formateado— y que lo que no es Error se ignora. El caso contra el broker real lo cubre
    /// Integration/SecurityIntegrationTests.cs: si una versión nueva de la librería cambia dónde lo
    /// escribe, ESE test falla.
    /// </summary>
    [Fact]
    public void ElLoggerQueSeLeDaALaLibreriaRecogeLosErrDelServidor()
    {
        var log = new DenialLog();
        var vistas = new List<Denial>();
        log.Recorded += vistas.Add;
        var logger = log.CreateLogger("NATS.Client.Core.NatsConnection");

        // Como lo escribe NATS.Net 2.5.16: `Server error {Error}`, nivel Error, EventId 1005.
        logger.Log(
            LogLevel.Error,
            new EventId(1005, "Protocol"),
            (IReadOnlyList<KeyValuePair<string, object?>>)new[]
            {
                new KeyValuePair<string, object?>("Error", "Permissions Violation for Publish to \"x.y\""),
                new KeyValuePair<string, object?>("{OriginalFormat}", "Server error {Error}"),
            },
            null,
            (_, _) => "Server error Permissions Violation for Publish to \"x.y\"");
        Assert.Equal(new[] { new Denial(DeniedAction.Publish, "x.y") }, vistas);

        // Una versión futura que cambie la plantilla: se lee del texto formateado.
        logger.Log(LogLevel.Error, new EventId(1), "otra forma", null,
            (_, _) => "El servidor dijo: Permissions Violation for Subscription to \"z.>\"");
        Assert.Equal(new Denial(DeniedAction.Subscribe, "z.>"), vistas[^1]);

        // Lo que no es Error (la librería llama a Log con niveles menores aunque IsEnabled diga que no) o no es
        // una denegación se ignora.
        var antes = vistas.Count;
        logger.Log(LogLevel.Debug, new EventId(1), "d", null,
            (_, _) => "Permissions Violation for Publish to \"x\"");
        logger.Log(LogLevel.Error, new EventId(1), "e", null, (_, _) => "Server error Authorization Violation");
        Assert.Equal(antes, vistas.Count);
        Assert.False(logger.IsEnabled(LogLevel.Debug));
        Assert.True(logger.IsEnabled(LogLevel.Error));
    }

    // ─── credenciales ────────────────────────────────────────────────────────

    [Fact]
    public void SinCredencialesNoSeAutenticaYCadaMetodoValidoSeTraduce()
    {
        Assert.True(ConnectSecurity.BuildAuth(null).IsAnonymous);
        Assert.True(ConnectSecurity.BuildAuth(new NatsCredentials()).IsAnonymous);

        Assert.Equal("t", ConnectSecurity.BuildAuth(new NatsCredentials { Token = "t" }).Token);

        var usuario = ConnectSecurity.BuildAuth(new NatsCredentials { User = "u", Password = "p" });
        Assert.Equal(("u", "p"), (usuario.Username, usuario.Password));

        var semilla = NKeys.CreateUserSeed();
        var nkey = ConnectSecurity.BuildAuth(new NatsCredentials { NkeySeed = "  " + semilla + "\n" });
        Assert.Equal(semilla, nkey.Seed);
        Assert.Equal(NKeys.PublicKeyFromSeed(semilla), nkey.NKey);

        var creds = ConnectSecurity.BuildAuth(new NatsCredentials
        {
            CredsContent = $"-----BEGIN NATS USER JWT-----\nhdr.pl.sig\n------END NATS USER JWT------\n\n-----BEGIN USER NKEY SEED-----\n{semilla}\n------END USER NKEY SEED------\n",
        });
        Assert.Equal("hdr.pl.sig", creds.Jwt);
        Assert.Equal(semilla, creds.Seed);
        // Se manda también la clave pública: un servidor con nkeys estáticas la exige y uno con JWT la ignora.
        Assert.Equal(NKeys.PublicKeyFromSeed(semilla), creds.NKey);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void UnValorVacioNoSeDegradaASinCredenciales(string vacio)
    {
        // Casi siempre es una variable de entorno sin definir.
        foreach (var c in new[]
                 {
                     new NatsCredentials { Token = vacio },
                     new NatsCredentials { CredsFile = vacio },
                     new NatsCredentials { CredsContent = vacio },
                     new NatsCredentials { NkeySeed = vacio },
                     new NatsCredentials { User = vacio, Password = "x" },
                     new NatsCredentials { User = "x", Password = vacio },
                 })
        {
            var e = Assert.Throws<InvalidCredentialsException>(() => ConnectSecurity.BuildAuth(c));
            Assert.Equal("INVALID_CREDENTIALS", e.FluxCode);
        }
    }

    [Fact]
    public void ElMensajeNoLlevaNuncaElValorDeUnaCredencial()
    {
        var semillaDeCuenta = NKeys.CreateAccountSeed();
        var e = Assert.Throws<InvalidCredentialsException>(
            () => ConnectSecurity.BuildAuth(new NatsCredentials { NkeySeed = semillaDeCuenta }));
        Assert.DoesNotContain(semillaDeCuenta, e.Message, StringComparison.Ordinal);
        Assert.Contains("SU", e.Message, StringComparison.Ordinal);

        var token = Assert.Throws<InvalidCredentialsException>(
            () => ConnectSecurity.BuildAuth(new NatsCredentials { Token = "t", User = "usuario-secreto", Password = "clave-secreta" }));
        Assert.DoesNotContain("clave-secreta", token.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("usuario-secreto", token.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void UnaSemillaQueNoEsDeUsuarioOQueEstaRotaSeRechazaSinRed()
    {
        Assert.Throws<InvalidCredentialsException>(() => ConnectSecurity.BuildAuth(new NatsCredentials { NkeySeed = NKeys.CreateOperatorSeed() }));
        Assert.Throws<InvalidCredentialsException>(() => ConnectSecurity.BuildAuth(new NatsCredentials { NkeySeed = "SUAAAA" }));
        Assert.Throws<InvalidCredentialsException>(() => ConnectSecurity.BuildAuth(new NatsCredentials { NkeySeed = "no-es-una-semilla" }));
    }

    // ─── TLS ─────────────────────────────────────────────────────────────────

    private static (string Cert, string Key) PemDePrueba(string dir, string nombre)
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=flux-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        var certPath = Path.Combine(dir, nombre + ".pem");
        var keyPath = Path.Combine(dir, nombre + ".key");
        File.WriteAllText(certPath, cert.ExportCertificatePem());
        File.WriteAllText(keyPath, rsa.ExportPkcs8PrivateKeyPem());
        return (certPath, keyPath);
    }

    [Fact]
    public void ConTlsSeExigeTlsYSeUsaLaCaIndicada()
    {
        Assert.Null(ConnectSecurity.BuildTls(null));

        var dir = Directory.CreateTempSubdirectory("flux-tls-").FullName;
        try
        {
            var (ca, _) = PemDePrueba(dir, "ca");
            var (cert, key) = PemDePrueba(dir, "cli");

            // `TlsOptions` vacío exige TLS con las CA del sistema.
            var vacio = ConnectSecurity.BuildTls(new TlsOptions())!;
            Assert.Equal(TlsMode.Require, vacio.Mode);
            Assert.Null(vacio.CaFile);

            var conTodo = ConnectSecurity.BuildTls(new TlsOptions { CaFile = ca, CertFile = cert, KeyFile = key })!;
            Assert.Equal(TlsMode.Require, conTodo.Mode); // NO baja a texto plano
            Assert.Equal(ca, conTodo.CaFile);
            Assert.Equal((cert, key), (conTodo.CertFile, conTodo.KeyFile));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void LasOpcionesTlsMalFormadasSeRechazanSinRed()
    {
        var dir = Directory.CreateTempSubdirectory("flux-tls-").FullName;
        try
        {
            var (ca, _) = PemDePrueba(dir, "ca");
            var (cert, key) = PemDePrueba(dir, "cli");
            var (_, otraClave) = PemDePrueba(dir, "otro");
            var basura = Path.Combine(dir, "basura.pem");
            File.WriteAllText(basura, "no es un PEM");

            var malas = new[]
            {
                new TlsOptions { CaFile = Path.Combine(dir, "no-existe.pem") },
                new TlsOptions { CaFile = basura },
                new TlsOptions { CaFile = "  " },
                new TlsOptions { CaFile = ca, CertFile = cert },
                new TlsOptions { CaFile = ca, KeyFile = key },
                new TlsOptions { CaFile = ca, CertFile = cert, KeyFile = basura },
                new TlsOptions { CaFile = ca, CertFile = basura, KeyFile = key },
                // La clave de OTRO certificado: no forman un par.
                new TlsOptions { CaFile = ca, CertFile = cert, KeyFile = otraClave },
            };

            foreach (var tls in malas)
            {
                var e = Assert.Throws<InvalidTlsOptionsException>(() => ConnectSecurity.BuildTls(tls));
                Assert.Equal("INVALID_TLS_OPTIONS", e.FluxCode);
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    // ─── qué falla en la primera conexión ────────────────────────────────────

    private static NatsException Envuelto(Exception causa) => new("can not start to connect nats server: nats://x:4222", causa);

    [Fact]
    public void UnaCredencialRechazadaEsConnectionRejectedAuth()
    {
        var e = ConnectSecurity.PermanentConnectError(Envuelto(new NatsServerException("Authorization Violation")), "nats://x:4222");
        Assert.NotNull(e);
        Assert.Equal(RejectionReason.Auth, e!.Reason);
        Assert.Equal("CONNECTION_REJECTED", e.FluxCode);

        // También cuando la autenticación caduca o se revoca.
        foreach (var texto in new[] { "User Authentication Expired", "User Authentication Revoked", "Account Authentication Expired" })
        {
            var otro = ConnectSecurity.PermanentConnectError(Envuelto(new NatsServerException(texto)), "nats://x:4222");
            Assert.Equal(RejectionReason.Auth, otro?.Reason);
        }
    }

    [Fact]
    public void LosFallosDeTlsSonConnectionRejectedTls()
    {
        // Certificado que no verifica (CA ausente, CA equivocada, host fuera de los SAN, caducado).
        var verifica = ConnectSecurity.PermanentConnectError(
            Envuelto(new NatsException("TLS authentication failed", new AuthenticationException("The remote certificate is invalid"))),
            "nats://x:4222");
        Assert.Equal(RejectionReason.Tls, verifica?.Reason);

        // Se exigió TLS y el servidor no lo ofrece.
        var sinTls = ConnectSecurity.PermanentConnectError(
            Envuelto(new NatsException("Server nats://x:4222 does not support TLS but TlsMode is set to Require")),
            "nats://x:4222");
        Assert.Equal(RejectionReason.Tls, sinTls?.Reason);
        Assert.Contains("no baja a texto plano", sinTls!.Message, StringComparison.Ordinal);

        // mTLS sin certificado de cliente: el servidor corta tras el handshake (TLS 1.3).
        var mtls = ConnectSecurity.PermanentConnectError(
            Envuelto(new IOException("The decryption operation failed, see inner exception.", new CryptographicException("tlsv13 alert certificate required"))),
            "nats://x:4222");
        Assert.Equal(RejectionReason.Tls, mtls?.Reason);
    }

    [Fact]
    public void UnBrokerQueAunNoEstaNoEsNingunRechazo()
    {
        // Las dos formas en que llega «no hay nadie»: sin causa (todos los servidores fallaron) y con un
        // SocketException. Y los errores del servidor que NO son de autenticación.
        foreach (var error in new Exception[]
                 {
                     new NatsException("can not connect uris: nats://x:4222"),
                     Envuelto(new SocketException((int)SocketError.ConnectionRefused)),
                     Envuelto(new TimeoutException()),
                     Envuelto(new NatsServerException("Maximum Connections Exceeded")),
                     Envuelto(new NatsServerException("Authentication Timeout")),
                     new InvalidOperationException("otra cosa"),
                 })
        {
            Assert.Null(ConnectSecurity.PermanentConnectError(error, "nats://x:4222"));
        }
    }

    [Fact]
    public void ElMensajeDelRechazoNombraAlBrokerSinSusCredenciales()
    {
        var e = ConnectSecurity.PermanentConnectError(
            Envuelto(new NatsServerException("Authorization Violation")),
            "nats://usuario:clave-secreta@broker-1:4222, tls://otro:tok@broker-2:4222");
        Assert.NotNull(e);
        Assert.Contains("broker-1:4222", e!.Message, StringComparison.Ordinal);
        Assert.Contains("broker-2:4222", e.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("clave-secreta", e.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("tok@", e.Message, StringComparison.Ordinal);
        Assert.Contains("Reintentar no lo arregla", e.Message, StringComparison.Ordinal);
    }

    // ─── lo que ConnectAsync comprueba antes de abrir un socket ──────────────

    [Fact]
    public async Task LasOpcionesMalFormadasFallanAntesDeAbrirNingunSocket()
    {
        // Nadie escucha en este puerto: si ConnectAsync intentase conectar, esperaría con jitter para
        // siempre. El WaitAsync lo haría fallar con un TimeoutException, que NO es lo que se espera.
        const string Servers = "nats://127.0.0.1:1";
        var credenciales = await Assert.ThrowsAsync<InvalidCredentialsException>(() =>
            FluxBus.ConnectAsync(new ConnectOptions
            {
                Servers = Servers, Service = "svc", Environment = "e", Version = "1",
                Credentials = new NatsCredentials { Token = "" },
            }).WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal("INVALID_CREDENTIALS", credenciales.FluxCode);

        var tls = await Assert.ThrowsAsync<InvalidTlsOptionsException>(() =>
            FluxBus.ConnectAsync(new ConnectOptions
            {
                Servers = Servers, Service = "svc", Environment = "e", Version = "1",
                Tls = new TlsOptions { CaFile = "/no/existe/ca.pem" },
            }).WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal("INVALID_TLS_OPTIONS", tls.FluxCode);

        // Un nombre de servicio inválido y una lista de servidores que no se entiende no son ninguno de los tres.
        await Assert.ThrowsAsync<InvalidServiceNameException>(() =>
            FluxBus.ConnectAsync(new ConnectOptions { Servers = Servers, Service = "Svc_X", Environment = "e", Version = "1" })
                .WaitAsync(TimeSpan.FromSeconds(5)));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            FluxBus.ConnectAsync(new ConnectOptions { Servers = "", Service = "svc", Environment = "e", Version = "1" })
                .WaitAsync(TimeSpan.FromSeconds(5)));
    }
}
