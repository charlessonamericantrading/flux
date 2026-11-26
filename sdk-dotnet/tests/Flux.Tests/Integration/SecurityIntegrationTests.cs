// Seguridad ejecutada contra un nats-server REAL — plan v1.1, Fase 1 (y 6, para .NET).
//
// Estos tests arrancan su propio broker con la ACL que genera `scripts/gen-acl.mjs` y los streams y
// durables de `flux provision`. Cada comprobación va en los DOS sentidos: lo concedido funciona y lo
// no concedido falla. Un test que solo se ha visto en verde no demuestra que el broker confine.
//
//   NATS_SERVER=/ruta/a/nats-server FLUX_REQUIRE_BROKER=1 dotnet test --filter SecurityIntegration
//
// Los códigos de la capacidad `security` se comprueban por su literal en `LosCincoCodigos` —
// NOT_PROVISIONED, PERMISSION_DENIED, CONNECTION_REJECTED, INVALID_CREDENTIALS e INVALID_TLS_OPTIONS—,
// que es lo que `scripts/check-sdk-codes.mjs` exige de la evidencia.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Xunit;

namespace Flux.Tests.Integration;

[Collection("broker")]
public class SecurityIntegrationTests
{
    private const string Subject = "pedidos.pedido.v1.creado";
    private const string Stream = "EVT_PEDIDOS";

    private static string Durable(string servicio) => $"{servicio}__pedidos_pedido_v1_creado";

    private static readonly TimeSpan Rapido = TimeSpan.FromSeconds(3);

    // ─── utilidades ──────────────────────────────────────────────────────────

    private sealed class Recibidos
    {
        public ConcurrentQueue<(FluxEvent Evento, Delivery Entrega)> Cola { get; } = new();

        public FluxHandler Handler => (e, d, _) =>
        {
            Cola.Enqueue((e, d));
            return Task.CompletedTask;
        };
    }

    private static async Task<bool> EsperarAsync(Func<bool> condicion, TimeSpan hasta)
    {
        var limite = DateTime.UtcNow + hasta;
        while (DateTime.UtcNow < limite)
        {
            if (condicion())
            {
                return true;
            }

            await Task.Delay(25);
        }

        return condicion();
    }

    private static async Task<T> FallaAsync<T>(Func<Task> operacion)
        where T : Exception
    {
        var t0 = Stopwatch.StartNew();
        // Un tope duro: un SDK que esperase para siempre (que es lo que se mide aquí) colgaría la suite en
        // vez de hacerla fallar.
        var e = await Assert.ThrowsAnyAsync<Exception>(() => operacion().WaitAsync(TimeSpan.FromSeconds(20)));
        Assert.True(e is not TimeoutException, "la operación no terminó en 20 s: espera para siempre en vez de fallar");
        Assert.True(t0.Elapsed < Rapido, $"tardó {t0.ElapsedMilliseconds} ms: parece que espera un timeout en vez de fallar");
        return Assert.IsType<T>(e);
    }

    private static async Task<FluxBus> ConectaAsync(Entorno env, string servicio, CapturingLogger? log = null) =>
        await FluxBus.ConnectAsync(env.Opciones(servicio, log));

    private static object Cuerpo(int n = 1) => new { pedidoId = "ped-" + n, clienteId = "cli-1", totalCents = 100, moneda = "EUR" };

    // ═════════════════════════════════════════════════════════════════════════
    //  A — extremo a extremo con inbox privado y permisos exactos
    // ═════════════════════════════════════════════════════════════════════════

    [BrokerFact(node: true)]
    public async Task A_ExtremoAExtremoConInboxPrivadoYPermisosExactos()
    {
        await using var env = await Entorno.CrearAsync();
        var log = new CapturingLogger();
        await using var productor = await ConectaAsync(env, "pedidos-api");
        await using var consumidor = await ConectaAsync(env, "facturacion-api", log);

        var recibidos = new Recibidos();
        await using var suscripcion = await consumidor.SubscribeAsync(Subject, recibidos.Handler);
        Assert.Equal(Durable("facturacion-api"), suscripcion.Durable);

        var publicado = await productor.PublishAsync(Subject, Cuerpo(), new PublishOptions { AggregateId = "ped-1" });

        Assert.True(await EsperarAsync(() => !recibidos.Cola.IsEmpty, TimeSpan.FromSeconds(10)), "el evento no llegó");
        Assert.True(recibidos.Cola.TryPeek(out var recibido));
        Assert.Equal(publicado.Id, recibido.Evento.Id);
        Assert.Equal(1, recibido.Entrega.Attempt);

        // El ack se confirmó: no queda nada pendiente de confirmar en el consumidor.
        var (nc, js) = await env.AbrirOpsAsync();
        await using (nc)
        {
            Assert.True(
                await EsperarAsync(
                    () => js.GetConsumerAsync(Stream, Durable("facturacion-api")).AsTask().GetAwaiter().GetResult()
                        .Info is { NumAckPending: 0, AckFloor.StreamSeq: >= 1 },
                    TimeSpan.FromSeconds(5)),
                "el evento se procesó pero no se confirmó (¿permiso de ack?)");
        }

        // El broker no anotó NINGUNA violación: con permisos exactos y el inbox privado, todo lo que
        // el SDK hace está concedido.
        Assert.Empty(env.Violaciones());
        Assert.Empty(log.Errors);
    }

    [BrokerFact(node: true)]
    public async Task A_UnEventoPublicadoEnUnDominioAjenoSeDeniegaRapidoConPermissionDenied()
    {
        await using var env = await Entorno.CrearAsync();
        await using var intruso = await ConectaAsync(env, "inventario-api");

        // `inventario-api` es dueño de `inventario`, no de `pedidos`: el broker lo deniega, y sin el
        // SDK esto sería un timeout de 10 s sin una palabra de permisos.
        var e = await FallaAsync<PermissionDeniedException>(
            () => intruso.PublishAsync(Subject, Cuerpo(), new PublishOptions { AggregateId = "ped-1" }));

        Assert.Equal("PERMISSION_DENIED", e.FluxCode);
        Assert.Equal(ErrorClass.Permanent, e.FluxClass);
        Assert.Equal(DeniedOperation.Publish, e.Operation);
        Assert.Equal(Subject, e.DeniedSubject);
        Assert.Contains($"\"{Subject}\"", e.Message, StringComparison.Ordinal);
        Assert.Contains("inventario-api", e.Message, StringComparison.Ordinal);
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  B — la ACL antigua falla de forma clara y rápida
    // ═════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Los permisos de `facturacion-api` tal cual los generaba `scripts/gen-acl.mjs` v1: sin una línea de
    /// `$JS.API.*`, y con el `_INBOX.>` común.
    /// </summary>
    private static readonly Perms AclAntigua = new(
        new[] { "facturacion.>", "dlq.pedidos.pedido.v1.>" },
        new[] { "pedidos.pedido.v1.>", "dlq.facturacion.>", "_INBOX.>" },
        null);

    [BrokerFact(node: true)]
    public async Task B_LaAclAntiguaFallaDeFormaClaraYRapida()
    {
        await using var env = await Entorno.CrearAsync(
            (nombre, p) => nombre == "facturacion-api" ? AclAntigua : p);
        await using var bus = await ConectaAsync(env, "facturacion-api");

        var e = await FallaAsync<PermissionDeniedException>(
            () => bus.SubscribeAsync(Subject, new Recibidos().Handler));

        Assert.Equal("PERMISSION_DENIED", e.FluxCode);
        // Nombra el subject denegado, completo, y dice dónde se arregla.
        Assert.Equal("$JS.API.STREAM.INFO." + Stream, e.DeniedSubject);
        Assert.Contains("gen-acl.mjs", e.Message, StringComparison.Ordinal);
        Assert.Contains("06-security.md", e.Message, StringComparison.Ordinal);
    }

    [BrokerFact(node: true)]
    public async Task B_ConPermisosExactosLaMismaOperacionFunciona()
    {
        await using var env = await Entorno.CrearAsync();
        await using var bus = await ConectaAsync(env, "facturacion-api");
        await using var suscripcion = await bus.SubscribeAsync(Subject, new Recibidos().Handler);
        Assert.Equal(Subject, suscripcion.Subject);
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  C — NOT_PROVISIONED
    // ═════════════════════════════════════════════════════════════════════════

    private static void AssertNoAprovisionado(NotProvisionedException e, ResourceKind tipo, string nombre)
    {
        Assert.Equal("NOT_PROVISIONED", e.FluxCode);
        Assert.Equal(ErrorClass.Permanent, e.FluxClass);
        Assert.Equal(tipo, e.Kind);
        Assert.Equal(nombre, e.Name);
        // El mensaje nombra el recurso, el subject denegado y lo que lo arregla.
        Assert.Contains(nombre, e.Message, StringComparison.Ordinal);
        Assert.Contains("flux provision", e.Message, StringComparison.Ordinal);
        Assert.StartsWith("$JS.API.", e.DeniedSubject, StringComparison.Ordinal);
        Assert.Contains(e.DeniedSubject, e.Message, StringComparison.Ordinal);
    }

    [BrokerFact(node: true)]
    public async Task C_StreamNoAprovisionadoYSinPermisoDeCrearEsNotProvisioned()
    {
        // NADIE aprovisiona nada.
        await using var env = await Entorno.CrearAsync(aprovisionar: false);
        await using var consumidor = await ConectaAsync(env, "facturacion-api");

        var e = await FallaAsync<NotProvisionedException>(
            () => consumidor.SubscribeAsync(Subject, new Recibidos().Handler));
        AssertNoAprovisionado(e, ResourceKind.Stream, Stream);
        Assert.Equal("$JS.API.STREAM.CREATE." + Stream, e.DeniedSubject);

        // Publicar en un dominio sin aprovisionar es lo mismo, y no un timeout.
        await using var productor = await ConectaAsync(env, "pedidos-api");
        var p = await FallaAsync<NotProvisionedException>(
            () => productor.PublishAsync(Subject, Cuerpo()));
        AssertNoAprovisionado(p, ResourceKind.Stream, Stream);
    }

    [BrokerFact(node: true)]
    public async Task C_ConsumidorNoAprovisionadoYSinPermisoDeCrearEsNotProvisioned()
    {
        await using var env = await Entorno.CrearAsync(aprovisionar: false);
        var (nc, js) = await env.AbrirOpsAsync();
        await using (nc)
        {
            // Los streams existen; el consumidor, no.
            await js.CreateStreamAsync(new NATS.Client.JetStream.Models.StreamConfig(Stream, new[] { "pedidos.>" }));
            await js.CreateStreamAsync(new NATS.Client.JetStream.Models.StreamConfig("DLQ_PEDIDOS", new[] { "dlq.pedidos.>" }));
        }

        await using var bus = await ConectaAsync(env, "facturacion-api");
        var e = await FallaAsync<NotProvisionedException>(() => bus.SubscribeAsync(Subject, new Recibidos().Handler));
        AssertNoAprovisionado(e, ResourceKind.Consumer, Durable("facturacion-api"));
        Assert.Equal(Stream, e.Stream);
        Assert.StartsWith("$JS.API.CONSUMER.CREATE." + Stream + "." + Durable("facturacion-api"), e.DeniedSubject, StringComparison.Ordinal);
    }

    [BrokerFact(node: true)]
    public async Task C_ConsumidorAprovisionadoSeUsaSinPermisoDeCrear_InfoAntesDeCrear()
    {
        // Producción: `flux provision` lo creó y la ACL NO concede CONSUMER.CREATE. El SDK pregunta
        // primero (CONSUMER.INFO) y no intenta crear: ni una violación en el broker.
        await using var env = await Entorno.CrearAsync();
        Assert.DoesNotContain(env.Permisos["facturacion-api"].Publish, p => p.StartsWith("$JS.API.CONSUMER.CREATE", StringComparison.Ordinal));

        await using var bus = await ConectaAsync(env, "facturacion-api");
        await using var suscripcion = await bus.SubscribeAsync(Subject, new Recibidos().Handler);

        Assert.Empty(env.Violaciones());
    }

    [BrokerFact(node: true)]
    public async Task C_SinPermisoDeConsultarElConsumidorEsPermissionDeniedNoNotProvisioned()
    {
        // Sin CONSUMER.INFO no se sabe si el consumidor falta: NO es NOT_PROVISIONED (04-errors.md §5).
        await using var env = await Entorno.CrearAsync(
            (nombre, p) => nombre == "facturacion-api"
                ? p.SinPublicar(s => s.StartsWith("$JS.API.CONSUMER.INFO.EVT_PEDIDOS", StringComparison.Ordinal))
                : p);
        await using var bus = await ConectaAsync(env, "facturacion-api");

        var e = await FallaAsync<PermissionDeniedException>(() => bus.SubscribeAsync(Subject, new Recibidos().Handler));
        Assert.StartsWith("$JS.API.CONSUMER.INFO." + Stream, e.DeniedSubject, StringComparison.Ordinal);
    }

    [BrokerFact(node: true)]
    public async Task C_ConPermisoDeCrearElSdkAprovisionaElConsumidor_ModoDesarrollo()
    {
        // `gen-acl.mjs --dev` concede CONSUMER.CREATE del durable propio. Los streams los crea quien
        // opera (el SDK nunca tiene STREAM.CREATE): aquí, `ops`.
        await using var env = await Entorno.CrearAsync(aprovisionar: false, genExtra: "--dev");
        Assert.Contains(env.Permisos["facturacion-api"].Publish, p => p.StartsWith("$JS.API.CONSUMER.CREATE." + Stream, StringComparison.Ordinal));
        var (nc, js) = await env.AbrirOpsAsync();
        await using (nc)
        {
            await js.CreateStreamAsync(new NATS.Client.JetStream.Models.StreamConfig(Stream, new[] { "pedidos.>" }));
            await js.CreateStreamAsync(new NATS.Client.JetStream.Models.StreamConfig("DLQ_PEDIDOS", new[] { "dlq.pedidos.>" }));

            await using var bus = await ConectaAsync(env, "facturacion-api");
            await using var suscripcion = await bus.SubscribeAsync(Subject, new Recibidos().Handler);

            var consumidor = await js.GetConsumerAsync(Stream, Durable("facturacion-api"));
            Assert.Equal(Subject, consumidor.Info.Config.FilterSubject);
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  D — el pull denegado hace fallar subscribe() (06-security.md §3.7: DEBE)
    // ═════════════════════════════════════════════════════════════════════════

    private static Perms SinPull(Perms p) =>
        p.SinPublicar(s => s.StartsWith("$JS.API.CONSUMER.MSG.NEXT." + Stream, StringComparison.Ordinal));

    [BrokerFact(node: true)]
    public async Task D_SinPermisoDePullSubscribeFallaConPermissionDenied()
    {
        await using var env = await Entorno.CrearAsync(
            (nombre, p) => nombre == "facturacion-api" ? SinPull(p) : p);
        var log = new CapturingLogger();
        await using var bus = await ConectaAsync(env, "facturacion-api", log);

        var e = await FallaAsync<PermissionDeniedException>(() => bus.SubscribeAsync(Subject, new Recibidos().Handler));

        // Nombra el subject denegado, COMPLETO.
        Assert.Equal($"$JS.API.CONSUMER.MSG.NEXT.{Stream}.{Durable("facturacion-api")}", e.DeniedSubject);
        Assert.Contains(e.DeniedSubject, e.Message, StringComparison.Ordinal);
        Assert.Contains("inerte", e.Message, StringComparison.Ordinal);

        // Y además lo registra, como cualquier otra denegación.
        Assert.True(
            await EsperarAsync(() => log.Errors.Any(l => l.Contains(e.DeniedSubject, StringComparison.Ordinal)), TimeSpan.FromSeconds(3)),
            "la denegación no quedó en el logger: " + string.Join(" | ", log.Lines));
    }

    [BrokerFact(node: true)]
    public async Task D_ConElPermisoDePullSubscribeFuncionaYLaSondaNoSeLlevaNingunMensaje()
    {
        await using var env = await Entorno.CrearAsync();
        await using var productor = await ConectaAsync(env, "pedidos-api");
        await using var consumidor = await ConectaAsync(env, "facturacion-api");

        // El evento ya está en el stream ANTES de suscribirse: la sonda del pull no debe llevárselo.
        var publicado = await productor.PublishAsync(Subject, Cuerpo(), new PublishOptions { AggregateId = "ped-1" });

        var recibidos = new Recibidos();
        await using var suscripcion = await consumidor.SubscribeAsync(Subject, recibidos.Handler);

        Assert.True(await EsperarAsync(() => !recibidos.Cola.IsEmpty, TimeSpan.FromSeconds(10)), "el evento no llegó");
        await Task.Delay(300);
        Assert.Single(recibidos.Cola);
        Assert.True(recibidos.Cola.TryPeek(out var recibido));
        Assert.Equal(publicado.Id, recibido.Evento.Id);
        // Primera entrega: si la sonda se hubiera llevado el mensaje, llegaría como intento 2.
        Assert.Equal(1, recibido.Entrega.Attempt);
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  E — el ack denegado solo deja rastro en el logger
    // ═════════════════════════════════════════════════════════════════════════

    [BrokerFact(node: true)]
    public async Task E_SinPermisoDeAckElSdkLoDiceAlLoggerYElEventoNoSeConfirma()
    {
        await using var env = await Entorno.CrearAsync(
            (nombre, p) => nombre == "facturacion-api"
                ? p.SinPublicar(s => s.StartsWith("$JS.ACK." + Stream, StringComparison.Ordinal))
                : p);
        var log = new CapturingLogger();
        await using var productor = await ConectaAsync(env, "pedidos-api");
        await using var consumidor = await ConectaAsync(env, "facturacion-api", log);
        var recibidos = new Recibidos();
        await using var suscripcion = await consumidor.SubscribeAsync(Subject, recibidos.Handler);

        await productor.PublishAsync(Subject, Cuerpo(), new PublishOptions { AggregateId = "ped-1" });
        Assert.True(await EsperarAsync(() => !recibidos.Cola.IsEmpty, TimeSpan.FromSeconds(10)), "el evento no llegó");

        // El log dice el subject normalizado y la consecuencia: se reentrega sin fin.
        var esperado = $"$JS.ACK.{Stream}.{Durable("facturacion-api")}.>";
        Assert.True(
            await EsperarAsync(() => log.Errors.Any(l => l.Contains(esperado, StringComparison.Ordinal) &&
                                                         l.Contains("REENTREGA", StringComparison.Ordinal)), TimeSpan.FromSeconds(5)),
            "el ack denegado no quedó en el logger: " + string.Join(" | ", log.Lines));

        // Y el evento NO se confirmó: sigue pendiente de ack.
        var (nc, js) = await env.AbrirOpsAsync();
        await using (nc)
        {
            var info = (await js.GetConsumerAsync(Stream, Durable("facturacion-api"))).Info;
            Assert.Equal(1L, info.NumAckPending);
            Assert.Equal(0UL, info.AckFloor.ConsumerSeq);
        }

        // Se avisa UNA vez por subject: un log por mensaje enterraría la causa bajo su volumen.
        await productor.PublishAsync(Subject, Cuerpo(2), new PublishOptions { AggregateId = "ped-2" });
        Assert.True(await EsperarAsync(() => recibidos.Cola.Count >= 2, TimeSpan.FromSeconds(10)));
        await Task.Delay(300);
        Assert.Single(log.Errors, l => l.Contains(esperado, StringComparison.Ordinal));
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  F — credenciales
    // ═════════════════════════════════════════════════════════════════════════

    private static ConnectOptions Base(string url, string servicio = "fx-sec", NatsCredentials? credenciales = null, TlsOptions? tls = null) =>
        new()
        {
            Servers = url,
            Service = servicio,
            Environment = "test",
            Version = "1.0.0",
            SchemaBaseUrl = "https://schemas.internal",
            Credentials = credenciales,
            Tls = tls,
        };

    private static async Task<ConnectionRejectedException> RechazadaAsync(ConnectOptions opciones, RejectionReason motivo)
    {
        var e = await FallaAsync<ConnectionRejectedException>(async () =>
        {
            await using var bus = await FluxBus.ConnectAsync(opciones);
        });
        Assert.Equal("CONNECTION_REJECTED", e.FluxCode);
        Assert.Equal(ErrorClass.Permanent, e.FluxClass);
        Assert.Equal(motivo, e.Reason);
        return e;
    }

    [BrokerFact]
    public async Task F_UsuarioYContrasena()
    {
        await using var broker = await TestBroker.StartAsync((puerto, store, _) =>
            $"listen: 127.0.0.1:{puerto}\njetstream {{ store_dir: \"{store}\" }}\nauthorization {{ users: [ {{ user: \"svc\", password: \"secreto-123\" }} ] }}\n");

        // Bien formada y correcta: conecta.
        await using (await FluxBus.ConnectAsync(Base(broker.Url, credenciales: new NatsCredentials { User = "svc", Password = "secreto-123" })))
        {
        }

        // Bien formada y EQUIVOCADA: CONNECTION_REJECTED (auth), nunca INVALID_CREDENTIALS; y el mensaje no
        // lleva la contraseña.
        var e = await RechazadaAsync(
            Base(broker.Url, credenciales: new NatsCredentials { User = "svc", Password = "otra-clave-999" }),
            RejectionReason.Auth);
        Assert.DoesNotContain("otra-clave-999", e.Message, StringComparison.Ordinal);
        Assert.Contains("rechazó las credenciales", e.Message, StringComparison.Ordinal);
        Assert.Contains("Reintentar no lo arregla", e.Message, StringComparison.Ordinal);
        Assert.Contains(broker.Url, e.Message, StringComparison.Ordinal);

        // Sin credenciales contra un broker que las exige: lo mismo.
        await RechazadaAsync(Base(broker.Url), RejectionReason.Auth);
    }

    [BrokerFact]
    public async Task F_Token()
    {
        await using var broker = await TestBroker.StartAsync((puerto, store, _) =>
            $"listen: 127.0.0.1:{puerto}\njetstream {{ store_dir: \"{store}\" }}\nauthorization {{ token: \"tok-correcto\" }}\n");

        await using (await FluxBus.ConnectAsync(Base(broker.Url, credenciales: new NatsCredentials { Token = "tok-correcto" })))
        {
        }

        var e = await RechazadaAsync(Base(broker.Url, credenciales: new NatsCredentials { Token = "tok-equivocado" }), RejectionReason.Auth);
        Assert.DoesNotContain("tok-equivocado", e.Message, StringComparison.Ordinal);
    }

    [BrokerFact]
    public async Task F_Nkey_Y_Creds()
    {
        var semilla = NKeys.CreateUserSeed();
        var publica = NKeys.PublicKeyFromSeed(semilla);
        await using var broker = await TestBroker.StartAsync((puerto, store, _) =>
            $"listen: 127.0.0.1:{puerto}\njetstream {{ store_dir: \"{store}\" }}\nauthorization {{ users: [ {{ nkey: \"{publica}\" }} ] }}\n");

        // nkey
        await using (await FluxBus.ConnectAsync(Base(broker.Url, credenciales: new NatsCredentials { NkeySeed = semilla })))
        {
        }

        var ajena = NKeys.CreateUserSeed();
        var e = await RechazadaAsync(Base(broker.Url, credenciales: new NatsCredentials { NkeySeed = ajena }), RejectionReason.Auth);
        Assert.DoesNotContain(ajena, e.Message, StringComparison.Ordinal);

        // .creds (un servidor con nkeys estáticas solo mira la semilla: A-8e). Por ruta y por contenido.
        string Creds(string s) =>
            "-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QifQ.e30.relleno\n------END NATS USER JWT------\n\n" +
            $"-----BEGIN USER NKEY SEED-----\n{s}\n------END USER NKEY SEED------\n";
        var fichero = Path.Combine(broker.Dir, "svc.creds");
        await File.WriteAllTextAsync(fichero, Creds(semilla));
        await using (await FluxBus.ConnectAsync(Base(broker.Url, credenciales: new NatsCredentials { CredsFile = fichero })))
        {
        }

        await using (await FluxBus.ConnectAsync(Base(broker.Url, credenciales: new NatsCredentials { CredsContent = Creds(semilla) })))
        {
        }

        // Un .creds bien formado pero de otro usuario: lo rechaza el broker (auth), no es INVALID_CREDENTIALS.
        await RechazadaAsync(Base(broker.Url, credenciales: new NatsCredentials { CredsContent = Creds(ajena) }), RejectionReason.Auth);
    }

    [BrokerFact]
    public async Task F_UnCredsOUnaSemillaMalFormadosFallanEnAltoYSinRed()
    {
        // Sin NINGÚN broker escuchando: si el SDK abriera un socket, esperaría para siempre. Falla antes.
        var puerto = TestBroker.PortRange().From;
        var url = $"nats://127.0.0.1:{puerto}";
        var dir = Directory.CreateTempSubdirectory("flux-creds-").FullName;
        try
        {
            var sinSemilla = Path.Combine(dir, "sin-semilla.creds");
            await File.WriteAllTextAsync(sinSemilla, "-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QifQ.e30.relleno\n------END NATS USER JWT------\n");
            var basura = Path.Combine(dir, "basura.creds");
            await File.WriteAllTextAsync(basura, "esto no es un .creds\n");

            var casos = new (string Motivo, NatsCredentials Credenciales)[]
            {
                ("ruta que no existe", new NatsCredentials { CredsFile = Path.Combine(dir, "no-existe.creds") }),
                ("un .creds sin semilla", new NatsCredentials { CredsFile = sinSemilla }),
                ("un .creds que no lo es", new NatsCredentials { CredsFile = basura }),
                ("una semilla que no es de usuario", new NatsCredentials { NkeySeed = NKeys.CreateAccountSeed() }),
                ("una semilla que no es una semilla", new NatsCredentials { NkeySeed = "SUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" }),
                ("un valor vacío (variable de entorno sin definir)", new NatsCredentials { Token = "" }),
                ("un valor en blanco", new NatsCredentials { User = "  ", Password = "x" }),
                ("usuario sin contraseña", new NatsCredentials { User = "svc" }),
                ("contraseña sin usuario", new NatsCredentials { Password = "x" }),
                ("dos métodos a la vez", new NatsCredentials { Token = "t", NkeySeed = NKeys.CreateUserSeed() }),
            };

            foreach (var (motivo, credenciales) in casos)
            {
                var e = await FallaAsync<InvalidCredentialsException>(async () =>
                {
                    await using var bus = await FluxBus.ConnectAsync(Base(url, credenciales: credenciales));
                });
                Assert.Equal("INVALID_CREDENTIALS", e.FluxCode);
                Assert.Equal(ErrorClass.Permanent, e.FluxClass);
                Assert.Contains("Reintentar no lo arregla", e.Message, StringComparison.Ordinal);
                Assert.True(e.Message.Length > 0, motivo);
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  G — TLS con CA propia
    // ═════════════════════════════════════════════════════════════════════════

    [BrokerFact(openssl: true)]
    public async Task G_TlsConCaPropia_EnLosDosSentidos()
    {
        using var pki = await Pki.CrearAsync();
        await using var tls = await TestBroker.StartAsync(
            (p, store, _) => $"listen: 127.0.0.1:{p}\njetstream {{ store_dir: \"{store}\" }}\n{pki.Tls("server", verify: false)}", dir: pki.Dir);
        await using var altname = await TestBroker.StartAsync(
            (p, store, _) => $"listen: 127.0.0.1:{p}\njetstream {{ store_dir: \"{store}\" }}\n{pki.Tls("altname", verify: false)}", dir: pki.Dir);
        await using var plano = await TestBroker.StartAsync(
            (p, store, _) => $"listen: 127.0.0.1:{p}\njetstream {{ store_dir: \"{store}\" }}\n", dir: pki.Dir);

        // Con la CA correcta: conecta, publica y la conexión va cifrada.
        await using (var bus = await FluxBus.ConnectAsync(Base(tls.Url, tls: new TlsOptions { CaFile = pki.Ca })))
        {
            Assert.True(bus.Connected);
            Assert.NotEmpty((await bus.PublishAsync("tls.cosa.v1.hecha", new { n = 1 })).Id);
        }

        // Sin `Tls` (el servidor lo exige): el certificado no verifica contra las CA del sistema.
        var sinCa = await RechazadaAsync(Base(tls.Url), RejectionReason.Tls);
        Assert.Contains("Tls.CaFile", sinCa.Message, StringComparison.Ordinal);
        Assert.Contains("Reintentar no lo arregla", sinCa.Message, StringComparison.Ordinal);

        // `Tls` vacío exige TLS y verifica contra las del sistema: tampoco conocen la CA de prueba.
        await RechazadaAsync(Base(tls.Url, tls: new TlsOptions()), RejectionReason.Tls);

        // La CA EQUIVOCADA: la indicada es la que manda, no se mezcla con nada más.
        await RechazadaAsync(Base(tls.Url, tls: new TlsOptions { CaFile = pki.CaAjena }), RejectionReason.Tls);

        // Un certificado que no cubre el host (SAN) falla aunque la CA sea la correcta.
        await RechazadaAsync(Base(altname.Url, tls: new TlsOptions { CaFile = pki.Ca }), RejectionReason.Tls);

        // Se pide TLS y el servidor no lo ofrece: falla, y NO baja a texto plano.
        var sinTls = await RechazadaAsync(Base(plano.Url, tls: new TlsOptions { CaFile = pki.Ca }), RejectionReason.Tls);
        Assert.Contains("no lo ofrece", sinTls.Message, StringComparison.Ordinal);
    }

    [BrokerFact(openssl: true)]
    public async Task G_TlsMutuoConCertificadoDeCliente()
    {
        using var pki = await Pki.CrearAsync();
        await using var mtls = await TestBroker.StartAsync(
            (p, store, _) => $"listen: 127.0.0.1:{p}\njetstream {{ store_dir: \"{store}\" }}\n{pki.Tls("server", verify: true)}", dir: pki.Dir);

        await using (var bus = await FluxBus.ConnectAsync(Base(mtls.Url, tls: new TlsOptions
        {
            CaFile = pki.Ca,
            CertFile = pki.F("cliente.pem"),
            KeyFile = pki.F("cliente.key"),
        })))
        {
            Assert.NotEmpty((await bus.PublishAsync("mtls.cosa.v1.hecha", new { n = 1 })).Id);
        }

        // Sin certificado de cliente el broker corta el handshake: CONNECTION_REJECTED (tls). La spec lo deja en
        // DEBERÍA porque no todos los clientes lo distinguen de un timeout; NATS.Net en Linux sí (A-211).
        await RechazadaAsync(Base(mtls.Url, tls: new TlsOptions { CaFile = pki.Ca }), RejectionReason.Tls);
    }

    [BrokerFact(openssl: true)]
    public async Task G_UnaCaOUnCertificadoMalFormadosFallanEnAltoYSinRed()
    {
        using var pki = await Pki.CrearAsync();
        // Sin ningún broker escuchando.
        var url = $"nats://127.0.0.1:{TestBroker.PortRange().From}";
        var noPem = pki.F("basura.pem");
        await File.WriteAllTextAsync(noPem, "esto no es un PEM\n");

        var casos = new (string Motivo, TlsOptions Tls)[]
        {
            ("una CA que no existe", new TlsOptions { CaFile = pki.F("no-existe.pem") }),
            ("una CA que no es un PEM", new TlsOptions { CaFile = noPem }),
            ("una ruta vacía", new TlsOptions { CaFile = "" }),
            ("un certificado de cliente sin su clave", new TlsOptions { CaFile = pki.Ca, CertFile = pki.F("cliente.pem") }),
            ("una clave de cliente sin su certificado", new TlsOptions { CaFile = pki.Ca, KeyFile = pki.F("cliente.key") }),
            ("una clave que no es PEM", new TlsOptions { CaFile = pki.Ca, CertFile = pki.F("cliente.pem"), KeyFile = noPem }),
            ("una clave de otro certificado", new TlsOptions { CaFile = pki.Ca, CertFile = pki.F("cliente.pem"), KeyFile = pki.F("server.key") }),
        };

        foreach (var (motivo, tls) in casos)
        {
            var e = await FallaAsync<InvalidTlsOptionsException>(async () =>
            {
                await using var bus = await FluxBus.ConnectAsync(Base(url, tls: tls));
            });
            Assert.Equal("INVALID_TLS_OPTIONS", e.FluxCode);
            Assert.Equal(ErrorClass.Permanent, e.FluxClass);
            Assert.Contains("Reintentar no lo arregla", e.Message, StringComparison.Ordinal);
            Assert.True(e.Message.Length > 0, motivo);
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  H — la primera conexión: esperar al broker, no a lo que no cambia
    // ═════════════════════════════════════════════════════════════════════════

    [BrokerFact]
    public async Task H_ConnectEsperaAlBrokerQueAunNoHaArrancado()
    {
        var puerto = TestBroker.PortRange().From + 3;
        var url = $"nats://127.0.0.1:{puerto}";
        var log = new CapturingLogger();

        var conexion = FluxBus.ConnectAsync(Base(url) with { Logger = log });
        await Task.Delay(1500);
        Assert.False(conexion.IsCompleted, "ConnectAsync no esperó a un broker que aún no ha arrancado");

        await using var broker = await TestBroker.StartAsync(
            (p, store, _) => $"listen: 127.0.0.1:{p}\njetstream {{ store_dir: \"{store}\" }}\n", port: puerto);
        await using var bus = await conexion.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.True(bus.Connected);
        Assert.Contains(log.Lines, l => l.StartsWith("WARN ", StringComparison.Ordinal) && l.Contains("no está disponible", StringComparison.Ordinal));
    }

    [BrokerFact]
    public async Task H_LaEsperaSeCancela()
    {
        var url = $"nats://127.0.0.1:{TestBroker.PortRange().From + 4}";
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(600));
        var t0 = Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => FluxBus.ConnectAsync(Base(url), cts.Token));
        Assert.True(t0.Elapsed < TimeSpan.FromSeconds(3));
    }

    [BrokerFact]
    public async Task H_ElBrokerQueNoEstaNoEsNingunoDeLosTresCodigos()
    {
        // La frontera por el otro lado: esperar a un broker caído NO produce CONNECTION_REJECTED. Con un
        // límite de tiempo, lo que sale es la cancelación, nunca uno de los tres códigos.
        var url = $"nats://127.0.0.1:{TestBroker.PortRange().From + 5}";
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(1500));
        var e = await Record.ExceptionAsync(() => FluxBus.ConnectAsync(Base(url), cts.Token));
        Assert.IsAssignableFrom<OperationCanceledException>(e);
        Assert.IsNotAssignableFrom<ClassifiedException>(e);
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  Los cinco códigos
    // ═════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Cada código estable de la capacidad, por su literal: `scripts/check-sdk-codes.mjs` exige que la
    /// evidencia los nombre, y este test comprueba que los que emite el SDK son estos y PERMANENT.
    /// </summary>
    [Fact]
    public void LosCincoCodigos()
    {
        Assert.Equal("NOT_PROVISIONED", new NotProvisionedException(ResourceKind.Stream, "EVT_X", null, "svc", "$JS.API.STREAM.CREATE.EVT_X").FluxCode);
        Assert.Equal("PERMISSION_DENIED", new PermissionDeniedException("svc", DeniedOperation.Publish, "x.y").FluxCode);
        Assert.Equal("CONNECTION_REJECTED", new ConnectionRejectedException(RejectionReason.Auth, "m").FluxCode);
        Assert.Equal("INVALID_CREDENTIALS", new InvalidCredentialsException("m").FluxCode);
        Assert.Equal("INVALID_TLS_OPTIONS", new InvalidTlsOptionsException("m").FluxCode);

        foreach (var e in new ClassifiedException[]
                 {
                     new NotProvisionedException(ResourceKind.Stream, "EVT_X", null, "svc", "s"),
                     new PermissionDeniedException("svc", DeniedOperation.Publish, "x.y"),
                     new ConnectionRejectedException(RejectionReason.Tls, "m"),
                     new InvalidCredentialsException("m"),
                     new InvalidTlsOptionsException("m"),
                 })
        {
            Assert.Equal(ErrorClass.Permanent, e.FluxClass);
        }

        Assert.Equal("auth", new ConnectionRejectedException(RejectionReason.Auth, "m").ReasonCode);
        Assert.Equal("tls", new ConnectionRejectedException(RejectionReason.Tls, "m").ReasonCode);
    }

    /// <summary>
    /// Cada código se produce en su situación y en NINGUNA otra — 04-errors.md §5.3. La tabla es lo que el
    /// SDK DEBE devolver por cada fila de 03-delivery.md §6.1.
    /// </summary>
    [BrokerFact(openssl: true)]
    public async Task H_CadaCodigoDeConnectSeProduceEnSuSituacionYEnNingunaOtra()
    {
        using var pki = await Pki.CrearAsync();
        await using var tls = await TestBroker.StartAsync(
            (p, store, _) => $"listen: 127.0.0.1:{p}\njetstream {{ store_dir: \"{store}\" }}\n{pki.Tls("server", verify: false)}", dir: pki.Dir);
        await using var auth = await TestBroker.StartAsync(
            (p, store, _) => $"listen: 127.0.0.1:{p}\njetstream {{ store_dir: \"{store}\" }}\nauthorization {{ users: [ {{ user: \"svc\", password: \"pw\" }} ] }}\n", dir: pki.Dir);
        var sinBroker = $"nats://127.0.0.1:{TestBroker.PortRange().From + 6}";
        var malas = pki.F("mala.pem");
        await File.WriteAllTextAsync(malas, "no es un PEM");

        // (situación, opciones, código esperado, motivo esperado)
        var tabla = new (string Situacion, ConnectOptions Opciones, string? Codigo, string? Motivo)[]
        {
            ("credencial equivocada", Base(auth.Url, credenciales: new NatsCredentials { User = "svc", Password = "mal" }), "CONNECTION_REJECTED", "auth"),
            ("certificado que no verifica", Base(tls.Url), "CONNECTION_REJECTED", "tls"),
            ("CA que no existe", Base(sinBroker, tls: new TlsOptions { CaFile = pki.F("no-existe.pem") }), "INVALID_TLS_OPTIONS", null),
            ("CA que no es PEM", Base(sinBroker, tls: new TlsOptions { CaFile = malas }), "INVALID_TLS_OPTIONS", null),
            ("usuario sin contraseña", Base(sinBroker, credenciales: new NatsCredentials { User = "svc" }), "INVALID_CREDENTIALS", null),
            ("un .creds ilegible", Base(sinBroker, credenciales: new NatsCredentials { CredsFile = pki.F("no-existe.creds") }), "INVALID_CREDENTIALS", null),
            // Los que NO llevan ninguno de los tres: no son estas situaciones.
            ("nombre de servicio inválido", Base(sinBroker, servicio: "Facturacion_API"), null, null),
            ("lista de servidores que no se entiende", Base("esto no es una url ://"), null, null),
        };

        foreach (var (situacion, opciones, codigo, motivo) in tabla)
        {
            var e = await Record.ExceptionAsync(async () =>
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await using var bus = await FluxBus.ConnectAsync(opciones, cts.Token);
            });
            Assert.True(e is not null, $"{situacion}: ConnectAsync no falló");
            var fluxCode = e is ClassifiedException c ? c.FluxCode : null;
            var esDeLosTres = fluxCode is "CONNECTION_REJECTED" or "INVALID_CREDENTIALS" or "INVALID_TLS_OPTIONS";
            if (codigo is null)
            {
                Assert.False(esDeLosTres, $"{situacion}: llevó el código {fluxCode} y no es ninguna de esas situaciones ({e!.GetType().Name}: {e.Message})");
            }
            else
            {
                Assert.True(codigo == fluxCode, $"{situacion}: esperaba {codigo} y salió {e!.GetType().Name} ({e.Message})");
                if (motivo is not null)
                {
                    Assert.Equal(motivo, ((ConnectionRejectedException)e!).ReasonCode);
                }
            }
        }
    }
}
