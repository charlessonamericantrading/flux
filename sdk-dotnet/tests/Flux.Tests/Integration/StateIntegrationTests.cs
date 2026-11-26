// Estado replicado ejecutado contra un nats-server REAL, con los permisos exactos que genera `scripts/gen-acl.mjs` y los streams y durables de
// `flux provision` — plan v1.1, Fase 2 (y 6, para .NET). 10-state.md es el contrato.
//
// Tres cosas distintas: el ciclo de aceptación de la fase (el dueño escribe 100 claves, un observador NUEVO las recibe todas y recibe
// `OnSynced`, se borra una, el observador recibe la lápida y `GetAsync` de esa clave da `null`), EN LOS DOS SENTIDOS (quitar un permiso se
// nota), y lo que solo se ve contra un broker (la DLQ de estado, un POISON, un durable que se borra bajo un `watch`).
//
// Los siete códigos de estado —INVALID_STATE_CONTRACT, INVALID_TENANT_FOR_STATE, INVALID_STATE_KEY, MISSING_AGGREGATE_VERSION,
// STATE_SUBJECT_MISMATCH, STATE_CONFLICT y RESERVED_DOMAIN— se comprueban por su literal en `LosSieteCodigos`, que es lo que
// `scripts/check-sdk-codes.mjs` exige de la evidencia.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Xunit;

namespace Flux.Tests.Integration;

[Collection("broker")]
public class StateIntegrationTests
{
    private const string Contrato = "clientes.cliente.v1";
    private const string Stream = "STATE_CLIENTES";
    private static readonly TimeSpan Rapido = TimeSpan.FromSeconds(3);

    private static string DurableTienda => "tienda-web__state_clientes_cliente_v1__acme";

    private static object Cliente(string key, long version) =>
        new { clienteId = key, nombre = "María Peña", email = "maria@example.com", aggregateVersion = version };

    private sealed class Recibidas
    {
        public ConcurrentQueue<StateEntry> Cola { get; } = new();

        public StateHandler Handler => (e, _, _) =>
        {
            Cola.Enqueue(e);
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
        var e = await Assert.ThrowsAnyAsync<Exception>(() => operacion().WaitAsync(TimeSpan.FromSeconds(20)));
        Assert.True(e is not TimeoutException, "la operación no terminó en 20 s: espera para siempre en vez de fallar");
        Assert.True(t0.Elapsed < Rapido, $"tardó {t0.ElapsedMilliseconds} ms: parece que espera un timeout en vez de fallar");
        return Assert.IsType<T>(e);
    }

    private static Task<FluxBus> ConectaAsync(Entorno env, string servicio, CapturingLogger? log = null, string tenant = "acme", IMetricsSink? metrics = null) =>
        FluxBus.ConnectAsync(env.Opciones(servicio, log, tenant) with { Metrics = metrics });

    // ═════════════════════════════════════════════════════════════════════════
    //  El ciclo de aceptación
    // ═════════════════════════════════════════════════════════════════════════

    [BrokerFact(node: true)]
    public async Task Ciclo_el_duenio_escribe_100_claves_el_observador_nuevo_las_recibe_y_la_lapida_llega()
    {
        await using var env = await Entorno.CrearAsync();
        var logDueno = new CapturingLogger();
        var logObservador = new CapturingLogger();
        await using var dueno = await ConectaAsync(env, "crm-api", logDueno);

        for (var i = 1; i <= 100; i++)
        {
            var r = await dueno.State.PutAsync(Contrato, $"cli-{i}", Cliente($"cli-{i}", 1));
            Assert.True(r.Applied);
            Assert.Equal(StateOutcome.Ok, r.Outcome);
        }

        // Un observador NUEVO: el durable no existía antes (lo aprovisionó `flux provision`) y entrega la foto completa.
        await using var observador = await ConectaAsync(env, "tienda-web", logObservador);
        var recibidas = new Recibidas();
        var synced = 0;
        await using var watch = await observador.State.WatchAsync(
            Contrato, recibidas.Handler, new StateWatchOptions { OnSynced = () => Interlocked.Increment(ref synced) });
        Assert.Equal(DurableTienda, watch.Durable);
        Assert.Equal("acme", watch.TenantId);

        Assert.True(await EsperarAsync(() => recibidas.Cola.Count >= 100, TimeSpan.FromSeconds(20)), $"solo llegaron {recibidas.Cola.Count} de 100");
        await watch.Synced.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(100, recibidas.Cola.Select(e => e.Key).Distinct().Count());
        Assert.All(recibidas.Cola, e =>
        {
            Assert.Equal("acme", e.TenantId);
            Assert.False(e.Deleted);
            Assert.Equal(1, e.AggregateVersion);
            Assert.Equal("María Peña", e.Data!.Value.GetProperty("nombre").GetString());
        });
        Assert.Equal(1, Volatile.Read(ref synced));

        // Se borra una; el observador recibe la lápida y `GetAsync` de esa clave da null.
        var lapida = await dueno.State.DeleteAsync(Contrato, "cli-7", new StateDeleteOptions { AggregateVersion = 2 });
        Assert.True(lapida.Applied);
        Assert.True(await EsperarAsync(() => recibidas.Cola.Any(e => e.Deleted), TimeSpan.FromSeconds(10)), "la lápida no llegó");
        var entrada = recibidas.Cola.Single(e => e.Deleted);
        Assert.Equal("cli-7", entrada.Key);
        Assert.Null(entrada.Data);
        Assert.Equal(2, entrada.AggregateVersion);
        Assert.Null(await observador.State.GetAsync(Contrato, "cli-7"));
        var otra = await observador.State.GetAsync(Contrato, "cli-8");
        Assert.Equal(1, otra!.AggregateVersion);

        // Y `OnSynced` NO se invocó de nuevo por la lápida: es una vez por watch.
        Assert.Equal(1, Volatile.Read(ref synced));

        // Con permisos exactos, el broker no anotó ninguna violación y nadie registró un error.
        Assert.Empty(env.Violaciones());
        Assert.Empty(logDueno.Errors);
        Assert.Empty(logObservador.Errors);
    }

    [BrokerFact(node: true)]
    public async Task Un_observador_confinado_a_un_tenant_no_lee_el_de_otro_y_el_duenio_si_escribe_los_dos()
    {
        await using var env = await Entorno.CrearAsync();
        await using var dueno = await ConectaAsync(env, "crm-api");
        await dueno.State.PutAsync(Contrato, "cli-1", Cliente("cli-1", 1), new StatePutOptions { TenantId = "acme" });
        await dueno.State.PutAsync(Contrato, "cli-1", Cliente("cli-1", 5), new StatePutOptions { TenantId = "globex" });

        await using var tienda = await ConectaAsync(env, "tienda-web"); // acme

        Assert.Equal(1, (await tienda.State.GetAsync(Contrato, "cli-1"))!.AggregateVersion);

        // El broker lo aplica: el permiso de DIRECT.GET lleva el tenant, no el SDK.
        var e = await FallaAsync<PermissionDeniedException>(
            () => tienda.State.GetAsync(Contrato, "cli-1", new StateGetOptions { TenantId = "globex" }));
        Assert.Equal("PERMISSION_DENIED", e.FluxCode);
        Assert.Contains("globex", e.DeniedSubject, StringComparison.Ordinal);
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  En los dos sentidos: quitar un permiso se nota
    // ═════════════════════════════════════════════════════════════════════════

    private static Func<string, Perms, Perms> Sin(string servicio, Func<string, bool> predicado) =>
        (nombre, p) => nombre == servicio ? p.SinPublicar(predicado) : p;

    [BrokerFact(node: true)]
    public async Task Sin_permiso_de_lectura_directa_get_y_put_fallan_rapido_con_permission_denied()
    {
        await using var env = await Entorno.CrearAsync(Sin("crm-api", s => s.StartsWith("$JS.API.DIRECT.GET", StringComparison.Ordinal)));
        await using var dueno = await ConectaAsync(env, "crm-api");

        var put = await FallaAsync<PermissionDeniedException>(() => dueno.State.PutAsync(Contrato, "cli-1", Cliente("cli-1", 1)));
        Assert.StartsWith($"$JS.API.DIRECT.GET.{Stream}.", put.DeniedSubject, StringComparison.Ordinal);
        await FallaAsync<PermissionDeniedException>(() => dueno.State.GetAsync(Contrato, "cli-1"));
    }

    [BrokerFact(node: true)]
    public async Task Sin_permiso_de_escritura_put_falla_con_permission_denied_nombrando_el_subject()
    {
        await using var env = await Entorno.CrearAsync(Sin("crm-api", s => s == "state.clientes.>"));
        await using var dueno = await ConectaAsync(env, "crm-api");

        var e = await FallaAsync<PermissionDeniedException>(() => dueno.State.PutAsync(Contrato, "cli-1", Cliente("cli-1", 1)));
        Assert.Equal("state.clientes.cliente.v1.acme.cli-1", e.DeniedSubject);
    }

    [BrokerFact(node: true)]
    public async Task Sin_permiso_de_pull_watch_falla_y_sin_permiso_de_consultar_el_consumidor_tambien()
    {
        await using (var env = await Entorno.CrearAsync(Sin("tienda-web", s => s.StartsWith("$JS.API.CONSUMER.MSG.NEXT.STATE", StringComparison.Ordinal))))
        {
            await using var observador = await ConectaAsync(env, "tienda-web");
            var e = await FallaAsync<PermissionDeniedException>(() => observador.State.WatchAsync(Contrato, new Recibidas().Handler));
            Assert.Equal($"$JS.API.CONSUMER.MSG.NEXT.{Stream}.{DurableTienda}", e.DeniedSubject);
        }

        await using (var env = await Entorno.CrearAsync(Sin("tienda-web", s => s.StartsWith("$JS.API.CONSUMER.INFO.STATE", StringComparison.Ordinal))))
        {
            await using var observador = await ConectaAsync(env, "tienda-web");
            var e = await FallaAsync<PermissionDeniedException>(() => observador.State.WatchAsync(Contrato, new Recibidas().Handler));
            Assert.Equal($"$JS.API.CONSUMER.INFO.{Stream}.{DurableTienda}", e.DeniedSubject);
        }
    }

    [BrokerFact(node: true)]
    public async Task Sin_permiso_de_ack_se_registra_y_la_entrada_no_se_confirma()
    {
        await using var env = await Entorno.CrearAsync(Sin("tienda-web", s => s.StartsWith("$JS.ACK.STATE", StringComparison.Ordinal)));
        await using var dueno = await ConectaAsync(env, "crm-api");
        await dueno.State.PutAsync(Contrato, "cli-1", Cliente("cli-1", 1));
        var log = new CapturingLogger();
        await using var observador = await ConectaAsync(env, "tienda-web", log);
        var recibidas = new Recibidas();
        await using var watch = await observador.State.WatchAsync(Contrato, recibidas.Handler);
        Assert.True(await EsperarAsync(() => !recibidas.Cola.IsEmpty, TimeSpan.FromSeconds(10)));

        var esperado = $"$JS.ACK.{Stream}.{DurableTienda}.>";
        Assert.True(
            await EsperarAsync(() => log.Errors.Any(l => l.Contains(esperado, StringComparison.Ordinal)), TimeSpan.FromSeconds(5)),
            "el ack denegado no quedó en el logger: " + string.Join(" | ", log.Lines));
    }

    [BrokerFact(node: true)]
    public async Task Sin_aprovisionar_el_stream_de_estado_es_not_provisioned_y_el_mensaje_nombra_las_dos_causas()
    {
        await using var env = await Entorno.CrearAsync(aprovisionar: false);
        await using var dueno = await ConectaAsync(env, "crm-api");

        // DIRECT.GET a un stream que no existe, o sin allow_direct: no contesta nadie (A-32), no un 404.
        var put = await FallaAsync<NotProvisionedException>(() => dueno.State.PutAsync(Contrato, "cli-1", Cliente("cli-1", 1)));
        Assert.Equal("NOT_PROVISIONED", put.FluxCode);
        Assert.Contains("allow_direct", put.Message, StringComparison.Ordinal);
        Assert.Contains("no existe", put.Message, StringComparison.Ordinal);
        Assert.Contains("flux provision", put.Message, StringComparison.Ordinal);
        Assert.Contains("flux doctor", put.Message, StringComparison.Ordinal);
        await FallaAsync<NotProvisionedException>(() => dueno.State.GetAsync(Contrato, "cli-1"));
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  Lo que solo se ve contra un broker
    // ═════════════════════════════════════════════════════════════════════════

    /// <summary>Todos los mensajes de un stream, parseados. Solo para inspeccionar la DLQ.</summary>
    private static async Task<List<JsonElement>> MensajesAsync(NatsJSContext js, string stream)
    {
        var s = await js.GetStreamAsync(stream);
        var salida = new List<JsonElement>();
        if (s.Info.State.Messages == 0)
        {
            return salida;
        }

        for (var seq = s.Info.State.FirstSeq; seq <= s.Info.State.LastSeq; seq++)
        {
            try
            {
                var m = await s.GetAsync(new StreamMsgGetRequest { Seq = seq });
                salida.Add(JsonDocument.Parse(m.Message.Data).RootElement.Clone());
            }
            catch (NatsJSApiException)
            {
            }
        }

        return salida;
    }

    [BrokerFact(node: true)]
    public async Task Un_handler_permanente_manda_la_entrada_a_la_dlq_de_estado_sin_su_dato_y_un_error_ajeno_no_deja_su_mensaje()
    {
        await using var env = await Entorno.CrearAsync();
        await using var dueno = await ConectaAsync(env, "crm-api");
        await dueno.State.PutAsync(Contrato, "cli-1", Cliente("cli-1", 3));
        await dueno.State.PutAsync(Contrato, "cli-2", Cliente("cli-2", 1));
        var dlqs = new ConcurrentQueue<DlqEventInfo>();
        var logObs = new CapturingLogger();
        var metricas = new InMemoryMetrics();
        await using var observador = await FluxBus.ConnectAsync(env.Opciones("tienda-web", logObs) with
        {
            Metrics = metricas,
            OnDlq = dlqs.Enqueue,
            // Con la política por defecto un error ajeno se reintenta 30 s; aquí se quiere ver qué se guarda al agotarse.
            Classifier = new ClassifierOptions { UnknownErrorPolicy = UnknownPolicy.Permanent },
        });

        await using var watch = await observador.State.WatchAsync(Contrato, (e, _, _) =>
            e.Key == "cli-1"
                ? throw new PermanentException("PAIS_NO_SOPORTADO: el cliente cli-1 tiene un país fuera del catálogo", code: "PAIS_NO_SOPORTADO")
                : throw new InvalidOperationException("duplicate key value violates unique constraint — Key (email)=(ana@example.com)"));

        Assert.True(await EsperarAsync(() => dlqs.Count >= 2, TimeSpan.FromSeconds(15)), "no llegaron las dos entradas a la DLQ: " + string.Join(" | ", logObs.Lines));

        // La métrica cuenta cada entrada que va a la DLQ (08-observability.md §2.4).
        Assert.Equal(2, metricas.Counters().Where(c => c.Key.StartsWith("flux_events_dlq_total{", StringComparison.Ordinal)).Sum(c => (long)c.Value));

        var (nc, js) = await env.AbrirOpsAsync();
        await using (nc)
        {
            var mensajes = await MensajesAsync(js, "DLQ_CLIENTES");
            Assert.Equal(2, mensajes.Count);
            var cli1 = mensajes.Single(m => m.GetProperty("subject").GetString() == "cli-1");
            // El sobre original, con las `dlq*` y SIN su dato: `data` es `{ key, revision }`.
            Assert.Equal("permanent", cli1.GetProperty("dlqreason").GetString());
            Assert.Equal(DurableTienda, cli1.GetProperty("dlqconsumer").GetString());
            Assert.StartsWith("PAIS_NO_SOPORTADO: ", cli1.GetProperty("dlqerror").GetString(), StringComparison.Ordinal);
            Assert.Equal("cli-1", cli1.GetProperty("data").GetProperty("key").GetString());
            Assert.True(cli1.GetProperty("data").GetProperty("revision").GetInt64() > 0);
            Assert.Equal(2, cli1.GetProperty("data").EnumerateObject().Count());
            Assert.DoesNotContain("maria@example.com", cli1.GetRawText(), StringComparison.Ordinal);
            Assert.False(cli1.TryGetProperty("signature", out _));
            // `data` es el ÚLTIMO atributo.
            Assert.Equal("data", cli1.EnumerateObject().Last().Name);

            // Un error que NO es de flux no deja su mensaje: la DLQ guarda 90 días y ahí vive la fila entera.
            var cli2 = mensajes.Single(m => m.GetProperty("subject").GetString() == "cli-2");
            var dlqerror = cli2.GetProperty("dlqerror").GetString()!;
            Assert.DoesNotContain("ana@example.com", dlqerror, StringComparison.Ordinal);
            Assert.Contains(StateProtocol.UnclassifiedDlqError, dlqerror, StringComparison.Ordinal);
        }
    }

    [BrokerFact(node: true)]
    public async Task Un_sobre_que_miente_sobre_su_identidad_es_poison_y_no_llega_al_handler()
    {
        await using var env = await Entorno.CrearAsync();
        var (nc, js) = await env.AbrirOpsAsync();
        await using (nc)
        {
            await using var dueno = await ConectaAsync(env, "crm-api");
            var bueno = await dueno.State.PutAsync(Contrato, "cli-1", Cliente("cli-1", 1));
            Assert.True(bueno.Applied);

            // Con credenciales de operación se publica un sobre cuyo `tenantid` es otro que el del subject, otro cuyo `subject` no
            // es la clave, uno `restricted` y uno ilegible.
            var n = 0;
            string Sobre(string tenant, string clave, string clasificacion) =>
                "{\"specversion\":\"1.0\",\"id\":\"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e5" + (++n) + "\",\"source\":\"/test/x\"," +
                "\"type\":\"com.flux.state.clientes.cliente.v1\",\"time\":\"2026-08-20T10:25:39.412Z\",\"datacontenttype\":\"application/json\"," +
                "\"dataschema\":\"https://schemas.internal/state/clientes/cliente/1.0.0.json\",\"subject\":\"" + clave + "\"," +
                "\"correlationid\":\"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55\",\"tenantid\":\"" + tenant + "\",\"producerversion\":\"1.0.0\"," +
                "\"dataclassification\":\"" + clasificacion + "\",\"partitionkey\":\"" + clave + "\",\"data\":{\"aggregateVersion\":1,\"nombre\":\"Ana Dato\"}}";

            await js.PublishAsync("state.clientes.cliente.v1.acme.cli-2", Encoding.UTF8.GetBytes(Sobre("globex", "cli-2", "confidential")));
            await js.PublishAsync("state.clientes.cliente.v1.acme.cli-3", Encoding.UTF8.GetBytes(Sobre("acme", "otra", "confidential")));
            await js.PublishAsync("state.clientes.cliente.v1.acme.cli-4", Encoding.UTF8.GetBytes(Sobre("acme", "cli-4", "restricted")));
            await js.PublishAsync("state.clientes.cliente.v1.acme.cli-5", Encoding.UTF8.GetBytes("esto no es JSON {Ana Dato"));

            var poison = new ConcurrentQueue<PoisonInfo>();
            var dlq = new ConcurrentQueue<DlqEventInfo>();
            var recibidas = new Recibidas();
            var logObs = new CapturingLogger();
            await using var observador = await FluxBus.ConnectAsync(env.Opciones("tienda-web", logObs) with { OnPoison = poison.Enqueue, OnDlq = dlq.Enqueue });
            await using var watch = await observador.State.WatchAsync(Contrato, recibidas.Handler);

            Assert.True(await EsperarAsync(() => dlq.Count >= 4, TimeSpan.FromSeconds(15)), $"solo {dlq.Count} de 4 llegaron a la DLQ: " + string.Join(" | ", logObs.Lines));
            // Solo la entrada buena llegó al handler: lo que miente nunca se aplica.
            Assert.Equal(new[] { "cli-1" }, recibidas.Cola.Select(e => e.Key).ToArray());
            Assert.Equal(4, poison.Count);
            Assert.All(poison, p => Assert.Empty(p.Raw)); // el cuerpo de una entrada de estado puede llevar datos personales

            var codigos = dlq.Select(d => d.Classification.Code).OrderBy(c => c).ToList();
            Assert.Equal(new[] { "INVALID_DATACLASSIFICATION", "MALFORMED_JSON", "STATE_SUBJECT_MISMATCH", "STATE_SUBJECT_MISMATCH" }, codigos);

            var mensajes = await MensajesAsync(js, "DLQ_CLIENTES");
            Assert.Equal(4, mensajes.Count);
            Assert.All(mensajes, m => Assert.Equal("poison", m.GetProperty("dlqreason").GetString()));
            // La clave que se registra es la DEL SUBJECT —la que decidió el broker—, no la del sobre.
            Assert.Contains(mensajes, m => m.GetProperty("data").GetProperty("key").GetString() == "cli-3");
            // Un mensaje ilegible no se copia: ni su cuerpo ni un `rawBase64`.
            var ilegible = mensajes.Single(m => m.GetProperty("data").GetProperty("key").GetString() == "cli-5");
            Assert.DoesNotContain("Ana Dato", ilegible.GetRawText(), StringComparison.Ordinal);
            Assert.DoesNotContain("rawBase64", ilegible.GetRawText(), StringComparison.Ordinal);
            Assert.Equal("acme", ilegible.GetProperty("tenantid").GetString());
        }
    }

    [BrokerFact(node: true)]
    public async Task Un_watch_vivo_reabre_su_consumo_cuando_el_durable_se_borra_y_se_recrea()
    {
        await using var env = await Entorno.CrearAsync();
        await using var dueno = await ConectaAsync(env, "crm-api");
        for (var i = 1; i <= 5; i++)
        {
            await dueno.State.PutAsync(Contrato, $"cli-{i}", Cliente($"cli-{i}", 1));
        }

        var log = new CapturingLogger();
        await using var observador = await ConectaAsync(env, "tienda-web", log);
        var recibidas = new Recibidas();
        await using var watch = await observador.State.WatchAsync(Contrato, recibidas.Handler);
        Assert.True(await EsperarAsync(() => recibidas.Cola.Count >= 5, TimeSpan.FromSeconds(10)));

        // `flux state resync`: con credenciales de operación se borra el durable y se vuelve a crear con su config canónica.
        var (nc, js) = await env.AbrirOpsAsync();
        await using (nc)
        {
            var antes = (await js.GetConsumerAsync(Stream, DurableTienda)).Info.Config;
            await js.DeleteConsumerAsync(Stream, DurableTienda);
            var t0 = Stopwatch.StartNew();
            await js.CreateConsumerAsync(Stream, antes);

            // Se entrega la foto OTRA vez (y un put en vivo después).
            var esperado = recibidas.Cola.Count + 5;
            Assert.True(
                await EsperarAsync(() => recibidas.Cola.Count >= esperado, TimeSpan.FromSeconds(60)),
                "el watch no reabrió su consumo: " + string.Join(" | ", log.Lines));
            var tiempo = t0.Elapsed;
            Console.WriteLine($"[medida] reapertura del consumo tras recrear el durable: {tiempo.TotalSeconds:F1} s");
            Assert.True(tiempo < TimeSpan.FromSeconds(45), $"reabrió en {tiempo.TotalSeconds:F1} s");

            await dueno.State.PutAsync(Contrato, "cli-9", Cliente("cli-9", 1));
            Assert.True(await EsperarAsync(() => recibidas.Cola.Any(e => e.Key == "cli-9"), TimeSpan.FromSeconds(15)), "el put en vivo no llegó tras reabrir");
        }
    }

    [BrokerFact(node: true)]
    public async Task Un_durable_aprovisionado_con_otro_filtro_o_otra_politica_de_entrega_no_se_consume()
    {
        await using var env = await Entorno.CrearAsync();
        var (nc, js) = await env.AbrirOpsAsync();
        await using (nc)
        {
            var canonica = (await js.GetConsumerAsync(Stream, DurableTienda)).Info.Config;

            // (a) Otro filtro: los datos de TODOS los tenants del contrato, sin ningún error. El SDK no lo consume.
            await js.DeleteConsumerAsync(Stream, DurableTienda);
            var ancho = Copia(canonica);
            ancho.FilterSubject = "state.clientes.cliente.v1.>";
            await js.CreateConsumerAsync(Stream, ancho);
            await using (var bus = await ConectaAsync(env, "tienda-web"))
            {
                var e = await Assert.ThrowsAsync<ConsumerConfigMismatchException>(() => bus.State.WatchAsync(Contrato, new Recibidas().Handler));
                Assert.Contains(e.Differences, d => d.Field == "filter_subject");
            }

            // (b) Otra política de entrega: `all` entregaría cada versión intermedia de cada clave, no la última.
            await js.DeleteConsumerAsync(Stream, DurableTienda);
            var todas = Copia(canonica);
            todas.DeliverPolicy = ConsumerConfigDeliverPolicy.All;
            await js.CreateConsumerAsync(Stream, todas);
            await using (var bus = await ConectaAsync(env, "tienda-web"))
            {
                var e = await Assert.ThrowsAsync<ConsumerConfigMismatchException>(() => bus.State.WatchAsync(Contrato, new Recibidas().Handler));
                Assert.Contains(e.Differences, d => d.Field == "deliver_policy");
            }
        }
    }

    private static ConsumerConfig Copia(ConsumerConfig c) =>
        System.Text.Json.JsonSerializer.Deserialize<ConsumerConfig>(System.Text.Json.JsonSerializer.Serialize(c))!;

    [BrokerFact(node: true)]
    public async Task Si_la_dlq_de_estado_no_se_puede_escribir_el_mensaje_no_se_termina_y_se_registra()
    {
        // Terminar sin haberlo guardado en ningún sitio lo PERDERÍA: un mensaje que nadie ha podido procesar ni guardar se reentrega.
        await using var env = await Entorno.CrearAsync(Sin("tienda-web", s => s.StartsWith("dlq.", StringComparison.Ordinal)));
        await using var dueno = await ConectaAsync(env, "crm-api");
        await dueno.State.PutAsync(Contrato, "cli-1", Cliente("cli-1", 1));
        var log = new CapturingLogger();
        var dlqs = new ConcurrentQueue<DlqEventInfo>();
        await using var observador = await FluxBus.ConnectAsync(env.Opciones("tienda-web", log) with { OnDlq = dlqs.Enqueue });
        await using var watch = await observador.State.WatchAsync(
            Contrato, (_, _, _) => throw new PermanentException("PAIS_NO_SOPORTADO: no", code: "PAIS_NO_SOPORTADO"));

        Assert.True(
            await EsperarAsync(() => log.Errors.Any(l => l.Contains("no se pudo publicar en la DLQ de estado", StringComparison.Ordinal)), TimeSpan.FromSeconds(10)),
            "no se registró el fallo de la DLQ: " + string.Join(" | ", log.Lines));
        Assert.Empty(dlqs); // no se avisó de una entrada «guardada» que no se guardó

        var (nc, js) = await env.AbrirOpsAsync();
        await using (nc)
        {
            var info = (await js.GetConsumerAsync(Stream, DurableTienda)).Info;
            // Sigue pendiente de confirmar (se reentregará): NO se terminó.
            Assert.Equal(1L, (long)info.NumAckPending + (long)info.NumPending);
        }
    }

    [BrokerFact(node: true)]
    public async Task Publish_y_subscribe_rechazan_los_dominios_reservados_antes_de_tocar_la_red()
    {
        await using var env = await Entorno.CrearAsync();
        await using var bus = await ConectaAsync(env, "crm-api");
        foreach (var subject in new[] { "state.clientes.cliente.v1", "state.cliente.v1.creado", "rpc.clientes.cliente.v1.obtener", "dlq.clientes.cliente.v1.creado", "system.clientes.cliente.v1.creado" })
        {
            var p = await Assert.ThrowsAsync<ReservedDomainException>(() => bus.PublishAsync(subject, new { a = 1 }));
            Assert.Equal("RESERVED_DOMAIN", p.FluxCode);
            await Assert.ThrowsAsync<ReservedDomainException>(() => bus.SubscribeAsync(subject, (_, _, _) => Task.CompletedTask));
        }

        // `State` y `rpc` lo mismo en el contrato.
        await Assert.ThrowsAsync<ReservedDomainException>(() => bus.State.PutAsync("state.cliente.v1", "k", Cliente("k", 1)));
    }

    [BrokerFact(node: true)]
    public async Task El_orden_de_validacion_de_put_y_delete_es_el_de_la_spec_y_el_primero_que_falla_gana()
    {
        await using var env = await Entorno.CrearAsync();
        await using var bus = await ConectaAsync(env, "crm-api");
        var sinVersion = new { clienteId = "x" };

        // Dos defectos a la vez: gana el que va antes (contrato → clave → tenant → versión), y se ve ANTES de tocar la red.
        await Assert.ThrowsAsync<InvalidStateKeyException>(
            () => bus.State.PutAsync(Contrato, "a.b", sinVersion, new StatePutOptions { TenantId = "ACME" }));
        await Assert.ThrowsAsync<InvalidTenantForStateException>(
            () => bus.State.PutAsync(Contrato, "cli-1", sinVersion, new StatePutOptions { TenantId = "ACME" }));
        await Assert.ThrowsAsync<MissingAggregateVersionException>(() => bus.State.PutAsync(Contrato, "cli-1", sinVersion));
        await Assert.ThrowsAsync<ReservedDomainException>(
            () => bus.State.PutAsync("state.cliente.v1", "a.b", sinVersion, new StatePutOptions { TenantId = "ACME" }));

        await Assert.ThrowsAsync<InvalidStateKeyException>(
            () => bus.State.DeleteAsync(Contrato, "a.b", new StateDeleteOptions { AggregateVersion = 0, TenantId = "ACME" }));
        await Assert.ThrowsAsync<InvalidTenantForStateException>(
            () => bus.State.DeleteAsync(Contrato, "cli-1", new StateDeleteOptions { AggregateVersion = 0, TenantId = "ACME" }));
        await Assert.ThrowsAsync<MissingAggregateVersionException>(
            () => bus.State.DeleteAsync(Contrato, "cli-1", new StateDeleteOptions { AggregateVersion = 0 }));
    }

    [BrokerFact(node: true)]
    public async Task El_flujo_asincrono_entrega_la_foto_y_solo_confirma_lo_que_el_cuerpo_del_bucle_termino()
    {
        await using var env = await Entorno.CrearAsync();
        await using var dueno = await ConectaAsync(env, "crm-api");
        for (var i = 1; i <= 3; i++)
        {
            await dueno.State.PutAsync(Contrato, $"cli-{i}", Cliente($"cli-{i}", 1));
        }

        await using var observador = await ConectaAsync(env, "tienda-web");
        var vistas = new List<string>();
        using (var limite = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
        {
            await foreach (var e in observador.State.WatchEntriesAsync(Contrato, cancellationToken: limite.Token))
            {
                vistas.Add(e.Key);
                if (vistas.Count == 2)
                {
                    // El cuerpo de la SEGUNDA vuelta no termina: se sale. Dar tiempo a que se confirme la primera.
                    await Task.Delay(300);
                    break;
                }
            }
        }

        Assert.Equal(2, vistas.Count);
        Assert.Equal(2, vistas.Distinct().Count());

        var (nc, js) = await env.AbrirOpsAsync();
        await using (nc)
        {
            var info = (await js.GetConsumerAsync(Stream, DurableTienda)).Info;
            // La primera entrega SÍ se confirmó (su cuerpo terminó); la segunda NO (se salió con `break`): el suelo de acks no la alcanza.
            Assert.Equal(1UL, info.AckFloor.StreamSeq);
        }
    }

    [BrokerFact(node: true)]
    public async Task Las_metricas_de_estado_cuentan_ok_stale_y_invalid_schema()
    {
        await using var env = await Entorno.CrearAsync();
        var metricas = new InMemoryMetrics();
        await using var dueno = await ConectaAsync(env, "crm-api", metrics: metricas);

        await dueno.State.PutAsync(Contrato, "cli-1", Cliente("cli-1", 1));
        await dueno.State.PutAsync(Contrato, "cli-1", Cliente("cli-1", 1)); // misma versión: stale
        await Assert.ThrowsAsync<MissingAggregateVersionException>(() => dueno.State.PutAsync(Contrato, "cli-1", new { clienteId = "x" }));

        var c = metricas.Counters();
        Assert.Equal(1, c["flux_state_writes_total{contract=\"clientes.cliente.v1\",outcome=\"ok\"}"]);
        Assert.Equal(1, c["flux_state_writes_total{contract=\"clientes.cliente.v1\",outcome=\"stale\"}"]);
        Assert.Equal(1, c["flux_state_writes_total{contract=\"clientes.cliente.v1\",outcome=\"invalid_schema\"}"]);
        Assert.Contains("# TYPE flux_state_writes_total counter", metricas.Render(), StringComparison.Ordinal);
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  Los dos códigos de conflicto de secuencia: 10071 y 10164
    // ═════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Un solo nodo da 10071; un R3 bajo carrera da 10164 (A-210), y un SDK que solo mira 10071 declara conflicto o pierde su propia
    /// escritura bajo carga. Un nodo no produce 10164: se FABRICA el error con el gancho y se comprueba lo que el SDK hace con él.
    /// </summary>
    [BrokerFact(node: true)]
    public async Task Un_conflicto_10164_se_trata_como_el_10071_se_reconoce_la_propia_escritura_y_se_agota_en_tres()
    {
        await using var env = await Entorno.CrearAsync();

        // (a) el publish SE APLICÓ y quien lo hizo ve un 10164: el paso 1b reconoce su `id` y es éxito, no conflicto.
        await using (var a = await ConectaAsync(env, "crm-api"))
        {
            var propias = 0;
            a.State.Hook = new WriteHook { LoseAck = n => n == 1, LoseAckErrCode = 10164, OnOwnWrite = _ => propias++ };
            var r = await a.State.PutAsync(Contrato, "cli-1", Cliente("cli-1", 1));
            Assert.True(r.Applied);
            Assert.Equal(1, propias);
            Assert.Equal(1, (await a.State.GetAsync(Contrato, "cli-1"))!.AggregateVersion);
        }

        // (b) el 10164 NO se aplicó, tres veces seguidas: STATE_CONFLICT (RETRYABLE) tras tres intentos, sin escribir.
        await using (var b = await ConectaAsync(env, "crm-api"))
        {
            var intentos = 0;
            b.State.Hook = new WriteHook { FailPublish = _ => 10164, OnDone = n => intentos = n };
            var e = await Assert.ThrowsAsync<StateConflictException>(() => b.State.PutAsync(Contrato, "cli-2", Cliente("cli-2", 1)));
            Assert.Equal("STATE_CONFLICT", e.FluxCode);
            Assert.Equal(ErrorClass.Retryable, e.FluxClass);
            Assert.Equal(3, intentos);
            Assert.Null(await b.State.GetAsync(Contrato, "cli-2"));
        }

        // (c) un 10164 y luego el publish normal: se reintenta y se aplica.
        await using (var c = await ConectaAsync(env, "crm-api"))
        {
            c.State.Hook = new WriteHook { FailPublish = n => n == 1 ? 10164 : null };
            Assert.True((await c.State.PutAsync(Contrato, "cli-3", Cliente("cli-3", 1))).Applied);
        }
    }

    [Fact]
    public void Los_dos_err_code_de_conflicto_se_reconocen_y_los_demas_no()
    {
        static NatsJSApiException Api(int err) => new(new ApiError { Code = 400, ErrCode = err, Description = "x" });
        Assert.True(StateClient.IsSequenceConflict(Api(10071)));
        Assert.True(StateClient.IsSequenceConflict(Api(10164)));
        Assert.False(StateClient.IsSequenceConflict(Api(10059))); // stream no encontrado: NOT_PROVISIONED, no un conflicto
        Assert.False(StateClient.IsSequenceConflict(Api(10014)));
        Assert.False(StateClient.IsSequenceConflict(Api(0)));
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  Los siete códigos
    // ═════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Los siete códigos de la capacidad, por su literal: `scripts/check-sdk-codes.mjs` exige que la evidencia los nombre. Y cada uno sale en SU
    /// situación (sin red) con su clase.
    /// </summary>
    [Fact]
    public void LosSieteCodigos()
    {
        static ClassifiedException Falla(Action a) => Assert.IsAssignableFrom<ClassifiedException>(Record.Exception(a));

        var esperados = new (string Codigo, ErrorClass Clase, ClassifiedException E)[]
        {
            ("INVALID_STATE_CONTRACT", ErrorClass.Permanent, Falla(() => StateProtocol.ParseContract("clientes.cliente"))),
            ("INVALID_TENANT_FOR_STATE", ErrorClass.Permanent, Falla(() => StateProtocol.AssertTenant("ACME_Corp"))),
            ("INVALID_STATE_KEY", ErrorClass.Permanent, Falla(() => StateProtocol.AssertKey("a.b"))),
            ("MISSING_AGGREGATE_VERSION", ErrorClass.Permanent, Falla(() => StateProtocol.AggregateVersionOf(JsonDocument.Parse("{}").RootElement))),
            ("STATE_SUBJECT_MISMATCH", ErrorClass.Poison, new StateSubjectMismatchException("tenantid", "x", "y")),
            ("STATE_CONFLICT", ErrorClass.Retryable, new StateConflictException(Contrato, "k", 3)),
            ("RESERVED_DOMAIN", ErrorClass.Permanent, Falla(() => StateProtocol.ParseContract("state.cliente.v1"))),
        };
        foreach (var (codigo, clase, e) in esperados)
        {
            Assert.Equal(codigo, e.FluxCode);
            Assert.Equal(clase, e.FluxClass);
        }
    }
}
