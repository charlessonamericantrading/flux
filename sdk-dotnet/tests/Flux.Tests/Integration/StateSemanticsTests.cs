// Estado replicado: los escenarios de `conformance/cases/state-semantics.json` como DATOS.
//
// El fichero que ejecuta la implementación de referencia en JavaScript (`conformance/state-broker.mjs`) lo ejecuta aquí, paso a paso,
// este SDK con su API (`PutAsync`, `DeleteAsync`, `GetAsync`, `WatchAsync`) contra un nats-server REAL. Lo esperado es lo del fichero,
// no lo que este SDK prefiera: si el SDK discrepa del algoritmo de 10-state.md §3.2, el caso falla.
//
// Un solo nodo. El caso de un cluster de tres nodos (`cluster-three-owner-instances-converge`) NO se ejecuta aquí: el 10164 que da un R3 bajo
// carrera se prueba con una respuesta fabricada (StateTests.ElConflictoSeReconoceConLosDosCodigos), que es la alternativa que admite
// 10-state.md §10.2. Ver el informe.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Xunit;

namespace Flux.Tests.Integration;

[Collection("broker")]
public class StateSemanticsTests
{
    private static JsonArray Casos()
    {
        var ruta = Path.Combine(Prereq.RepoRoot, "conformance", "cases", "state-semantics.json");
        return JsonNode.Parse(File.ReadAllText(ruta))!["cases"]!.AsArray();
    }

    public static IEnumerable<object[]> Escenarios() =>
        Casos().Select(c => new object[] { c!["id"]!.GetValue<string>() }).Where(c => (string)c[0] != "cluster-three-owner-instances-converge");

    /// <summary>El fichero se lee de verdad: si el caso del cluster desapareciera, este test lo notaría.</summary>
    [BrokerFact]
    public void ElFicheroTieneLosDoceEscenarios()
    {
        var ids = Casos().Select(c => c!["id"]!.GetValue<string>()).ToList();
        Assert.Equal(12, ids.Count);
        Assert.Contains("cluster-three-owner-instances-converge", ids);
    }

    [BrokerTheory]
    [MemberData(nameof(Escenarios))]
    public async Task Escenario(string id)
    {
        Prereq.AssertAvailable();
        var caso = Casos().First(c => c!["id"]!.GetValue<string>() == id)!;
        await using var broker = await TestBroker.StartAsync((p, store, _) =>
            $"listen: 127.0.0.1:{p}\njetstream {{ store_dir: \"{store}\" }}\n");
        await using var mundo = new Mundo(broker);
        var fallos = new List<string>();
        var pasos = caso["steps"]!.AsArray();
        for (var i = 0; i < pasos.Count; i++)
        {
            var paso = pasos[i]!.AsObject();
            JsonObject observado;
            try
            {
                observado = await mundo.PasoAsync(paso);
            }
            catch (Exception e)
            {
                fallos.Add($"paso {i} ({paso["op"]}): lanzó {e.GetType().Name}: {e.Message}");
                continue;
            }

            if (paso["expect"] is JsonObject esperado)
            {
                foreach (var (ruta, valor) in esperado)
                {
                    var real = Camino(observado, ruta);
                    if (!Igual(valor, real))
                    {
                        fallos.Add($"paso {i} ({paso["op"]} {paso["title"]}): `{ruta}` esperaba {valor?.ToJsonString() ?? "null"} y salió {real?.ToJsonString() ?? "null"}");
                    }
                }
            }
        }

        Assert.True(fallos.Count == 0, $"{id}:\n  " + string.Join("\n  ", fallos));
    }

    private static JsonNode? Camino(JsonNode? raiz, string ruta)
    {
        var actual = raiz;
        foreach (var parte in ruta.Split('.'))
        {
            actual = actual switch
            {
                JsonObject o => o.TryGetPropertyValue(parte, out var v) ? v : null,
                JsonArray a when int.TryParse(parte, out var i) && i >= 0 && i < a.Count => a[i],
                _ => null,
            };
            if (actual is null)
            {
                return null;
            }
        }

        return actual;
    }

    private static bool Igual(JsonNode? esperado, JsonNode? real)
    {
        if (esperado is JsonObject o && o.Count == 1)
        {
            if (o.ContainsKey("gte"))
            {
                return real is not null && real.GetValue<double>() >= o["gte"]!.GetValue<double>();
            }

            if (o.ContainsKey("lte"))
            {
                return real is not null && real.GetValue<double>() <= o["lte"]!.GetValue<double>();
            }
        }

        return JsonNode.DeepEquals(esperado, real) ||
               (esperado is JsonValue ev && real is JsonValue rv && ev.ToJsonString() == rv.ToJsonString());
    }

    /// <summary>El «mundo» de un escenario: un broker abierto, un cliente de operación y un bus por servicio.</summary>
    private sealed class Mundo : IAsyncDisposable
    {
        private readonly TestBroker _broker;
        private readonly NatsConnection _ops;
        private readonly NatsJSContext _js;
        private readonly Dictionary<string, FluxBus> _buses = new();
        private readonly List<IAsyncDisposable> _watches = new();

        public Mundo(TestBroker broker)
        {
            _broker = broker;
            _ops = new NatsConnection(NatsOpts.Default with { Url = broker.Url, Name = "ops" });
            _js = new NatsJSContext(_ops);
        }

        private async Task<FluxBus> BusAsync(string servicio)
        {
            if (!_buses.TryGetValue(servicio, out var bus))
            {
                bus = await FluxBus.ConnectAsync(new ConnectOptions
                {
                    Servers = _broker.Url,
                    Service = servicio,
                    Environment = "test",
                    Version = "1.0.0",
                    SchemaBaseUrl = "https://schemas.internal",
                });
                _buses[servicio] = bus;
            }

            return bus;
        }

        public async Task<JsonObject> PasoAsync(JsonObject paso)
        {
            switch (paso["op"]!.GetValue<string>())
            {
                case "stream_add":
                    return await StreamAddAsync(paso);
                case "stream_state":
                {
                    var s = await _js.GetStreamAsync(paso["name"]!.GetValue<string>());
                    return new JsonObject
                    {
                        ["messages"] = (long)s.Info.State.Messages,
                        ["lastSeq"] = (long)s.Info.State.LastSeq,
                        ["firstSeq"] = (long)s.Info.State.FirstSeq,
                        ["numSubjects"] = (long)s.Info.State.NumSubjects,
                    };
                }

                case "state_put":
                case "state_delete":
                    return await EscribirAsync(paso);
                case "state_get":
                    return await LeerAsync(paso);
                case "state_race":
                    return await CarreraAsync(paso);
                case "state_watch":
                    return await WatchAsync(paso);
                default:
                    throw new InvalidOperationException("op desconocida: " + paso["op"]);
            }
        }

        private async Task<JsonObject> StreamAddAsync(JsonObject paso)
        {
            var nombre = paso["name"]!.GetValue<string>();
            var subjects = paso["subjects"]!.AsArray().Select(s => s!.GetValue<string>()).ToList();
            var replicas = paso["config"]?["num_replicas"]?.GetValue<int>() ?? 1;
            // La configuración de estado de referencia (10-state.md §2.1).
            await _js.CreateStreamAsync(new StreamConfig(nombre, subjects)
            {
                Storage = StreamConfigStorage.File,
                Retention = StreamConfigRetention.Limits,
                MaxMsgsPerSubject = 1,
                AllowDirect = true,
                MaxAge = TimeSpan.Zero,
                Discard = StreamConfigDiscard.Old,
                DuplicateWindow = TimeSpan.FromMinutes(2),
                NumReplicas = replicas,
            });
            return new JsonObject { ["outcome"] = "ok" };
        }

        private static JsonObject Datos(string key, JsonNode? version) =>
            new() { ["clienteId"] = key, ["nombre"] = "María Peña", ["aggregateVersion"] = version?.DeepClone() };

        private static JsonObject Datos(string key, long version) => Datos(key, JsonValue.Create(version));

        /// <summary><c>{repeat:x:N}</c> dentro de una cadena la repite N veces.</summary>
        private static string Expande(string s)
        {
            var i = s.IndexOf("{repeat:", StringComparison.Ordinal);
            if (i < 0)
            {
                return s;
            }

            var resto = s[(i + 8)..];
            var fin = resto.IndexOf('}');
            var partes = resto[..fin].Split(':');
            return s[..i] + string.Concat(Enumerable.Repeat(partes[0], int.Parse(partes[1]))) + resto[(fin + 1)..];
        }

        private async Task<JsonObject> EscribirAsync(JsonObject paso)
        {
            var contrato = paso["contract"]!.GetValue<string>();
            var key = Expande(paso["key"]!.GetValue<string>());
            var tenant = paso["tenant"]!.GetValue<string>();
            var version = paso["version"]!;
            var borrar = paso["op"]!.GetValue<string>() == "state_delete";
            var bus = await BusAsync("crm");
            var intentos = 0;
            var propia = false;
            var rival = await BusAsync("rival");
            var gancho = paso["hook"] as JsonObject;
            var lostAck = gancho?["lostAckOnce"]?.GetValue<bool>() == true;
            var antes = gancho?["beforePublish"]?.AsArray();
            bus.State.Hook = new WriteHook
            {
                IdOverride = paso["id"]?.GetValue<string>(),
                LoseAck = lostAck ? a => a == 1 : null,
                OnOwnWrite = _ => propia = true,
                OnDone = n => intentos = n,
                BeforePublish = antes is null
                    ? null
                    : async (a, _) =>
                    {
                        foreach (var h in antes)
                        {
                            if (h!["attempt"]!.GetValue<int>() == a)
                            {
                                await rival.State.PutAsync(contrato, key, Datos(key, h["fromVersion"]!.GetValue<long>()),
                                    new StatePutOptions { TenantId = tenant });
                            }
                        }
                    },
            };
            try
            {
                var r = borrar
                    ? await bus.State.DeleteAsync(contrato, key, new StateDeleteOptions { AggregateVersion = version.GetValue<long>(), TenantId = tenant })
                    : await bus.State.PutAsync(contrato, key, Datos(key, version), new StatePutOptions { TenantId = tenant });
                return new JsonObject
                {
                    ["outcome"] = r.Outcome == StateOutcome.Ok ? "ok" : "stale",
                    ["applied"] = r.Applied,
                    ["result"] = r.Outcome == StateOutcome.Ok ? "ok" : "stale",
                    ["revision"] = r.Revision,
                    ["attempts"] = intentos,
                    ["ownWrite"] = propia,
                };
            }
            catch (ClassifiedException e)
            {
                return new JsonObject { ["outcome"] = "error", ["errCode"] = e.FluxCode, ["text"] = e.Message, ["attempts"] = intentos };
            }
            finally
            {
                bus.State.Hook = null;
            }
        }

        private async Task<JsonObject> LeerAsync(JsonObject paso)
        {
            var bus = await BusAsync("crm");
            var contrato = paso["contract"]!.GetValue<string>();
            var key = paso["key"]!.GetValue<string>();
            var tenant = paso["tenant"]!.GetValue<string>();
            var e = await bus.State.GetAsync(contrato, key, new StateGetOptions { TenantId = tenant });
            if (e is not null)
            {
                return new JsonObject
                {
                    ["isNull"] = false, ["aggregateVersion"] = e.AggregateVersion, ["revision"] = e.Revision, ["deleted"] = false,
                    ["entry"] = new JsonObject { ["key"] = e.Key, ["tenantId"] = e.TenantId, ["deleted"] = e.Deleted, ["aggregateVersion"] = e.AggregateVersion, ["revision"] = e.Revision },
                };
            }

            // `null`: o no existe, o es una lápida. La API no distingue —`GetAsync` devuelve `null` en los dos casos, y es lo que
            // manda la spec—; el escenario sí, y se mira con una lectura cruda.
            var subject = StateProtocol.Subject(contrato, tenant, key);
            var crudo = await _ops.RequestAsync<byte[], byte[]>(
                $"$JS.API.DIRECT.GET.{StateProtocol.StreamName(contrato.Split('.')[0])}.{subject}",
                Array.Empty<byte>(),
                replyOpts: new NatsSubOpts { Timeout = TimeSpan.FromSeconds(5) });
            if (crudo.Headers?.Code == 404)
            {
                return new JsonObject { ["entry"] = null, ["isNull"] = true };
            }

            using var doc = JsonDocument.Parse(crudo.Data!);
            return new JsonObject
            {
                ["entry"] = null,
                ["isNull"] = true,
                ["aggregateVersion"] = doc.RootElement.GetProperty("data").GetProperty("aggregateVersion").GetInt64(),
                ["deleted"] = doc.RootElement.GetProperty("type").GetString()!.Contains(".deleted.", StringComparison.Ordinal),
            };
        }

        private async Task<JsonObject> CarreraAsync(JsonObject paso)
        {
            var contrato = paso["contract"]!.GetValue<string>();
            var key = paso["key"]!.GetValue<string>();
            var tenant = paso["tenant"]!.GetValue<string>();
            var n = paso["instances"]!.GetValue<int>();
            var versiones = Enumerable.Range(paso["from"]!.GetValue<int>(), paso["to"]!.GetValue<int>() - paso["from"]!.GetValue<int>() + 1)
                .OrderBy(_ => Random.Shared.Next()).ToList();
            var buses = new List<FluxBus>();
            for (var i = 0; i < n; i++)
            {
                buses.Add(await BusAsync("dueno-" + i));
            }

            int ok = 0, stale = 0, conflicto = 0, otro = 0;
            var tareas = buses.Select((b, i) => Task.Run(async () =>
            {
                foreach (var v in versiones.Where((_, j) => j % n == i))
                {
                    try
                    {
                        var r = await b.State.PutAsync(contrato, key, Datos(key, v), new StatePutOptions { TenantId = tenant });
                        Interlocked.Increment(ref r.Applied ? ref ok : ref stale);
                    }
                    catch (StateConflictException)
                    {
                        Interlocked.Increment(ref conflicto);
                    }
                    catch
                    {
                        Interlocked.Increment(ref otro);
                    }

                    await Task.Delay(Random.Shared.Next(0, 5));
                }
            }));
            await Task.WhenAll(tareas);
            var final = await buses[0].State.GetAsync(contrato, key, new StateGetOptions { TenantId = tenant });
            return new JsonObject
            {
                ["finalVersion"] = final?.AggregateVersion, ["ok"] = ok, ["stale"] = stale, ["conflict"] = conflicto, ["other"] = otro,
            };
        }

        private async Task<JsonObject> WatchAsync(JsonObject paso)
        {
            var contrato = paso["contract"]!.GetValue<string>();
            var durable = paso["durable"]!.GetValue<string>();
            var servicio = durable[..durable.IndexOf("__", StringComparison.Ordinal)];
            var tenant = paso["tenant"]?.GetValue<string>();
            var bus = await BusAsync(servicio);
            var entradas = new ConcurrentQueue<JsonObject>();
            var sincronizadoEn = -2;
            var contadas = 0;

            var despues = paso["afterCreate"]?.AsArray();
            bus.State.Hook = despues is null
                ? null
                : new WriteHook
                {
                    AfterOpen = async () =>
                    {
                        var dueno = await BusAsync("crm");
                        foreach (var w in despues)
                        {
                            await dueno.State.PutAsync(contrato, w!["key"]!.GetValue<string>(),
                                Datos(w["key"]!.GetValue<string>(), w["version"]!.GetValue<long>()),
                                new StatePutOptions { TenantId = tenant });
                        }
                    },
                };
            try
            {
                var w = await bus.State.WatchAsync(
                    contrato,
                    (e, _, _) =>
                    {
                        entradas.Enqueue(new JsonObject
                        {
                            ["key"] = e.Key,
                            ["tenantId"] = e.TenantId,
                            ["deleted"] = e.Deleted,
                            ["data"] = e.Data is { } d ? JsonNode.Parse(d.GetRawText()) : null,
                            ["aggregateVersion"] = e.AggregateVersion,
                            ["revision"] = e.Revision,
                        });
                        Interlocked.Increment(ref contadas);
                        return Task.CompletedTask;
                    },
                    new StateWatchOptions { TenantId = tenant, OnSynced = () => sincronizadoEn = Volatile.Read(ref contadas) - 1 });
                _watches.Add(w);
                Assert.Equal(durable, w.Durable);

                // Se drena: sincronizado y sin entregas nuevas durante un rato.
                var limite = Stopwatch.StartNew();
                var ultimo = -1;
                var estable = Stopwatch.StartNew();
                while (limite.Elapsed < TimeSpan.FromSeconds(15))
                {
                    await Task.Delay(50);
                    var ahora = Volatile.Read(ref contadas);
                    if (ahora != ultimo)
                    {
                        ultimo = ahora;
                        estable.Restart();
                    }

                    if (w.Synced.IsCompleted && estable.ElapsedMilliseconds > 400)
                    {
                        break;
                    }
                }

                var lista = entradas.ToList();
                var inmediato = sincronizadoEn == -1;
                return new JsonObject
                {
                    ["count"] = lista.Count,
                    ["keys"] = new JsonArray(lista.Select(e => (JsonNode?)JsonValue.Create(e["key"]!.GetValue<string>())).ToArray()),
                    ["entries"] = new JsonArray(lista.Select(e => (JsonNode?)JsonNode.Parse(e.ToJsonString())).ToArray()),
                    ["syncedAtIndex"] = sincronizadoEn == -2 ? null : sincronizadoEn,
                    ["syncedImmediately"] = inmediato,
                };
            }
            finally
            {
                bus.State.Hook = null;
            }
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var w in _watches)
            {
                await w.DisposeAsync();
            }

            foreach (var b in _buses.Values)
            {
                await b.DisposeAsync();
            }

            await _ops.DisposeAsync();
        }
    }
}
