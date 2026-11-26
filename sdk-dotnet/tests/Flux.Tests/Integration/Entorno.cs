// El entorno seguro de los tests de integración: un nats-server REAL con la ACL que genera
// `scripts/gen-acl.mjs` (no una escrita a mano), identidades nkey por servicio y los streams y
// durables que crea `flux provision` con credenciales de operación.
//
// Es la misma forma de montar el broker que usan la suite de seguridad y los tests de Node y de
// Rust: dos copias de «cómo se monta el broker» acabarían divergiendo, y la que mintiera sería la
// que daba por buena una ACL distinta de la real.

using System.Diagnostics;
using System.Text;
using System.Text.Json;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace Flux.Tests.Integration;

/// <summary><c>allow_responses</c> de un dueño: <c>{ max, expires }</c>.</summary>
internal sealed record AllowResponses(int? Max, string? Expires);

/// <summary>Los permisos de un servicio, tal y como los afirma <c>gen-acl.mjs --json</c>.</summary>
internal sealed record Perms(IReadOnlyList<string> Publish, IReadOnlyList<string> Subscribe, AllowResponses? AllowResponses)
{
    /// <summary>Una copia sin los permisos de publicación que cumplen <paramref name="predicado"/>.</summary>
    public Perms SinPublicar(Func<string, bool> predicado) =>
        this with { Publish = Publish.Where(p => !predicado(p)).ToList() };

    /// <summary>Una copia sin los permisos de suscripción que cumplen <paramref name="predicado"/>.</summary>
    public Perms SinSuscribir(Func<string, bool> predicado) =>
        this with { Subscribe = Subscribe.Where(p => !predicado(p)).ToList() };
}

/// <summary>Un <see cref="IFluxLogger"/> que guarda lo que se le dice, para que los tests lo lean.</summary>
internal sealed class CapturingLogger : IFluxLogger
{
    private readonly List<string> _lines = new();

    /// <summary>Todo lo registrado (<c>WARN …</c> / <c>ERROR …</c>).</summary>
    public IReadOnlyList<string> Lines
    {
        get
        {
            lock (_lines)
            {
                return _lines.ToList();
            }
        }
    }

    /// <summary>Solo lo registrado a nivel de error.</summary>
    public IReadOnlyList<string> Errors => Lines.Where(l => l.StartsWith("ERROR ", StringComparison.Ordinal)).ToList();

    /// <inheritdoc />
    public void Warn(string message) => Add("WARN " + message);

    /// <inheritdoc />
    public void Error(string message) => Add("ERROR " + message);

    private void Add(string line)
    {
        lock (_lines)
        {
            _lines.Add(line);
        }
    }
}

/// <summary>Un broker con la ACL generada, identidades nkey y el aprovisionamiento de <c>flux provision</c>.</summary>
internal sealed class Entorno : IAsyncDisposable
{
    /// <summary>La semilla de operación: aprovisiona y mira, sin límites.</summary>
    private readonly string _opsSeed;

    private Entorno(
        TestBroker broker,
        string manifiesto,
        IReadOnlyDictionary<string, Perms> permisos,
        IReadOnlyDictionary<string, string> semillas,
        string opsSeed,
        string provision)
    {
        Broker = broker;
        Manifiesto = manifiesto;
        Permisos = permisos;
        Semillas = semillas;
        _opsSeed = opsSeed;
        Provision = provision;
    }

    /// <summary>El nats-server.</summary>
    public TestBroker Broker { get; }

    /// <summary>El manifiesto que se usó (<c>examples/services.json</c> por defecto).</summary>
    public string Manifiesto { get; }

    /// <summary>Los permisos de cada servicio, con las variantes ya aplicadas.</summary>
    public IReadOnlyDictionary<string, Perms> Permisos { get; }

    /// <summary>La semilla nkey de cada servicio.</summary>
    public IReadOnlyDictionary<string, string> Semillas { get; }

    /// <summary>La salida de <c>flux provision</c> (vacía si no se aprovisionó).</summary>
    public string Provision { get; }

    /// <summary>La URL del broker.</summary>
    public string Url => Broker.Url;

    /// <summary>
    /// Monta el entorno.
    /// </summary>
    /// <param name="variante">
    /// Cambia los permisos de un servicio ANTES de cargarlos en el broker: así se «quita un permiso» y se
    /// comprueba que se nota. Devuelve los permisos nuevos (o los mismos).
    /// </param>
    /// <param name="aprovisionar">Si se ejecuta <c>flux provision</c>. Sin él, nadie crea nada: es el broker sin aprovisionar.</param>
    /// <param name="manifiesto">Otro manifiesto; por defecto el del ejemplo del repo.</param>
    /// <param name="genExtra">Argumentos de <c>gen-acl.mjs</c>, p. ej. <c>--dev</c>.</param>
    public static async Task<Entorno> CrearAsync(
        Func<string, Perms, Perms>? variante = null,
        bool aprovisionar = true,
        string? manifiesto = null,
        params string[] genExtra)
    {
        Prereq.AssertAvailable(conNode: true);
        var raiz = Prereq.RepoRoot;
        manifiesto ??= Path.Combine(raiz, "examples", "services.json");

        var generados = await GenAclAsync(manifiesto, genExtra).ConfigureAwait(false);
        var permisos = new Dictionary<string, Perms>(StringComparer.Ordinal);
        var semillas = new Dictionary<string, string>(StringComparer.Ordinal);
        var publicas = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (nombre, perms) in generados)
        {
            var p = variante?.Invoke(nombre, perms) ?? perms;
            permisos[nombre] = p;
            var semilla = NKeys.CreateUserSeed();
            semillas[nombre] = semilla;
            publicas[nombre] = NKeys.PublicKeyFromSeed(semilla);
        }

        var opsSeed = NKeys.CreateUserSeed();
        var opsPublica = NKeys.PublicKeyFromSeed(opsSeed);

        var broker = await TestBroker.StartAsync((puerto, store, _) =>
        {
            var sb = new StringBuilder();
            sb.Append($"listen: 127.0.0.1:{puerto}\njetstream {{ store_dir: \"{store}\" }}\nauthorization {{\n  users: [\n");
            sb.Append($"    {{ nkey: \"{opsPublica}\" }}\n");
            foreach (var (nombre, p) in permisos)
            {
                sb.Append(UsuarioConf(publicas[nombre], p));
            }

            sb.Append("  ]\n}\n");
            return sb.ToString();
        }).ConfigureAwait(false);

        try
        {
            var salida = string.Empty;
            if (aprovisionar)
            {
                salida = await ProvisionAsync(manifiesto, broker.Url, opsSeed, broker.Dir).ConfigureAwait(false);
            }

            return new Entorno(broker, manifiesto, permisos, semillas, opsSeed, salida);
        }
        catch
        {
            await broker.StopAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Las opciones de <c>ConnectAsync</c> de un servicio del manifiesto, con su identidad nkey.</summary>
    public ConnectOptions Opciones(string servicio, CapturingLogger? logger = null, string tenant = "acme") =>
        new()
        {
            Servers = Url,
            Service = servicio,
            Environment = "test",
            Version = "1.0.0",
            TenantId = tenant,
            SchemaBaseUrl = "https://schemas.internal",
            Credentials = new NatsCredentials { NkeySeed = Semillas[servicio] },
            Logger = logger,
        };

    /// <summary>Una conexión cruda de operación (sin el SDK): para aprovisionar y mirar.</summary>
    public async Task<(NatsConnection Nc, NatsJSContext Js)> AbrirOpsAsync()
    {
        var nc = new NatsConnection(NatsOpts.Default with
        {
            Url = Url,
            Name = "ops-test",
            AuthOpts = new NatsAuthOpts { NKey = NKeys.PublicKeyFromSeed(_opsSeed), Seed = _opsSeed },
        });
        await nc.ConnectAsync().ConfigureAwait(false);
        return (nc, new NatsJSContext(nc));
    }

    /// <summary>Las violaciones de permiso que el servidor ha anotado en su log.</summary>
    public IReadOnlyList<string> Violaciones() =>
        Broker.Log().Split('\n').Where(l => l.Contains("Permissions Violation", StringComparison.Ordinal)).ToList();

    /// <inheritdoc />
    public ValueTask DisposeAsync() => Broker.DisposeAsync();

    // ─── gen-acl y provision ─────────────────────────────────────────────────

    /// <summary>Lo que afirma <c>node scripts/gen-acl.mjs &lt;manifiesto&gt; --json</c>, por servicio.</summary>
    public static async Task<IReadOnlyList<KeyValuePair<string, Perms>>> GenAclAsync(string manifiesto, params string[] extra)
    {
        var args = new List<string> { Path.Combine(Prereq.RepoRoot, "scripts", "gen-acl.mjs"), manifiesto, "--json" };
        args.AddRange(extra);
        var (codigo, salida, errores) = await Ejecuta(Prereq.Node!, args, Prereq.RepoRoot, null).ConfigureAwait(false);
        if (codigo != 0)
        {
            throw new InvalidOperationException($"gen-acl.mjs falló ({codigo}):\n{errores}");
        }

        using var doc = JsonDocument.Parse(salida);
        var lista = new List<KeyValuePair<string, Perms>>();
        foreach (var servicio in doc.RootElement.EnumerateArray())
        {
            AllowResponses? allow = null;
            if (servicio.TryGetProperty("allowResponses", out var ar) && ar.ValueKind == JsonValueKind.Object)
            {
                allow = new AllowResponses(
                    ar.TryGetProperty("max", out var max) ? max.GetInt32() : null,
                    ar.TryGetProperty("expires", out var exp) ? exp.GetString() : null);
            }

            lista.Add(new(
                servicio.GetProperty("name").GetString()!,
                new Perms(
                    servicio.GetProperty("publish").EnumerateArray().Select(e => e.GetString()!).ToList(),
                    servicio.GetProperty("subscribe").EnumerateArray().Select(e => e.GetString()!).ToList(),
                    allow)));
        }

        return lista;
    }

    /// <summary>
    /// <c>flux provision</c> con credenciales de OPERACIÓN. Es la CLI real: crea los streams y los
    /// durables con las configuraciones canónicas de <c>protocol.json</c>, que son lo que las ACLs presuponen.
    /// </summary>
    public static async Task<string> ProvisionAsync(string manifiesto, string url, string opsSeed, string dir)
    {
        var creds = Path.Combine(dir, "ops.creds");
        await File.WriteAllTextAsync(
            creds,
            "-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QifQ.e30.relleno\n------END NATS USER JWT------\n\n" +
            $"-----BEGIN USER NKEY SEED-----\n{opsSeed}\n------END USER NKEY SEED------\n").ConfigureAwait(false);

        var cli = Path.Combine(Prereq.RepoRoot, "cli", "src", "index.mjs");
        var (codigo, salida, errores) = await Ejecuta(
            Prereq.Node!,
            new[] { cli, "provision", manifiesto, "--server", url, "--creds", creds },
            Prereq.RepoRoot,
            new Dictionary<string, string> { ["NO_COLOR"] = "1" }).ConfigureAwait(false);
        if (codigo != 0)
        {
            throw new InvalidOperationException($"flux provision falló ({codigo}):\n{salida}\n{errores}");
        }

        return salida;
    }

    private static string List(IEnumerable<string> xs) => "[" + string.Join(", ", xs.Select(x => "\"" + x + "\"")) + "]";

    /// <summary>
    /// Un usuario nkey con permisos, escrito como lo escribe <c>gen-acl.mjs</c>. OJO: una lista vacía
    /// NO deniega nada —<c>allow: []</c> es «sin restricción»— (A-25), así que un permiso que se quita
    /// de una lista que se queda vacía no deniega: los tests nunca vacían una lista.
    /// </summary>
    private static string UsuarioConf(string publica, Perms p)
    {
        var sb = new StringBuilder();
        sb.Append($"    {{ nkey: \"{publica}\"\n      permissions: {{\n");
        sb.Append($"        publish: {{ allow: {List(p.Publish)} }}\n");
        sb.Append($"        subscribe: {{ allow: {List(p.Subscribe)} }}\n");
        if (p.AllowResponses is { } ar)
        {
            sb.Append("        allow_responses: { max: " + (ar.Max ?? 1) +
                      (ar.Expires is null ? string.Empty : $", expires: \"{ar.Expires}\"") + " }\n");
        }

        sb.Append("      } }\n");
        return sb.ToString();
    }

    private static async Task<(int Codigo, string Salida, string Errores)> Ejecuta(
        string programa,
        IEnumerable<string> args,
        string cwd,
        IReadOnlyDictionary<string, string>? env)
    {
        var psi = new ProcessStartInfo(programa)
        {
            WorkingDirectory = cwd,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (var a in args)
        {
            psi.ArgumentList.Add(a);
        }

        if (env is not null)
        {
            foreach (var (k, v) in env)
            {
                psi.Environment[k] = v;
            }
        }

        using var p = Process.Start(psi)!;
        var salida = p.StandardOutput.ReadToEndAsync();
        var errores = p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync().ConfigureAwait(false);
        return (p.ExitCode, await salida.ConfigureAwait(false), await errores.ConfigureAwait(false));
    }
}
