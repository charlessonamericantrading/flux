// Un `nats-server` REAL para los tests de integración.
//
// Los permisos que prueba el SDK son los del broker: un mock no los aplica, y el plan v1.1
// existe precisamente porque el modelo de permisos «nunca se había ejecutado». Por eso aquí no
// hay mocks.
//
// Sin binario, estos tests se SALTAN con aviso — salvo con `FLUX_REQUIRE_BROKER=1`, donde
// FALLAN. Es lo que impide que el CI dé verde sin haberlos ejecutado nunca: un fixture que nadie
// ejecuta aparenta cobertura.
//
//   NATS_SERVER=/ruta/a/nats-server        binario (si no, se busca en el PATH)
//   FLUX_REQUIRE_BROKER=1                  su ausencia es un FALLO, no un salto
//   FLUX_TEST_PORTS=7500-7599              rango de puertos (por defecto, 4800-4899)

using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using Xunit;

namespace Flux.Tests.Integration;

/// <summary>Los requisitos de los tests de integración y qué hacer si faltan.</summary>
internal static class Prereq
{
    /// <summary><c>FLUX_REQUIRE_BROKER</c> definida y distinta de vacío, <c>0</c> o <c>false</c>.</summary>
    public static bool Required
    {
        get
        {
            var v = (Environment.GetEnvironmentVariable("FLUX_REQUIRE_BROKER") ?? string.Empty).Trim().ToLowerInvariant();
            return v is not ("" or "0" or "false");
        }
    }

    /// <summary>Ruta del <c>nats-server</c>, o <see langword="null"/>.</summary>
    public static string? NatsServer
    {
        get
        {
            var explicito = Environment.GetEnvironmentVariable("NATS_SERVER");
            if (!string.IsNullOrEmpty(explicito))
            {
                // Si lo indican y no existe, NO se cae al PATH: usar otro binario sin avisar sería
                // probar contra algo distinto de lo que se cree.
                return File.Exists(explicito) ? explicito : null;
            }

            return OnPath("nats-server");
        }
    }

    /// <summary>Ruta de <c>node</c> (para <c>gen-acl.mjs</c> y <c>flux provision</c>), o <see langword="null"/>.</summary>
    public static string? Node => OnPath("node");

    /// <summary>Ruta de <c>openssl</c> (para los certificados de prueba), o <see langword="null"/>.</summary>
    public static string? OpenSsl => OnPath("openssl");

    /// <summary>La raíz del repositorio (donde están <c>protocol.json</c> y <c>scripts/</c>).</summary>
    public static string RepoRoot
    {
        get
        {
            for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
            {
                if (File.Exists(Path.Combine(dir.FullName, "protocol.json")) &&
                    File.Exists(Path.Combine(dir.FullName, "scripts", "gen-acl.mjs")))
                {
                    return dir.FullName;
                }
            }

            throw new InvalidOperationException("no se encuentra la raíz del repositorio (protocol.json)");
        }
    }

    /// <summary>
    /// Por qué faltan requisitos, o <see langword="null"/> si están todos. <paramref name="conNode"/>:
    /// el test usa el generador de ACL y la CLI; <paramref name="conOpenSsl"/>: genera certificados.
    /// </summary>
    public static string? Missing(bool conNode = false, bool conOpenSsl = false)
    {
        if (NatsServer is null)
        {
            return "no hay `nats-server` en el PATH ni NATS_SERVER definida (o apunta a algo que no existe)";
        }

        if (conNode && Node is null)
        {
            return "no hay `node` en el PATH: hace falta para scripts/gen-acl.mjs y `flux provision`";
        }

        if (conNode && !Directory.Exists(Path.Combine(RepoRoot, "cli", "node_modules")))
        {
            return "falta `cli/node_modules` (`npm install` en cli/): `flux provision` lo necesita";
        }

        if (conOpenSsl && OpenSsl is null)
        {
            return "no hay `openssl` en el PATH: hace falta para generar los certificados de prueba";
        }

        return null;
    }

    private static string? OnPath(string name)
    {
        foreach (var dir in (Environment.GetEnvironmentVariable("PATH") ?? string.Empty).Split(Path.PathSeparator))
        {
            if (string.IsNullOrEmpty(dir))
            {
                continue;
            }

            var candidate = Path.Combine(dir, name);
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        return null;
    }

    /// <summary>
    /// Falla si faltan requisitos y el entorno exige broker. Los tests lo llaman antes de
    /// arrancar nada: con <c>FLUX_REQUIRE_BROKER</c> una ausencia es un FALLO ruidoso, y sin ella
    /// el atributo ya los habrá saltado.
    /// </summary>
    public static void AssertAvailable(bool conNode = false, bool conOpenSsl = false)
    {
        var falta = Missing(conNode, conOpenSsl);
        if (falta is not null)
        {
            throw new Xunit.Sdk.XunitException(
                $"{falta}. Con FLUX_REQUIRE_BROKER los tests de integración son obligatorios: saltarlos " +
                "daría un CI verde sin haber ejecutado nada contra el broker.");
        }
    }
}

/// <summary>
/// <c>[Fact]</c> de un test que necesita un <c>nats-server</c> real: se salta con aviso si falta el
/// binario, salvo con <c>FLUX_REQUIRE_BROKER=1</c>, donde se ejecuta y falla.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class BrokerFactAttribute : FactAttribute
{
    /// <summary>Crea el atributo. <paramref name="node"/> y <paramref name="openssl"/>: requisitos extra.</summary>
    public BrokerFactAttribute(bool node = false, bool openssl = false)
    {
        var falta = Prereq.Missing(node, openssl);
        if (falta is not null && !Prereq.Required)
        {
            Skip = $"se SALTA: {falta}. Define NATS_SERVER, o FLUX_REQUIRE_BROKER=1 para que su ausencia sea un fallo.";
        }
    }
}

/// <summary>Un <c>nats-server</c> con su configuración y su directorio temporal. Al soltarlo mata SOLO su proceso.</summary>
internal sealed class TestBroker : IAsyncDisposable
{
    private readonly Process _process;
    private readonly List<string> _lines;
    private bool _ownsDir;

    private TestBroker(Process process, int port, string dir, List<string> lines, bool ownsDir)
    {
        _process = process;
        Port = port;
        Dir = dir;
        _lines = lines;
        _ownsDir = ownsDir;
    }

    /// <summary>Puerto del listener de clientes.</summary>
    public int Port { get; }

    /// <summary>Directorio temporal propio (almacén de JetStream, certificados…).</summary>
    public string Dir { get; }

    /// <summary><c>nats://127.0.0.1:&lt;puerto&gt;</c>.</summary>
    public string Url => $"nats://127.0.0.1:{Port}";

    /// <summary>El log del servidor hasta ahora.</summary>
    public string Log()
    {
        lock (_lines)
        {
            return string.Join('\n', _lines);
        }
    }

    /// <summary>El rango de <c>FLUX_TEST_PORTS</c> (<c>desde-hasta</c>).</summary>
    public static (int From, int To) PortRange()
    {
        var v = Environment.GetEnvironmentVariable("FLUX_TEST_PORTS") ?? "4800-4899";
        var partes = v.Split('-');
        if (partes.Length != 2 || !int.TryParse(partes[0], out var desde) || !int.TryParse(partes[1], out var hasta))
        {
            throw new InvalidOperationException("FLUX_TEST_PORTS debe ser `desde-hasta`, por ejemplo 7500-7599");
        }

        return (desde, hasta);
    }

    /// <summary>
    /// Arranca un nats-server con la config que escribe <paramref name="config"/>. El puerto se
    /// elige al azar dentro del rango y se REINTENTA si el servidor no consigue escuchar en él. Se da
    /// por bueno solo cuando el log DEL PROPIO servidor dice que escucha en ese puerto: comprobar que
    /// el puerto responde engañaría si otro proceso ya lo ocupaba.
    /// </summary>
    /// <param name="config">Recibe <c>(puerto, almacén, directorio)</c> y devuelve la config.</param>
    /// <param name="port">Un puerto concreto, sin reintentar con otro (para reabrir uno que se acaba de cerrar).</param>
    /// <param name="dir">Directorio ya creado donde dejar los ficheros (certificados…); si no, uno nuevo.</param>
    public static async Task<TestBroker> StartAsync(
        Func<int, string, string, string> config,
        int? port = null,
        string? dir = null)
    {
        var binario = Prereq.NatsServer ?? throw new InvalidOperationException("no hay nats-server");
        var (desde, hasta) = PortRange();
        var propio = dir is null;
        dir ??= Directory.CreateTempSubdirectory("flux-nats-").FullName;
        var intentos = port is not null ? 1 : Math.Min(30, hasta - desde + 1);

        for (var i = 0; i < intentos; i++)
        {
            var elegido = port ?? Random.Shared.Next(desde, hasta + 1);
            var store = Path.Combine(dir, $"js-{elegido}");
            var conf = Path.Combine(dir, $"nats-{elegido}.conf");
            await File.WriteAllTextAsync(conf, config(elegido, store, dir)).ConfigureAwait(false);

            var psi = new ProcessStartInfo(binario) { RedirectStandardError = true, RedirectStandardOutput = true };
            psi.ArgumentList.Add("-c");
            psi.ArgumentList.Add(conf);
            var process = new Process { StartInfo = psi };
            var lineas = new List<string>();
            void Recoge(object _, DataReceivedEventArgs e)
            {
                if (e.Data is not null)
                {
                    lock (lineas)
                    {
                        lineas.Add(e.Data);
                    }
                }
            }

            process.ErrorDataReceived += Recoge;
            process.OutputDataReceived += Recoge;
            process.Start();
            process.BeginErrorReadLine();
            process.BeginOutputReadLine();

            var limite = DateTime.UtcNow.AddSeconds(10);
            var listo = false;
            while (DateTime.UtcNow < limite)
            {
                if (process.HasExited)
                {
                    break;
                }

                string[] copia;
                lock (lineas)
                {
                    copia = lineas.ToArray();
                }

                if (copia.Any(l => l.Contains($":{elegido}", StringComparison.Ordinal) &&
                                   l.Contains("Listening for client connections", StringComparison.Ordinal)) &&
                    copia.Any(l => l.Contains("Server is ready", StringComparison.Ordinal)))
                {
                    listo = true;
                    break;
                }

                await Task.Delay(25).ConfigureAwait(false);
            }

            if (listo)
            {
                // El log lo sigue llenando el mismo handler, sobre la misma lista.
                return new TestBroker(process, elegido, dir, lineas, propio);
            }

            try
            {
                if (!process.HasExited)
                {
                    process.Kill(entireProcessTree: true);
                }
            }
            catch (InvalidOperationException)
            {
            }

            string log;
            lock (lineas)
            {
                log = string.Join('\n', lineas);
            }

            if (!Regex.IsMatch(log, "address already in use|Error listening", RegexOptions.IgnoreCase))
            {
                // No es un puerto ocupado: es la config. Reintentar con otro puerto la repetiría.
                if (propio)
                {
                    Directory.Delete(dir, recursive: true);
                }

                throw new InvalidOperationException($"nats-server no arrancó con la config de test:\n{log}");
            }
        }

        if (propio)
        {
            Directory.Delete(dir, recursive: true);
        }

        throw new InvalidOperationException($"no se encontró un puerto libre en {desde}-{hasta}");
    }

    /// <summary>Para el servidor (SIGTERM, y SIGKILL si no obedece) y borra el directorio si es suyo.</summary>
    public async ValueTask StopAsync()
    {
        if (!_process.HasExited)
        {
            try
            {
                _process.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException)
            {
            }

            await _process.WaitForExitAsync().ConfigureAwait(false);
        }

        if (_ownsDir)
        {
            try
            {
                Directory.Delete(Dir, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => StopAsync();
}
