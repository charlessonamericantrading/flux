// Certificados de prueba, generados con `openssl` por el propio test.
//
// 06-security.md §2: TLS es obligatorio en el listener, y el plan v1.1 exige que los SDKs acepten
// una CA propia. Cada caso va en los dos sentidos: con la CA correcta conecta; con cualquier otra
// cosa `ConnectAsync` FALLA pronto y apuntando a `Tls.CaFile`.

using System.Diagnostics;

namespace Flux.Tests.Integration;

/// <summary>Una CA, otra ajena, y certificados de servidor, de cliente y uno con un SAN que no cubre 127.0.0.1.</summary>
internal sealed class Pki : IDisposable
{
    private Pki(string dir) => Dir = dir;

    /// <summary>Directorio con los ficheros.</summary>
    public string Dir { get; }

    /// <summary>Ruta de un fichero del directorio.</summary>
    public string F(string nombre) => Path.Combine(Dir, nombre);

    /// <summary>La CA que firma todo lo demás.</summary>
    public string Ca => F("ca.pem");

    /// <summary>Una CA que NO firmó nada.</summary>
    public string CaAjena => F("ca-ajena.pem");

    /// <summary>Genera la PKI.</summary>
    public static async Task<Pki> CrearAsync()
    {
        Prereq.AssertAvailable(conOpenSsl: true);
        var pki = new Pki(Directory.CreateTempSubdirectory("flux-pki-").FullName);
        await pki.CaAsync("ca").ConfigureAwait(false);
        await pki.CaAsync("ca-ajena").ConfigureAwait(false);
        await pki.EmitirAsync("server", "localhost", "DNS:localhost,IP:127.0.0.1").ConfigureAwait(false);
        await pki.EmitirAsync("cliente", "cliente-de-prueba", "DNS:cliente").ConfigureAwait(false);
        await pki.EmitirAsync("altname", "otro.example", "DNS:otro.example").ConfigureAwait(false);
        return pki;
    }

    private Task CaAsync(string nombre) =>
        OpenSsl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", F($"{nombre}.key"), "-out", F($"{nombre}.pem"),
            "-days", "2", "-subj", $"/CN=flux-{nombre}");

    private async Task EmitirAsync(string nombre, string cn, string san)
    {
        await File.WriteAllTextAsync(F($"{nombre}.ext"), $"subjectAltName={san}\nbasicConstraints=CA:FALSE\n").ConfigureAwait(false);
        await OpenSsl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", F($"{nombre}.key"), "-out", F($"{nombre}.csr"),
            "-subj", $"/CN={cn}").ConfigureAwait(false);
        await OpenSsl("x509", "-req", "-in", F($"{nombre}.csr"), "-CA", F("ca.pem"), "-CAkey", F("ca.key"), "-CAcreateserial",
            "-out", F($"{nombre}.pem"), "-days", "2", "-extfile", F($"{nombre}.ext")).ConfigureAwait(false);
    }

    private async Task OpenSsl(params string[] args)
    {
        var psi = new ProcessStartInfo(Prereq.OpenSsl!)
        {
            WorkingDirectory = Dir,
            RedirectStandardError = true,
            RedirectStandardOutput = true,
        };
        foreach (var a in args)
        {
            psi.ArgumentList.Add(a);
        }

        using var p = Process.Start(psi)!;
        var errores = p.StandardError.ReadToEndAsync();
        _ = p.StandardOutput.ReadToEndAsync();
        await p.WaitForExitAsync().ConfigureAwait(false);
        if (p.ExitCode != 0)
        {
            throw new InvalidOperationException($"openssl {string.Join(' ', args)} falló:\n{await errores.ConfigureAwait(false)}");
        }
    }

    /// <summary>
    /// La config de un listener con TLS. <paramref name="cert"/>: <c>server</c> (cubre 127.0.0.1) o
    /// <c>altname</c> (no lo cubre). <paramref name="verify"/>: exige certificado de cliente (mTLS).
    /// </summary>
    public string Tls(string cert, bool verify) =>
        $"tls {{ cert_file: \"{F($"{cert}.pem")}\", key_file: \"{F($"{cert}.key")}\", ca_file: \"{F("ca.pem")}\", verify: {(verify ? "true" : "false")} }}\n";

    /// <inheritdoc />
    public void Dispose()
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
