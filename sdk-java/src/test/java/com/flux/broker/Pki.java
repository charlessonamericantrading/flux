/*
 * PKI efimera con `openssl`: una CA propia, una CA AJENA (que no firmo nada), un certificado de servidor y uno de
 * cliente (mTLS). Ficheros de configuracion y no `-addext`: `-addext` no existe en LibreSSL. Los certificados duran
 * dos dias y viven en un temporal.
 */
package com.flux.broker;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class Pki {

    public final Path dir;
    /** La CA que firma el servidor y el cliente. */
    public final Path ca;
    /** Una CA ajena: no firmo nada. Un cliente que confia en ella rechaza el certificado del servidor. */
    public final Path otherCa;
    public final Path serverCert;
    public final Path serverKey;
    public final Path clientCert;
    /** La clave del cliente en SEC1 (`BEGIN EC PRIVATE KEY`), como la deja `openssl ecparam -genkey`. */
    public final Path clientKey;
    /** La misma clave en PKCS#8 (`BEGIN PRIVATE KEY`). */
    public final Path clientKeyPkcs8;

    private Pki(Path dir) {
        this.dir = dir;
        this.ca = dir.resolve("ca.pem");
        this.otherCa = dir.resolve("otra-ca.pem");
        this.serverCert = dir.resolve("servidor.pem");
        this.serverKey = dir.resolve("servidor.key");
        this.clientCert = dir.resolve("cliente.pem");
        this.clientKey = dir.resolve("cliente.key");
        this.clientKeyPkcs8 = dir.resolve("cliente-pkcs8.key");
    }

    private static void openssl(Path cwd, String... args) throws IOException {
        String[] cmd = new String[args.length + 1];
        cmd[0] = "openssl";
        System.arraycopy(args, 0, cmd, 1, args.length);
        Process p = new ProcessBuilder(cmd).directory(cwd.toFile()).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        try {
            if (p.waitFor() != 0) {
                throw new IOException("openssl " + String.join(" ", args) + " fallo: " + out.trim());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private static void ca(Path dir, String name, String cn) throws IOException {
        Path cnf = dir.resolve(name + ".cnf");
        Files.writeString(cnf, "[req]\ndistinguished_name = dn\nprompt = no\n[dn]\nCN = " + cn
                + "\n[v3_ca]\nbasicConstraints = critical,CA:TRUE\nkeyUsage = critical,keyCertSign,cRLSign\n");
        openssl(dir, "ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", name + ".key");
        openssl(dir, "req", "-x509", "-new", "-key", name + ".key", "-sha256", "-days", "2", "-config", cnf.toString(),
                "-extensions", "v3_ca", "-out", name + ".pem");
    }

    private static void leaf(Path dir, String name, String cn, String san, String usage) throws IOException {
        Path ext = dir.resolve(name + ".ext");
        Files.writeString(ext, "subjectAltName = " + san + "\nbasicConstraints = CA:FALSE\nkeyUsage = digitalSignature\n"
                + "extendedKeyUsage = " + usage + "\n");
        openssl(dir, "ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", name + ".key");
        openssl(dir, "req", "-new", "-key", name + ".key", "-subj", "/CN=" + cn, "-out", name + ".csr");
        openssl(dir, "x509", "-req", "-in", name + ".csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial",
                "-days", "2", "-sha256", "-extfile", ext.toString(), "-out", name + ".pem");
    }

    /**
     * El certificado del servidor cubre {@code localhost} e {@code 127.0.0.1} y NADA mas: conectarse a otro host
     * (p. ej. {@code 127.0.0.2}) es «host fuera de los SAN».
     */
    public static Pki create(Path dir) throws IOException {
        Pki pki = new Pki(dir);
        ca(dir, "ca", "flux-test-ca");
        ca(dir, "otra-ca", "flux-otra-ca");
        leaf(dir, "servidor", "localhost", "DNS:localhost,IP:127.0.0.1", "serverAuth");
        leaf(dir, "cliente", "flux-cliente", "DNS:cliente", "clientAuth");
        openssl(dir, "pkcs8", "-topk8", "-nocrypt", "-in", "cliente.key", "-out", "cliente-pkcs8.key");
        return pki;
    }
}
