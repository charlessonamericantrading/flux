/**
 * TLS del SDK contra un nats-server REAL, con certificados generados por el propio test.
 *
 * 06-security.md §2: TLS es obligatorio en el listener, y el plan v1.1 exige que los SDKs
 * acepten una CA propia. Cada caso va en los dos sentidos: con la CA correcta conecta; con
 * cualquier otra cosa `connect()` FALLA pronto y apuntando a `tls.caFile`. Sin esto último,
 * un certificado que no verifica dejaba `connect()` esperando para siempre (medido).
 */

import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { join } from "node:path";

import { connect as crudo } from "@nats-io/transport-node";

import { connect, ConnectionRejectedError, type ConnectOptions } from "../../src/index.js";
import { describeBroker, dormir, esperarHasta, startBroker, type Broker } from "./broker.js";

function falta(): string | undefined {
  try {
    execFileSync("openssl", ["version"], { stdio: "pipe" });
    return undefined;
  } catch {
    return "no hay `openssl` en el PATH: hace falta para generar los certificados de prueba";
  }
}

describeBroker(
  "SDK Node: TLS con CA propia",
  () => {
    let dir: string;
    let tls: Broker; // TLS, sin verificar cliente
    let mtls: Broker; // TLS y certificado de cliente obligatorio
    let plano: Broker; // sin TLS
    let altnameMalo: Broker; // TLS con un certificado que no cubre 127.0.0.1
    const f = (n: string) => join(dir, n);

    const base = (b: Broker): ConnectOptions => ({
      servers: b.url,
      service: "fx-tls",
      environment: "test",
      version: "1.0.0",
      schemaBaseUrl: "https://schemas.internal",
    });

    /** Un `connect()` que debe ser RECHAZADO por TLS, y pronto. */
    async function rechazadoPorTls(opts: ConnectOptions): Promise<ConnectionRejectedError> {
      const t0 = Date.now();
      let error: unknown;
      await connect(opts).then(
        (bus) => bus.close(),
        (e) => void (error = e),
      );
      assert.ok(error, "connect() no falló: aceptó un certificado que no debía");
      assert.ok(error instanceof ConnectionRejectedError, `lanzó ${(error as Error).constructor.name}: ${(error as Error).message}`);
      assert.equal(error.reason, "tls");
      assert.ok(Date.now() - t0 < 3000, `tardó ${Date.now() - t0} ms: parece que reintenta en vez de fallar`);
      return error;
    }

    const openssl = (...args: string[]) => execFileSync("openssl", args, { stdio: "pipe" });
    function ca(nombre: string): void {
      openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", f(`${nombre}.key`), "-out", f(`${nombre}.pem`), "-days", "2", "-subj", `/CN=flux-${nombre}`);
    }
    function emitir(nombre: string, cn: string, san: string, firmante = "ca"): void {
      writeFileSync(f(`${nombre}.ext`), `subjectAltName=${san}\nbasicConstraints=CA:FALSE\n`);
      openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", f(`${nombre}.key`), "-out", f(`${nombre}.csr`), "-subj", `/CN=${cn}`);
      openssl("x509", "-req", "-in", f(`${nombre}.csr`), "-CA", f(`${firmante}.pem`), "-CAkey", f(`${firmante}.key`), "-CAcreateserial", "-out", f(`${nombre}.pem`), "-days", "2", "-extfile", f(`${nombre}.ext`));
    }

    before(async () => {
      // El broker se arranca primero solo para disponer del directorio temporal propio.
      plano = await startBroker(({ port, store }) => `listen: 127.0.0.1:${port}\njetstream { store_dir: "${store}" }\n`);
      dir = plano.dir;
      ca("ca");
      ca("ca-ajena");
      emitir("server", "localhost", "DNS:localhost,IP:127.0.0.1");
      emitir("cliente", "cliente-de-prueba", "DNS:cliente");
      emitir("altname", "otro.example", "DNS:otro.example");

      const conf = (cert: string, verify: boolean) => ({ port, store }: { port: number; store: string }) => `listen: 127.0.0.1:${port}
jetstream { store_dir: "${store}" }
tls { cert_file: "${f(`${cert}.pem`)}", key_file: "${f(`${cert}.key`)}", ca_file: "${f("ca.pem")}", verify: ${verify} }
`;
      tls = await startBroker(conf("server", false));
      mtls = await startBroker(conf("server", true));
      altnameMalo = await startBroker(conf("altname", false));
    });

    after(async () => {
      for (const b of [tls, mtls, altnameMalo, plano]) await b?.stop();
    });

    test("control: el listener EXIGE TLS (una conexión sin TLS es rechazada por el propio servidor)", async () => {
      await assert.rejects(
        crudo({ servers: tls.url, tls: null, waitOnFirstConnect: false, timeout: 2000 }),
        /tls/i,
      );
    });

    test("con `tls.caFile` conecta, publica y la conexión va cifrada", async () => {
      const bus = await connect({ ...base(tls), tls: { caFile: f("ca.pem") } });
      try {
        assert.ok((await bus.publish("tls.cosa.v1.hecha", { n: 1 })).id);
      } finally {
        await bus.close();
      }
    });

    test("sin `tls` (el servidor lo exige): el certificado no verifica → falla pronto y apunta a tls.caFile", async () => {
      const r = await rechazadoPorTls(base(tls));
      assert.match(r.message, /tls\.caFile/);
      assert.match(r.message, /UNABLE_TO_VERIFY_LEAF_SIGNATURE|SELF_SIGNED|ISSUER/);
    });

    test("`tls: {}` verifica contra las CA del sistema, que no conocen la CA de prueba → falla", async () => {
      await rechazadoPorTls({ ...base(tls), tls: {} });
    });

    test("con la CA EQUIVOCADA falla: la CA indicada es la que manda, no se mezcla con nada más", async () => {
      await rechazadoPorTls({ ...base(tls), tls: { caFile: f("ca-ajena.pem") } });
    });

    test("un certificado que no cubre el host (SAN) falla aunque la CA sea la correcta", async () => {
      const r = await rechazadoPorTls({ ...base(altnameMalo), tls: { caFile: f("ca.pem") } });
      assert.match(r.message, /ERR_TLS_CERT_ALTNAME_INVALID/);
    });

    test("se pide `tls` y el servidor no lo ofrece: falla, y NO baja a texto plano", async () => {
      const r = await rechazadoPorTls({ ...base(plano), tls: { caFile: f("ca.pem") } });
      assert.match(r.message, /no ofrece TLS/);
    });

    describe("mTLS (el servidor exige certificado de cliente)", () => {
      test("con `caFile` + `certFile` + `keyFile` conecta y publica", async () => {
        const bus = await connect({
          ...base(mtls),
          tls: { caFile: f("ca.pem"), certFile: f("cliente.pem"), keyFile: f("cliente.key") },
        });
        try {
          assert.ok((await bus.publish("mtls.cosa.v1.hecha", { n: 1 })).id);
        } finally {
          await bus.close();
        }
      });

      // Sin certificado de cliente la librería NO da un error de TLS: el servidor corta el
      // handshake y el cliente solo ve que no llega nada (un timeout). Por eso este sentido
      // se comprueba en el SERVIDOR: es su log el que dice que rechazó, y es lo que importa.
      test("sin certificado de cliente el SERVIDOR lo rechaza (y el cliente solo ve un timeout)", async () => {
        await assert.rejects(
          crudo({ servers: mtls.url, tls: { caFile: f("ca.pem") }, waitOnFirstConnect: false, timeout: 1500 }),
          (e: Error) => e.name === "TimeoutError",
        );
        assert.ok(
          await esperarHasta(() => /client didn't provide a certificate|certificate required/i.test(mtls.log()), 2000),
          `el servidor no registró el rechazo:\n${mtls.log()}`,
        );
        await dormir(0);
      });
    });
  },
  falta,
);
