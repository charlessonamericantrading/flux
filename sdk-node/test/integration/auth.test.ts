/**
 * Autenticación del SDK contra brokers REALES: nkey, usuario y contraseña, token y `.creds`.
 *
 * Hallazgo H4 del plan v1.1: `credentials.creds` se esparcía en una opción que el cliente
 * NATS no conoce y se ignoraba en silencio. Cada método se prueba en los dos sentidos: con
 * la credencial correcta conecta y publica; con una incorrecta el broker la rechaza y
 * `connect()` FALLA —no se cuelga reintentando—.
 */

import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { nkeys } from "@nats-io/transport-node";
import { jetstreamManager } from "@nats-io/jetstream";

import { connect, ConnectionRejectedError, type ConnectOptions, type FluxBus } from "../../src/index.js";
import { conectarCrudo, describeBroker, dormir, startBroker, type Broker } from "./broker.js";
import { crearEntornoJwt } from "./jwt.js";

const base = (broker: Broker, service = "fx-auth"): ConnectOptions => ({
  servers: broker.url,
  service,
  environment: "test",
  version: "1.0.0",
  schemaBaseUrl: "https://schemas.internal",
});

/** Un `connect()` que debe ser RECHAZADO, y pronto: no vale colgarse reintentando. */
async function rechazado(opts: ConnectOptions): Promise<ConnectionRejectedError> {
  const t0 = Date.now();
  let error: unknown;
  await connect(opts).then(
    (bus) => bus.close(),
    (e) => void (error = e),
  );
  assert.ok(error, "connect() no falló: aceptó una credencial que no debía");
  assert.ok(error instanceof ConnectionRejectedError, `lanzó ${(error as Error).constructor.name}: ${(error as Error).message}`);
  assert.ok(Date.now() - t0 < 3000, `tardó ${Date.now() - t0} ms: parece que reintenta en vez de fallar`);
  return error;
}

/** Publica y confirma que el evento llegó al stream. */
async function publicaYLlega(bus: FluxBus, broker: Broker, user: string, pass: string, dominio: string): Promise<void> {
  const ev = await bus.publish(`${dominio}.cosa.v1.hecha`, { n: 1 });
  assert.ok(ev.id);
  const nc = await conectarCrudo(broker, user, pass);
  try {
    const info = await (await jetstreamManager(nc)).streams.info(`EVT_${dominio.toUpperCase()}`);
    assert.ok(info.state.messages >= 1, "el evento no llegó al stream");
  } finally {
    await nc.close();
  }
}

// ─── nkey y usuario/contraseña ───────────────────────────────────────────────

describeBroker("SDK Node: autenticación por nkey y por usuario y contraseña", () => {
  let broker: Broker;
  const usuario = nkeys.createUser();
  const seed = new TextDecoder().decode(usuario.getSeed());
  const abiertos: FluxBus[] = [];

  before(async () => {
    broker = await startBroker(
      ({ port, store }) => `listen: 127.0.0.1:${port}
jetstream { store_dir: "${store}" }
accounts { PROD: { jetstream: enabled, users: [
  { nkey: "${usuario.getPublicKey()}" },
  { user: "fx-pass", password: "pw" }
] } }
`,
    );
  });
  after(async () => {
    for (const b of abiertos) await b.close().catch(() => {});
    await broker?.stop();
  });

  test("nkey: la semilla correcta conecta y publica", async () => {
    const bus = await connect({ ...base(broker), credentials: { nkeySeed: seed } });
    abiertos.push(bus);
    await publicaYLlega(bus, broker, "fx-pass", "pw", "nkey");
  });

  test("nkey: una semilla válida pero que el broker no conoce es RECHAZADA", async () => {
    const otra = new TextDecoder().decode(nkeys.createUser().getSeed());
    const r = await rechazado({ ...base(broker), credentials: { nkeySeed: otra } });
    assert.equal(r.reason, "auth");
    assert.equal(r.code, "CONNECTION_REJECTED");
  });

  test("usuario y contraseña: la correcta conecta y publica", async () => {
    const bus = await connect({ ...base(broker), credentials: { user: "fx-pass", pass: "pw" } });
    abiertos.push(bus);
    await publicaYLlega(bus, broker, "fx-pass", "pw", "pass");
  });

  test("usuario y contraseña: una contraseña incorrecta es RECHAZADA", async () => {
    const r = await rechazado({ ...base(broker), credentials: { user: "fx-pass", pass: "mal" } });
    assert.equal(r.reason, "auth");
    assert.match(r.message, /credentials/);
  });

  test("sin credenciales contra un broker que las exige es RECHAZADO", async () => {
    await rechazado(base(broker));
  });

  test("el mensaje del rechazo no contiene la credencial", async () => {
    const r = await rechazado({ ...base(broker), credentials: { user: "fx-pass", pass: "S3cr3t0-incorrecto" } });
    assert.doesNotMatch(r.message, /S3cr3t0-incorrecto/);
    assert.doesNotMatch(String((r.cause as Error | undefined)?.message), /S3cr3t0-incorrecto/);
  });
});

// ─── token ───────────────────────────────────────────────────────────────────

describeBroker("SDK Node: autenticación por token", () => {
  let broker: Broker;
  before(async () => {
    broker = await startBroker(
      ({ port, store }) => `listen: 127.0.0.1:${port}
jetstream { store_dir: "${store}" }
authorization { token: "t0k3n-de-prueba" }
`,
    );
  });
  after(async () => void (await broker?.stop()));

  test("el token correcto conecta y publica", async () => {
    const bus = await connect({ ...base(broker), credentials: { token: "t0k3n-de-prueba" } });
    try {
      assert.ok((await bus.publish("tok.cosa.v1.hecha", { n: 1 })).id);
    } finally {
      await bus.close();
    }
  });

  test("un token incorrecto es RECHAZADO", async () => {
    await rechazado({ ...base(broker), credentials: { token: "no-es-este" } });
  });
});

// ─── .creds en un broker en modo operador/JWT ────────────────────────────────

describeBroker("SDK Node: autenticación por .creds contra un broker en modo operador/JWT", () => {
  let broker: Broker;
  const env = crearEntornoJwt();
  const abiertos: FluxBus[] = [];

  before(async () => {
    broker = await startBroker(
      ({ port, store }) => `listen: 127.0.0.1:${port}
jetstream { store_dir: "${store}" }
${env.conf}`,
    );
  });
  after(async () => {
    for (const b of abiertos) await b.close().catch(() => {});
    await broker?.stop();
  });

  test("sin credenciales, el broker en modo operador RECHAZA (el control: el modo operador está activo)", async () => {
    await rechazado(base(broker));
  });

  test(".creds por contenido: conecta y publica", async () => {
    const bus = await connect({ ...base(broker), credentials: { credsContent: env.creds } });
    abiertos.push(bus);
    assert.ok((await bus.publish("jwtc.cosa.v1.hecha", { n: 1 })).id);
  });

  test(".creds por RUTA: conecta y publica — antes se ignoraba en silencio (H4)", async () => {
    const ruta = env.escribirCreds(broker.dir, "svc.creds", env.creds);
    const bus = await connect({ ...base(broker), credentials: { creds: ruta } });
    abiertos.push(bus);
    assert.ok((await bus.publish("jwtr.cosa.v1.hecha", { n: 1 })).id);
  });

  test("un .creds cuyo JWT no corresponde a la semilla es RECHAZADO", async () => {
    await rechazado({ ...base(broker), credentials: { credsContent: env.credsConSemillaAjena } });
  });

  test("un .creds mal formado falla en connect(), antes de tocar el broker", async () => {
    const ruta = env.escribirCreds(broker.dir, "roto.creds", "no es un creds");
    await assert.rejects(connect({ ...base(broker), credentials: { creds: ruta } }), /credenciales inválidas/);
    assert.equal(readFileSync(ruta, "utf8"), "no es un creds");
  });
});

// ─── el broker aún no está: se espera, y solo eso ────────────────────────────

describeBroker("SDK Node: la primera conexión espera a un broker que aún arranca", () => {
  const conf = ({ port, store }: { port: number; store: string }) => `listen: 127.0.0.1:${port}
jetstream { store_dir: "${store}" }
`;

  test("connect() no falla ni se rinde si NATS tarda: conecta en cuanto aparece", { timeout: 30_000 }, async () => {
    // Se reserva un puerto, se libera, y se llama a connect() contra él: «conexión rechazada».
    const primero = await startBroker(conf);
    const puerto = primero.port;
    await primero.stop();

    const pendiente = connect(base(primero)); // `primero.url` apunta al puerto que acaba de cerrarse
    // Un servicio que arranca antes que NATS es lo normal en un compose: no debe caerse.
    const estado = await Promise.race([pendiente.then(() => "conectó", () => "falló"), dormir(2500).then(() => "esperando")]);
    assert.equal(estado, "esperando", "connect() se rindió ante un broker caído en vez de esperar");

    const segundo = await startBroker(conf, { port: puerto });
    try {
      const bus = await Promise.race([
        pendiente,
        dormir(10_000).then(() => {
          throw new Error("connect() no conectó tras arrancar el broker");
        }),
      ]);
      try {
        assert.ok((await bus.publish("espera.cosa.v1.hecha", { n: 1 })).id);
      } finally {
        await bus.close();
      }
    } finally {
      await segundo.stop();
    }
  });
});
