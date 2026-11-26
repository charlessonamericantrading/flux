import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { errors, nkeys, TE } from "@nats-io/transport-node";

import * as publica from "../src/index.js";
import { connect } from "../src/client.js";
import { classify } from "../src/classify.js";
import { ErrorClass, PermanentError, isClassifiedError } from "../src/errors.js";
import { InvalidServiceNameError } from "../src/protocol.js";
import {
  ConnectionRejectedError,
  InvalidCredentialsError,
  InvalidTlsOptionsError,
  NotProvisionedError,
  PermissionDeniedError,
  buildAuthenticator,
  buildTls,
  inboxPrefixFor,
  isNotFound,
  normalizeDeniedSubject,
  permanentConnectError,
  permissionViolation,
} from "../src/security.js";

// ─── Utilidades ──────────────────────────────────────────────────────────────

const seedDe = (kp: ReturnType<typeof nkeys.createUser>) => new TextDecoder().decode(kp.getSeed());

/** Un `.creds` con la forma real: el JWT es de mentira, la semilla es válida. */
function credsDe(seed: string): string {
  return [
    "-----BEGIN NATS USER JWT-----",
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJzdWIiOiJ4In0.firma-de-mentira",
    "------END NATS USER JWT------",
    "",
    "************************* IMPORTANT *************************",
    "    NKEY Seed printed below can be used to sign and prove identity.",
    "",
    "-----BEGIN USER NKEY SEED-----",
    seed,
    "------END USER NKEY SEED------",
    "",
    "*************************************************************",
    "",
  ].join("\n");
}

let dir: string;
before(() => {
  dir = mkdtempSync(join(tmpdir(), "flux-sec-"));
});
after(() => rmSync(dir, { recursive: true, force: true }));

function fichero(nombre: string, contenido: string): string {
  const ruta = join(dir, nombre);
  writeFileSync(ruta, contenido);
  return ruta;
}

/** Lo que verá el servidor: la respuesta del autenticador al desafío. */
const respuesta = (a: ReturnType<typeof buildAuthenticator>) =>
  a!("nonce-del-servidor") as unknown as Record<string, string>;

// ─── Inbox privado ───────────────────────────────────────────────────────────

describe("inbox privado — _INBOX.<servicio>", () => {
  test("se deriva del nombre del servicio", () => {
    assert.equal(inboxPrefixFor("fx-fact"), "_INBOX.fx-fact");
    assert.equal(inboxPrefixFor("facturacion-api"), "_INBOX.facturacion-api");
  });

  // El servicio pasa a ser UN token de subject: un punto lo partiría en dos y un
  // comodín dejaría el inbox abierto. Lo que no encaja con [a-z0-9-] no se acepta.
  for (const malo of ["Fx", "a.b", "a b", "a>", "a*", "", "-a", "a-", "a--b", "a_b"]) {
    test(`rechaza el servicio ${JSON.stringify(malo)}`, () => {
      assert.throws(() => inboxPrefixFor(malo), InvalidServiceNameError);
    });
  }
});

// ─── Credenciales ────────────────────────────────────────────────────────────

describe("credenciales → autenticador", () => {
  test("sin credenciales no hay autenticador (desarrollo contra un NATS abierto)", () => {
    assert.equal(buildAuthenticator(undefined), undefined);
    assert.equal(buildAuthenticator({}), undefined);
  });

  test("usuario y contraseña", () => {
    const r = respuesta(buildAuthenticator({ user: "u", pass: "p" }));
    assert.deepEqual(r, { user: "u", pass: "p" });
  });

  test("token", () => {
    assert.deepEqual(respuesta(buildAuthenticator({ token: "t0k3n" })), { auth_token: "t0k3n" });
  });

  test("semilla nkey: anuncia SU clave pública y firma el desafío", () => {
    const kp = nkeys.createUser();
    const r = respuesta(buildAuthenticator({ nkeySeed: seedDe(kp) }));
    assert.equal(r.nkey, kp.getPublicKey());
    assert.ok(r.sig && r.sig.length > 20, "la firma del desafío no puede ir vacía");
  });

  test(".creds por contenido: lleva el JWT y la clave pública de la semilla", () => {
    const kp = nkeys.createUser();
    const r = respuesta(buildAuthenticator({ credsContent: credsDe(seedDe(kp)) }));
    assert.equal(r.jwt, "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJzdWIiOiJ4In0.firma-de-mentira");
    assert.equal(r.nkey, kp.getPublicKey());
    assert.ok(r.sig && r.sig.length > 20);
  });

  test(".creds por ruta: igual que por contenido", () => {
    const kp = nkeys.createUser();
    const ruta = fichero("a.creds", credsDe(seedDe(kp)));
    const r = respuesta(buildAuthenticator({ creds: ruta }));
    assert.equal(r.nkey, kp.getPublicKey());
    assert.ok(r.jwt);
  });

  test(".creds por ruta se RELEE en cada conexión: rotar la credencial no exige reiniciar", () => {
    const a = nkeys.createUser();
    const b = nkeys.createUser();
    const ruta = fichero("rota.creds", credsDe(seedDe(a)));
    const auth = buildAuthenticator({ creds: ruta });
    assert.equal(respuesta(auth).nkey, a.getPublicKey());
    writeFileSync(ruta, credsDe(seedDe(b)));
    assert.equal(respuesta(auth).nkey, b.getPublicKey());
  });

  // ── Lo incorrecto falla EN ALTO (hallazgo H4: antes se ignoraba en silencio) ──

  test("una clave que no existe lanza, no se ignora (`password`, `credsFile`…)", () => {
    for (const mal of [{ user: "u", password: "p" }, { credsFile: "/x" }, { nkey: "SU…" }]) {
      assert.throws(
        () => buildAuthenticator(mal as never),
        (e: Error) => e instanceof InvalidCredentialsError && /claves desconocidas/.test(e.message),
      );
    }
  });

  test("dos métodos a la vez es ambiguo y lanza", () => {
    const kp = nkeys.createUser();
    assert.throws(
      () => buildAuthenticator({ token: "t", user: "u", pass: "p" }),
      (e: Error) => e instanceof InvalidCredentialsError && /UN método/.test(e.message),
    );
    assert.throws(
      () => buildAuthenticator({ creds: "/x", nkeySeed: seedDe(kp) }),
      InvalidCredentialsError,
    );
  });

  test("`user` y `pass` van juntos", () => {
    assert.throws(() => buildAuthenticator({ user: "u" }), InvalidCredentialsError);
    assert.throws(() => buildAuthenticator({ pass: "p" }), InvalidCredentialsError);
  });

  test("un valor vacío no es «sin credencial»: lanza", () => {
    for (const mal of [{ token: "" }, { token: "  " }, { user: "", pass: "p" }, { nkeySeed: "" }]) {
      assert.throws(() => buildAuthenticator(mal), InvalidCredentialsError);
    }
  });

  test("un valor del tipo equivocado lanza", () => {
    assert.throws(() => buildAuthenticator({ token: 42 } as never), InvalidCredentialsError);
    assert.throws(() => buildAuthenticator("tok" as never), InvalidCredentialsError);
    assert.throws(() => buildAuthenticator(null as never), InvalidCredentialsError);
  });

  test("una ruta .creds que no existe, o que es un directorio, lanza", () => {
    assert.throws(
      () => buildAuthenticator({ creds: join(dir, "no-existe.creds") }),
      (e: Error) => e instanceof InvalidCredentialsError && /no se puede leer/.test(e.message),
    );
    const sub = join(dir, "es-un-directorio");
    mkdirSync(sub);
    assert.throws(() => buildAuthenticator({ creds: sub }), InvalidCredentialsError);
  });

  test("un .creds mal formado lanza en connect(), no en el handshake", () => {
    assert.throws(() => buildAuthenticator({ credsContent: "esto no es un creds" }), InvalidCredentialsError);
    const ruta = fichero("roto.creds", "-----BEGIN NATS USER JWT-----\nsolo-jwt\n------END NATS USER JWT------\n");
    assert.throws(() => buildAuthenticator({ creds: ruta }), InvalidCredentialsError);
  });

  test("una semilla malformada lanza, y una de cuenta («SA…») también: no autentica usuarios", () => {
    assert.throws(() => buildAuthenticator({ nkeySeed: "SUAXXXX" }), InvalidCredentialsError);
    assert.throws(() => buildAuthenticator({ nkeySeed: "no-es-una-semilla" }), InvalidCredentialsError);
    const cuenta = new TextDecoder().decode(nkeys.createAccount().getSeed());
    assert.match(cuenta, /^SA/);
    assert.throws(
      () => buildAuthenticator({ nkeySeed: cuenta }),
      (e: Error) => e instanceof InvalidCredentialsError && /USUARIO/.test(e.message),
    );
  });

  test("los mensajes de error NUNCA incluyen el valor de la credencial", () => {
    const secreto = "S3cr3t0-que-no-debe-salir";
    const intentos: Array<() => unknown> = [
      () => buildAuthenticator({ token: secreto, user: "u", pass: "p" }),
      () => buildAuthenticator({ nkeySeed: secreto }),
      () => buildAuthenticator({ credsContent: secreto }),
      () => buildAuthenticator({ user: "u", pass: secreto, token: "t" }),
      () => buildAuthenticator({ pass: secreto }),
    ];
    for (const f of intentos) {
      assert.throws(f, (e: Error) => {
        assert.doesNotMatch(String(e.message), new RegExp(secreto));
        assert.doesNotMatch(String((e.cause as Error | undefined)?.message ?? ""), new RegExp(secreto));
        return e instanceof InvalidCredentialsError;
      });
    }
  });
});

// ─── TLS ─────────────────────────────────────────────────────────────────────

describe("TLS → opciones del cliente", () => {
  test("sin `tls` no se pide TLS; `tls: {}` lo exige con las CA del sistema", () => {
    assert.equal(buildTls(undefined), undefined);
    assert.deepEqual(buildTls({}), {});
  });

  test("traduce los tres ficheros tal cual", () => {
    const ca = fichero("ca.pem", "ca");
    const cert = fichero("c.pem", "cert");
    const key = fichero("c.key", "key");
    assert.deepEqual(buildTls({ caFile: ca }), { caFile: ca });
    assert.deepEqual(buildTls({ caFile: ca, certFile: cert, keyFile: key }), {
      caFile: ca,
      certFile: cert,
      keyFile: key,
    });
  });

  test("un fichero que no existe lanza en connect(), no a los 20 s del handshake", () => {
    assert.throws(
      () => buildTls({ caFile: join(dir, "no-existe.pem") }),
      (e: Error) => e instanceof InvalidTlsOptionsError && /no se puede leer/.test(e.message),
    );
  });

  test("`certFile` y `keyFile` van juntos", () => {
    const cert = fichero("c2.pem", "cert");
    const key = fichero("c2.key", "key");
    assert.throws(() => buildTls({ certFile: cert }), /van juntos/);
    assert.throws(() => buildTls({ keyFile: key }), InvalidTlsOptionsError);
  });

  test("`null` NO se admite: en el cliente NATS significa «no uses TLS»", () => {
    assert.throws(() => buildTls(null as never), InvalidTlsOptionsError);
    assert.throws(() => buildTls(true as never), InvalidTlsOptionsError);
  });

  test("claves desconocidas, rutas vacías o de otro tipo lanzan", () => {
    assert.throws(() => buildTls({ ca: "x" } as never), /claves desconocidas/);
    assert.throws(() => buildTls({ caFile: "" }), InvalidTlsOptionsError);
    assert.throws(() => buildTls({ caFile: 7 } as never), InvalidTlsOptionsError);
  });
});

// ─── connect() valida ANTES de dialar ────────────────────────────────────────

describe("connect() falla en alto, sin tocar la red", () => {
  // Si validara después de dialar, estas llamadas se quedarían reintentando contra un
  // puerto cerrado y el test moriría por timeout: eso es lo que prueba que es ANTES.
  const base = {
    servers: "nats://127.0.0.1:1",
    environment: "test",
    version: "1.0.0",
    schemaBaseUrl: "https://schemas.internal",
  };

  test("servicio inválido", async () => {
    await assert.rejects(connect({ ...base, service: "Mal.Nombre" }), InvalidServiceNameError);
  });

  test("credenciales inválidas", async () => {
    await assert.rejects(
      connect({ ...base, service: "fx-fact", credentials: { user: "u", password: "p" } as never }),
      InvalidCredentialsError,
    );
  });

  test("TLS inválido", async () => {
    await assert.rejects(
      connect({ ...base, service: "fx-fact", tls: { caFile: join(dir, "no-existe.pem") } }),
      InvalidTlsOptionsError,
    );
  });
});

// ─── Errores: NOT_PROVISIONED y PERMISSION_DENIED ────────────────────────────

describe("NotProvisionedError", () => {
  const e = new NotProvisionedError({ kind: "stream", name: "EVT_PEDIDOS" }, "fx-fact", "$JS.API.STREAM.CREATE.EVT_PEDIDOS");

  test("código NOT_PROVISIONED, y nombra el recurso y cómo se aprovisiona", () => {
    assert.equal(e.code, "NOT_PROVISIONED");
    assert.match(e.message, /EVT_PEDIDOS/);
    assert.match(e.message, /flux provision/);
    assert.match(e.message, /\$JS\.API\.STREAM\.CREATE\.EVT_PEDIDOS/);
  });

  test("es PERMANENT: un handler que lo provoque va a la DLQ y no atasca la cola", () => {
    assert.ok(e instanceof PermanentError);
    assert.ok(isClassifiedError(e));
    const c = classify(e);
    assert.equal(c.class, ErrorClass.PERMANENT);
    assert.equal(c.code, "NOT_PROVISIONED");
    assert.equal(c.maxAttempts, undefined);
  });

  test("para un consumidor nombra también su stream", () => {
    const c = new NotProvisionedError(
      { kind: "consumer", name: "fx-fact__pedidos_pedido_v1_creado", stream: "EVT_PEDIDOS" },
      "fx-fact",
      "$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.x.y",
    );
    assert.match(c.message, /consumidor "fx-fact__pedidos_pedido_v1_creado"/);
    assert.match(c.message, /EVT_PEDIDOS/);
  });
});

describe("PermissionDeniedError", () => {
  const e = new PermissionDeniedError("fx-fact", "publish", "$JS.API.INFO");

  test("es PERMANENT con código PERMISSION_DENIED", () => {
    assert.equal(e.code, "PERMISSION_DENIED");
    const c = classify(e);
    assert.equal(c.class, ErrorClass.PERMANENT);
    assert.equal(c.code, "PERMISSION_DENIED");
  });

  test("para JetStream apunta a la ACL anterior a la v1.1; para un dominio, no", () => {
    assert.match(e.message, /ACL generada antes de la v1\.1/);
    assert.doesNotMatch(
      new PermissionDeniedError("fx-fact", "publish", "pedidos.pedido.v1.creado").message,
      /v1\.1/,
    );
  });
});

// ─── Reconocer lo que devuelve el cliente NATS ───────────────────────────────

describe("reconocer denegaciones y «no existe»", () => {
  const violacion = errors.PermissionViolationError.parse(
    'Permissions Violation for Publish to "$JS.API.STREAM.INFO.EVT_X"',
  )!;

  test("una petición denegada llega como RequestError con la violación en `cause`", () => {
    const e = new errors.RequestError("x", { cause: violacion });
    assert.deepEqual(permissionViolation(e), { operation: "publish", subject: "$JS.API.STREAM.INFO.EVT_X" });
    assert.deepEqual(permissionViolation(violacion), {
      operation: "publish",
      subject: "$JS.API.STREAM.INFO.EVT_X",
    });
  });

  test("distingue publicar de suscribirse", () => {
    const sub = errors.PermissionViolationError.parse('Permissions Violation for Subscription to "_INBOX.>"')!;
    assert.deepEqual(permissionViolation(sub), { operation: "subscribe", subject: "_INBOX.>" });
  });

  test("un error cualquiera NO es una denegación (ni un timeout, ni un «no existe»)", () => {
    for (const e of [new Error("boom"), new errors.TimeoutError(), { code: 10059 }, null, undefined, "x"]) {
      assert.equal(permissionViolation(e), undefined);
    }
  });

  test("«no existe» se reconoce por código y no se confunde entre stream y consumidor", () => {
    const stream = Object.assign(new Error("stream not found"), { code: 10059 });
    const consumer = Object.assign(new Error("consumer not found"), { code: 10014 });
    assert.ok(isNotFound(stream, "stream"));
    assert.ok(!isNotFound(stream, "consumer"));
    assert.ok(isNotFound(consumer, "consumer"));
    assert.ok(!isNotFound(consumer, "stream"));
    assert.ok(!isNotFound(violacion, "stream"));
    assert.ok(!isNotFound(new Error("x"), "consumer"));
  });

  test("el subject de un ack se normaliza: lleva secuencia y hora y nunca se repetiría", () => {
    assert.equal(
      normalizeDeniedSubject("$JS.ACK.EVT_PEDIDOS.fx-fact__p.1.5.5.1795687468273379351.0"),
      "$JS.ACK.EVT_PEDIDOS.fx-fact__p.>",
    );
    assert.equal(normalizeDeniedSubject("$JS.API.INFO"), "$JS.API.INFO");
  });
});

describe("fallos de la primera conexión: ¿se espera o se aborta?", () => {
  test("credenciales rechazadas: se aborta, esperar no las arregla", () => {
    const r = permanentConnectError(new errors.AuthorizationError("Authorization Violation"));
    assert.ok(r instanceof ConnectionRejectedError);
    assert.equal(r.reason, "auth");
    assert.equal(r.code, "CONNECTION_REJECTED");
  });

  test("certificado que no verifica: se aborta y se apunta a `tls.caFile`", () => {
    for (const code of ["UNABLE_TO_VERIFY_LEAF_SIGNATURE", "DEPTH_ZERO_SELF_SIGNED_CERT", "CERT_HAS_EXPIRED", "ERR_TLS_CERT_ALTNAME_INVALID"]) {
      const r = permanentConnectError(Object.assign(new Error("x"), { code }));
      assert.ok(r instanceof ConnectionRejectedError, code);
      assert.equal(r.reason, "tls");
      assert.match(r.message, /tls\.caFile/);
      assert.match(r.message, new RegExp(code));
    }
  });

  test("se pidió TLS y el servidor no lo ofrece: se aborta, y no se baja a texto plano", () => {
    const r = permanentConnectError(new errors.ConnectionError("server does not support 'tls'"));
    assert.ok(r instanceof ConnectionRejectedError);
    assert.equal(r.reason, "tls");
    assert.match(r.message, /no ofrece TLS/);
  });

  // El lado contrario importa tanto como el otro: abortar ante un servidor que aún
  // arranca tumbaría todo el despliegue por un NATS que tarda cinco segundos.
  test("servidor caído, DNS caído, timeout o reset: se SIGUE esperando", () => {
    const refused = new errors.ConnectionError("connection refused", {
      cause: Object.assign(new Error("connect ECONNREFUSED"), { code: "ECONNREFUSED" }),
    });
    for (const e of [
      refused,
      Object.assign(new Error("x"), { code: "ECONNRESET" }),
      Object.assign(new Error("x"), { code: "EAI_AGAIN" }),
      Object.assign(new Error("x"), { code: "ETIMEDOUT" }),
      new errors.TimeoutError(),
      new Error("algo raro"),
    ]) {
      assert.equal(permanentConnectError(e), undefined);
    }
  });
});

// ─── API pública ─────────────────────────────────────────────────────────────

describe("API pública (index.ts)", () => {
  test("exporta los errores nuevos, con su código", () => {
    assert.equal(publica.NotProvisionedError, NotProvisionedError);
    assert.equal(publica.PermissionDeniedError, PermissionDeniedError);
    assert.equal(publica.ConnectionRejectedError, ConnectionRejectedError);
    assert.equal(publica.InvalidCredentialsError, InvalidCredentialsError);
    assert.equal(publica.InvalidTlsOptionsError, InvalidTlsOptionsError);
  });

  test("conserva lo que ya exportaba: añadir seguridad no rompe a quien ya usaba el SDK", () => {
    for (const nombre of [
      "connect", "FluxBus", "ConsumerConfigMismatchError", "TenantIsolationError",
      "RetryableError", "PermanentError", "PoisonError", "ErrorClass", "classify",
      "createClassifier", "durableName", "streamName", "InvalidServiceNameError",
    ]) {
      assert.ok(nombre in publica, `falta ${nombre}`);
    }
  });

  test("ningún tipo de NATS se filtra a la API pública", () => {
    // Regla del SDK (client.ts): sustituir el broker no debe tocar aplicaciones. Las opciones
    // nuevas son tipos PROPIOS (`Credentials`, `TlsOptions`), no los de la librería.
    const exportados = Object.keys(publica);
    assert.ok(!exportados.some((n) => /^(nkeys|credsAuthenticator|nkeyAuthenticator|Authenticator)$/.test(n)));
  });
});
