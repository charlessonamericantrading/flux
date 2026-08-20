/**
 * Firma de eventos con Ed25519.
 * Contrato normativo: specification/07-signing.md
 *
 * Usa `node:crypto` — sin dependencias. Ed25519 está en la biblioteca estándar desde
 * Node 12, así que la extensión no cuesta un paquete más.
 */

import {
  createPrivateKey,
  createPublicKey,
  generateKeyPairSync,
  sign,
  verify,
} from "node:crypto";
import type { KeyObject } from "node:crypto";

import { PoisonError } from "./errors.js";
import { serialize, stripDlqExtensions, type FluxEvent } from "./envelope.js";

export type VerificationMode = "off" | "warn" | "require";

export interface SigningOptions {
  /**
   * Clave privada Ed25519 en PEM (PKCS#8) para firmar al publicar. Omitir para solo
   * verificar. **NUNCA se versiona** — 06-security.md §2.
   */
  privateKeyPem?: string;
  /** Id de la clave, formato `<servicio>-<n>`. Obligatorio si se firma. */
  keyId?: string;
  /**
   * Claves públicas conocidas: `signkeyid` → PEM (SPKI).
   *
   * **Incluye aquí las claves retiradas mientras exista algún evento firmado con
   * ellas** (mínimo 90 días, la retención de la DLQ). Retirar una clave impide emitir
   * con ella, no verificar lo ya emitido — 07-signing.md §6.
   */
  publicKeys?: Record<string, string>;
  /** @default "off" */
  verify?: VerificationMode;
}

export class SigningKeyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SigningKeyError";
  }
}

/**
 * Bytes canónicos que se firman: el evento sin `signature`, y sin las extensiones
 * `dlq*` (se añaden después de firmar). Ver 07-signing.md §5.
 *
 * No hay una canonicalización aparte para firmar: es el mismo `serialize()` que usa el
 * productor, y funciona porque 01-envelope.md §1.1, §2.2 y §6 fijan una única
 * representación en bytes.
 */
function signablePayload(event: FluxEvent): Uint8Array {
  const sinDlq = stripDlqExtensions(event);
  const { signature: _omitida, ...resto } = sinDlq as FluxEvent & { signature?: string };
  return serialize(resto as FluxEvent);
}

export interface Signer {
  /** Devuelve el evento con `signkeyid` y `signature`, ambos antes de `data`. */
  sign(event: FluxEvent): FluxEvent;
}

export interface Verifier {
  /** Lanza PoisonError según el modo configurado. Ver 07-signing.md §7. */
  check(event: FluxEvent): void;
}

export function createSigner(options: SigningOptions): Signer | null {
  if (!options.privateKeyPem) return null;
  if (!options.keyId) {
    throw new SigningKeyError(
      "signing.privateKeyPem requiere signing.keyId: sin él, un verificador no sabe " +
        "qué clave pública usar (07-signing.md §4)",
    );
  }

  let key: KeyObject;
  try {
    key = createPrivateKey(options.privateKeyPem);
  } catch (cause) {
    throw new SigningKeyError(
      `clave privada inválida: ${cause instanceof Error ? cause.message : String(cause)}`,
    );
  }
  if (key.asymmetricKeyType !== "ed25519") {
    throw new SigningKeyError(
      `la clave es ${key.asymmetricKeyType}, se esperaba ed25519. El protocolo no ` +
        `negocia algoritmo a propósito: los formatos con algoritmo negociable acumulan ` +
        `una familia de vulnerabilidades que solo existe porque hay algo que negociar ` +
        `(07-signing.md §3)`,
    );
  }

  const keyId = options.keyId;

  return {
    sign(event) {
      // `signkeyid` va DENTRO de lo firmado: si quedara fuera, un atacante podría
      // cambiarlo para que la verificación buscara otra clave — 07-signing.md §5.
      const { data, ...resto } = event;
      const conKeyId = { ...resto, signkeyid: keyId, data } as FluxEvent;
      const firma = sign(null, signablePayload(conKeyId), key);

      // `data` sigue siendo el último atributo — 01-envelope.md §6.
      const { data: d, ...r } = conKeyId;
      return {
        ...r,
        signature: firma.toString("base64url"),
        data: d,
      } as FluxEvent;
    },
  };
}

export function createVerifier(options: SigningOptions): Verifier | null {
  const mode = options.verify ?? "off";
  if (mode === "off") return null;

  const keys = new Map<string, KeyObject>();
  for (const [id, pem] of Object.entries(options.publicKeys ?? {})) {
    try {
      const k = createPublicKey(pem);
      if (k.asymmetricKeyType !== "ed25519") {
        throw new SigningKeyError(`la clave pública "${id}" es ${k.asymmetricKeyType}, se esperaba ed25519`);
      }
      keys.set(id, k);
    } catch (cause) {
      if (cause instanceof SigningKeyError) throw cause;
      throw new SigningKeyError(
        `clave pública "${id}" inválida: ${cause instanceof Error ? cause.message : String(cause)}`,
      );
    }
  }

  if (keys.size === 0) {
    throw new SigningKeyError(
      `signing.verify = "${mode}" requiere signing.publicKeys. Incluye también las ` +
        `claves RETIRADAS mientras existan eventos firmados con ellas (07-signing.md §6)`,
    );
  }

  const fallar = (code: string, message: string) => {
    const err = new PoisonError(message, { code });
    if (mode === "require") throw err;
    console.warn(`[flux] ${message}`);
  };

  return {
    check(event) {
      const firma = (event as FluxEvent & { signature?: string }).signature;
      if (firma === undefined) {
        fallar("MISSING_SIGNATURE", `evento ${event.id} sin firma`);
        return;
      }
      const keyId = (event as FluxEvent & { signkeyid?: string }).signkeyid;
      const key = keyId !== undefined ? keys.get(keyId) : undefined;
      if (!key) {
        // Una clave desconocida NO es lo mismo que una retirada: si el id no está en
        // el mapa, el operador olvidó conservarla o el evento viene de fuera.
        fallar(
          "UNKNOWN_SIGNING_KEY",
          `evento ${event.id} firmado con signkeyid=${JSON.stringify(keyId)}, que no ` +
            `está entre las claves conocidas. ¿Se retiró sin conservar la pública?`,
        );
        return;
      }
      let ok = false;
      try {
        ok = verify(null, signablePayload(event), key, Buffer.from(firma, "base64url"));
      } catch {
        ok = false;
      }
      if (!ok) {
        fallar("INVALID_SIGNATURE", `la firma del evento ${event.id} no verifica`);
      }
    },
  };
}

/** Genera un par Ed25519 en PEM. Comodidad para pruebas y para `flux keygen`. */
export function generateKeyPair(): { privateKeyPem: string; publicKeyPem: string } {
  const { privateKey, publicKey } = generateKeyPairSync("ed25519");
  return {
    privateKeyPem: privateKey.export({ type: "pkcs8", format: "pem" }).toString(),
    publicKeyPem: publicKey.export({ type: "spki", format: "pem" }).toString(),
  };
}
