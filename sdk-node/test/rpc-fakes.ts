/**
 * Una «red» EN MEMORIA para probar `bus.request` y `bus.respond` sin broker.
 *
 * Hace lo que se MIDIÓ contra nats-server 2.14.5, y solo eso:
 *   · `request` a un subject sin nadie → `RequestError` con causa `NoResponders` (A-56);
 *   · un grupo de cola entrega CADA petición a UNA de sus instancias, al azar (A-58);
 *   · una respuesta tardía o repetida a un inbox que ya no espera se IGNORA (A-61);
 *   · `sub.drain()` deja de escuchar y entrega lo que ya estaba en cola antes de terminar el iterador;
 *   · una suscripción denegada no lanza al crearse: su iterador lanza `PermissionViolationError` (A-8f).
 *
 * Lo que NO hace es lo que prueban los tests de integración contra un broker real: permisos, límites de payload del
 * servidor, reparto real, tiempos. Un fake que «hiciera lo que el test necesita» probaría el fake.
 */

import { errors } from "@nats-io/transport-node";

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export interface MensajeFalso {
  subject: string;
  reply: string;
  data: Uint8Array;
  /** Las cabeceras de NATS no se usan (11-request-reply.md §3.4): el fake no las modela. */
  headers: undefined;
}

/** Una suscripción falsa: un iterador asíncrono que entrega lo encolado y, con `drain`, termina al vaciarse. */
export class SuscripcionFalsa {
  readonly cola: MensajeFalso[] = [];
  #despierta: (() => void) | undefined;
  #terminada = false;
  #error: unknown;
  drenada = false;
  cerrada = false;

  constructor(
    readonly subject: string,
    readonly queue: string | undefined,
    private readonly alCerrar: (s: SuscripcionFalsa) => void,
  ) {}

  push(m: MensajeFalso): void {
    if (this.#terminada) return;
    this.cola.push(m);
    this.#despierta?.();
  }

  /** El broker deniega la suscripción: el iterador LANZA (A-8f). */
  falla(e: unknown): void {
    this.#error = e;
    this.cerrada = true;
    this.#despierta?.();
  }

  isClosed(): boolean {
    return this.cerrada || this.#terminada;
  }

  /** Deja de escuchar (UNSUB), y el iterador termina cuando entregue lo que ya había. */
  async drain(): Promise<void> {
    this.drenada = true;
    this.alCerrar(this);
    this.#terminada = true;
    this.#despierta?.();
  }

  unsubscribe(): void {
    this.cerrada = true;
    this.cola.length = 0;
    this.alCerrar(this);
    this.#terminada = true;
    this.#despierta?.();
  }

  [Symbol.asyncIterator](): AsyncIterator<MensajeFalso> {
    return {
      next: async () => {
        for (;;) {
          if (this.#error !== undefined) throw this.#error;
          const m = this.cola.shift();
          if (m) return { value: m, done: false };
          if (this.#terminada) return { value: undefined as never, done: true };
          await new Promise<void>((r) => (this.#despierta = r));
        }
      },
    };
  }
}

interface Interes {
  subject: string;
  queue: string | undefined;
  sub: SuscripcionFalsa | undefined;
  crudo: ((m: MensajeFalso) => void) | undefined;
}

export class RedEnMemoria {
  readonly intereses: Interes[] = [];
  readonly #esperas = new Map<string, (data: Uint8Array) => void>();
  #n = 0;

  conectar(): ConexionFalsa {
    return new ConexionFalsa(this);
  }

  /** Un suscriptor «en bruto»: lo que haría un `nats sub` o un productor roto. `fn` recibe el mensaje y puede contestar con `publish`. */
  escuchaCrudo(subject: string, fn: (m: MensajeFalso) => void): void {
    this.intereses.push({ subject, queue: undefined, sub: undefined, crudo: fn });
  }

  nuevoInbox(): string {
    return `_INBOX.falso.${++this.#n}`;
  }

  espera(inbox: string, resuelve: (d: Uint8Array) => void): void {
    this.#esperas.set(inbox, resuelve);
  }

  olvida(inbox: string): void {
    this.#esperas.delete(inbox);
  }

  /** Entrega a UNA instancia de cada grupo de cola, y a todos los suscriptores sin grupo. Devuelve si hubo interés. */
  enruta(subject: string, reply: string, data: Uint8Array): boolean {
    const casa = this.intereses.filter((i) => i.subject === subject && !(i.sub?.isClosed() && !i.crudo));
    if (casa.length === 0) return false;
    const msg: MensajeFalso = { subject, reply, data, headers: undefined };
    const porGrupo = new Map<string, Interes[]>();
    for (const i of casa) {
      if (i.queue === undefined) {
        i.sub?.push(msg);
        i.crudo?.(msg);
      } else {
        porGrupo.set(i.queue, [...(porGrupo.get(i.queue) ?? []), i]);
      }
    }
    for (const miembros of porGrupo.values()) {
      miembros[Math.floor(Math.random() * miembros.length)].sub?.push(msg);
    }
    return true;
  }

  /** Una publicación a un inbox: la primera resuelve la espera; las demás (y las tardías) se IGNORAN (A-61). */
  publica(subject: string, data: Uint8Array): void {
    const resuelve = this.#esperas.get(subject);
    if (!resuelve) return;
    this.#esperas.delete(subject);
    resuelve(data);
  }
}

export class ConexionFalsa {
  readonly enviadas: Array<{ subject: string; bytes: Uint8Array; timeout: number | undefined }> = [];
  readonly publicadas: Array<{ subject: string; bytes: Uint8Array; argumentos: number }> = [];
  readonly suscripciones: SuscripcionFalsa[] = [];
  readonly argumentosDeSubscribe: Array<{ subject: string; opts: unknown }> = [];
  /** Lo que ve el test sobre el orden de cierre. */
  readonly orden: string[] = [];
  info: { max_payload: number } | undefined = { max_payload: 1_048_576 };
  /** Si está, `request` falla con esto SIN tocar la red (una denegación, un cierre…). */
  falloDeLaPeticion: (() => Error) | undefined;
  /** Si es `true`, `request` ni contesta ni falla: se queda esperando hasta su timeout (una librería que no entrega la denegación). */
  mudo = false;
  /** Si está, `publish` falla con esto (una conexión cerrándose). */
  falloDelPublish: Error | undefined;
  #estados: Array<unknown> = [];
  #despiertaEstado: (() => void) | undefined;
  cerrada = false;

  constructor(private readonly red: RedEnMemoria) {}

  /** Inyecta un evento de estado de la conexión (por ejemplo, el `-ERR 'Permissions Violation …'`). */
  emiteEstado(ev: unknown): void {
    this.#estados.push(ev);
    this.#despiertaEstado?.();
  }

  status() {
    return {
      [Symbol.asyncIterator]: () => ({
        next: async () => {
          for (;;) {
            const e = this.#estados.shift();
            if (e !== undefined) return { value: e, done: false };
            await new Promise<void>((r) => (this.#despiertaEstado = r));
          }
        },
      }),
    };
  }

  isClosed(): boolean {
    return this.cerrada;
  }

  async flush(): Promise<void> {}

  /** Cuántas respuestas se habían publicado cuando se cerró la conexión: lo que un cierre prematuro PIERDE. */
  publicadasAlDrenar: number | undefined;

  async drain(): Promise<void> {
    this.orden.push("nc.drain");
    this.publicadasAlDrenar = this.publicadas.length;
    this.cerrada = true;
  }

  async request(subject: string, data: Uint8Array, opts?: { timeout?: number }): Promise<{ data: Uint8Array }> {
    this.enviadas.push({ subject, bytes: data, timeout: opts?.timeout });
    if (this.falloDeLaPeticion) throw this.falloDeLaPeticion();
    const inbox = this.red.nuevoInbox();
    const respuesta = new Promise<{ data: Uint8Array }>((res) => this.red.espera(inbox, (d) => res({ data: d })));
    if (!this.mudo && !this.red.enruta(subject, inbox, data)) {
      this.red.olvida(inbox);
      throw new errors.RequestError(`no responders: '${subject}'`, { cause: new errors.NoRespondersError(subject) });
    }
    const timeout = opts?.timeout ?? 1000;
    let temporizador: ReturnType<typeof setTimeout> | undefined;
    const vencido = new Promise<never>((_, rej) => {
      temporizador = setTimeout(() => {
        this.red.olvida(inbox); // la respuesta tardía llegará a un inbox que ya no espera: se ignora
        rej(new errors.TimeoutError());
      }, timeout);
    });
    try {
      return await Promise.race([respuesta, vencido]);
    } finally {
      clearTimeout(temporizador);
    }
  }

  subscribe(subject: string, opts?: { queue?: string }): SuscripcionFalsa {
    this.argumentosDeSubscribe.push({ subject, opts });
    const sub = new SuscripcionFalsa(subject, opts?.queue, (s) => {
      this.orden.push(s.drenada ? `sub.drain:${s.subject}` : `sub.unsubscribe:${s.subject}`);
    });
    this.suscripciones.push(sub);
    this.red.intereses.push({ subject, queue: opts?.queue, sub, crudo: undefined });
    return sub;
  }

  publish(subject: string, bytes: Uint8Array, ...resto: unknown[]): void {
    if (this.falloDelPublish) throw this.falloDelPublish;
    this.publicadas.push({ subject, bytes, argumentos: 2 + resto.length });
    if (subject.startsWith("_INBOX.")) this.red.publica(subject, bytes);
    else this.red.enruta(subject, "", bytes);
  }

  /** Un mensaje que llega a la suscripción de `subject`, sin pasar por `request`: `nats pub` a mano, o una petición mal formada. */
  entrega(subject: string, data: Uint8Array | string, reply = ""): void {
    const bytes = typeof data === "string" ? new TextEncoder().encode(data) : data;
    const msg: MensajeFalso = { subject, reply, data: bytes, headers: undefined };
    for (const s of this.suscripciones) if (s.subject === subject) s.push(msg);
  }
}

export { sleep };
