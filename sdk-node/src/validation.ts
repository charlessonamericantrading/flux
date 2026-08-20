/**
 * Validación L3 contra el JSON Schema del evento.
 * Contrato normativo: specification/00-protocol.md §5 (nivel L3)
 *
 * Cierra el hueco más grande que quedaba: sin esto, un productor puede publicar un
 * payload que viola su propio `dataschema` y nadie se entera hasta que un consumidor
 * —posiblemente en otro equipo, otro lenguaje y otra semana— se atraganta. El error
 * aparece lejísimos de su causa.
 *
 * Validar en `publish()` lo convierte en un fallo del servicio que lo provocó.
 */

import { PermanentError } from "./errors.js";
import type { FluxEvent } from "./envelope.js";

export type ValidationMode = "off" | "warn" | "strict";

export interface SchemaBundle {
  /** subject → URI del esquema con el MINOR más alto de su mayor. */
  subjects: Record<string, string>;
  /** URI → JSON Schema. */
  schemas: Record<string, object>;
}

export interface ValidationOptions {
  /**
   * `"strict"` — `publish()` lanza si el payload no valida. Es lo que hace que un
   * contrato roto sea un fallo del productor y no un misterio del consumidor.
   *
   * `"warn"` — registra y publica igual. Útil para introducir validación en un
   * ecosistema en marcha sin romper nada el primer día.
   *
   * `"off"` — nivel L2. Sin coste.
   *
   * @default "off"
   */
  mode?: ValidationMode;
  /** Bundle generado por `scripts/bundle-schemas.mjs`. */
  bundle?: SchemaBundle;
  /**
   * Validar también al CONSUMIR. Un fallo se clasifica PERMANENT: el evento es
   * sintácticamente correcto pero incumple su contrato, y reintentarlo dará
   * exactamente el mismo resultado.
   *
   * @default false
   */
  onConsume?: boolean;
}

/**
 * Extiende `PermanentError` a propósito: un evento que incumple su contrato es
 * sintácticamente correcto pero **nunca va a validar por mucho que se reintente**, así
 * que la clase correcta es PERMANENT (04-errors.md §1.2).
 *
 * Sin esa herencia el error caía en la política de lo DESCONOCIDO —`retryable-bounded`,
 * 2 entregas— y llegaba a la DLQ como `retryable`: gastaba un reintento inútil y, peor,
 * la DLQ lo agrupaba por una causa equivocada. Lo destapó el port a Rust al comparar su
 * clasificación con la de esta referencia.
 */
export class SchemaValidationError extends PermanentError {
  constructor(
    readonly subject: string,
    readonly dataschema: string,
    readonly errors: string[],
  ) {
    super(
      `el payload de "${subject}" no cumple su esquema (${dataschema}):\n` +
        errors.map((e) => `  · ${e}`).join("\n"),
      { code: "INVALID_SCHEMA" },
    );
    this.name = "SchemaValidationError";
  }
}

/** También PERMANENT: si el esquema no está en el bundle, reintentar no lo traerá. */
export class SchemaNotFoundError extends PermanentError {
  constructor(readonly subject: string, readonly dataschema: string) {
    super(
      `no hay esquema para "${subject}" (${dataschema}) en el bundle. ` +
        `Regenera con \`node scripts/bundle-schemas.mjs\`, o baja \`validation.mode\` a "warn".`,
      { code: "SCHEMA_NOT_FOUND" },
    );
    this.name = "SchemaNotFoundError";
  }
}

type ValidateFn = (data: unknown) => string[] | null;

/**
 * Compila los validadores del bundle.
 *
 * `ajv` se resuelve por importación dinámica y es una dependencia OPCIONAL: L3 es
 * opt-in, así que su coste también debe serlo. Un servicio en L2 no debería arrastrar
 * un validador de JSON Schema que no va a ejecutar.
 */
export async function createValidator(
  options: ValidationOptions,
): Promise<((event: FluxEvent, subject: string) => void) | null> {
  const mode = options.mode ?? "off";
  if (mode === "off") return null;

  if (!options.bundle) {
    throw new Error(
      `validation.mode = "${mode}" requiere validation.bundle. ` +
        `Genera el bundle con \`node scripts/bundle-schemas.mjs\` e impórtalo:\n` +
        `  import bundle from "./schemas/bundle.json" with { type: "json" };`,
    );
  }

  let Ajv: new (opts: object) => {
    compile: (s: object) => ((d: unknown) => boolean) & { errors?: Array<{ instancePath: string; message?: string }> | null };
  };
  try {
    // `ajv/dist/2020`, NO el export por defecto: los esquemas de flux declaran
    // `$schema: draft/2020-12` y el Ajv por defecto solo entiende draft-07. Compilar
    // uno con el otro no da un error de versión — da
    // `no schema with key or ref "…/draft/2020-12/schema"`, que no dice nada útil.
    const mod = await import("ajv/dist/2020.js" as string);
    Ajv = mod.default?.default ?? mod.default ?? mod.Ajv2020;
  } catch (cause) {
    throw new Error(
      `validation.mode = "${mode}" necesita el paquete "ajv" (build 2020-12), que es ` +
        `una dependencia opcional: L3 es opt-in, así que su coste también.\n` +
        `  npm install ajv\n` +
        `Causa: ${cause instanceof Error ? cause.message : String(cause)}`,
    );
  }

  // allErrors: un payload con tres campos mal debe reportar los tres. Reportar solo
  // el primero convierte arreglarlo en tres despliegues.
  const ajv = new Ajv({ allErrors: true, strict: false });
  const compiled = new Map<string, ValidateFn>();

  for (const [uri, schema] of Object.entries(options.bundle.schemas)) {
    const fn = ajv.compile(schema);
    compiled.set(uri, (data) => {
      if (fn(data)) return null;
      return (fn.errors ?? []).map(
        (e) => `${e.instancePath || "(raíz)"} ${e.message ?? "inválido"}`,
      );
    });
  }

  return (event, subject) => {
    const validate = compiled.get(event.dataschema);
    if (!validate) {
      if (mode === "strict") throw new SchemaNotFoundError(subject, event.dataschema);
      console.warn(`[flux] sin esquema para ${subject} (${event.dataschema})`);
      return;
    }
    const errors = validate(event.data);
    if (!errors) return;
    const err = new SchemaValidationError(subject, event.dataschema, errors);
    if (mode === "strict") throw err;
    console.warn(`[flux] ${err.message}`);
  };
}

/** Resuelve el `dataschema` de un subject desde el bundle. */
export function schemaUriFor(bundle: SchemaBundle, subject: string): string | undefined {
  return bundle.subjects[subject];
}
