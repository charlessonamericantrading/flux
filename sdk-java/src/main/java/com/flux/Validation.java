/*
 * Validacion L3 contra el JSON Schema del evento.
 * Contrato normativo: specification/00-protocol.md §5 (nivel L3).
 *
 * Cierra el hueco mas grande que quedaba en L2: sin esto, un productor puede publicar un
 * payload que viola su propio `dataschema` y nadie se entera hasta que un consumidor
 * —posiblemente en otro equipo, otro lenguaje y otra semana— se atraganta. El error
 * aparece lejisimos de su causa.
 *
 * Validar en `publish()` lo convierte en un fallo del servicio que lo provoco.
 *
 * ⚠️ La libreria de validacion es una dependencia OPCIONAL del pom
 * (`com.networknt:json-schema-validator`, marcada <optional>true</optional>): L3 es opt-in,
 * asi que su coste tambien debe serlo. Un servicio en L2 no deberia arrastrar un validador
 * de JSON Schema que no va a ejecutar nunca. Por eso todo lo que la toca vive en
 * NetworkntValidator, una clase que solo se CARGA cuando el modo no es OFF: si el jar no
 * esta, el fallo es un mensaje accionable en connect() y no un NoClassDefFoundError a
 * media publicacion.
 */
package com.flux;

/**
 * Modo, bundle y fabrica de la validacion L3.
 *
 * <p>Clase de utilidad: no se instancia.
 *
 * <pre>{@code
 * SchemaBundle bundle = SchemaBundle.fromPath(Path.of("schemas/bundle.json"));
 *
 * FluxBus bus = FluxBus.connect(new FluxBus.ConnectOptions()
 *         // …
 *         .validation(new Validation.Options()
 *                 .mode(Validation.Mode.STRICT)
 *                 .bundle(bundle)
 *                 .onConsume(true)));
 * }</pre>
 */
public final class Validation {

    private Validation() {
        throw new AssertionError("clase de utilidad");
    }

    // ─── Modo ────────────────────────────────────────────────────────────────

    /** Que hacer cuando el payload no cumple su esquema — 00-protocol.md §5. */
    public enum Mode {
        /**
         * Default: nivel L2. No se compila nada y no se paga nada.
         */
        OFF,
        /**
         * Se registra y se publica igual.
         *
         * <p>Existe por la misma razon que el {@code warn} de la firma: introducir
         * validacion en un ecosistema en marcha exige un periodo en el que se ve el
         * incumplimiento sin romper a nadie el primer dia.
         */
        WARN,
        /**
         * {@code publish()} LANZA si el payload no valida. Es el nivel L3 de verdad: un
         * contrato roto pasa a ser un fallo del productor.
         */
        STRICT
    }

    // ─── Opciones ────────────────────────────────────────────────────────────

    /** Configuracion de la validacion L3. */
    public static final class Options {
        private Mode mode = Mode.OFF;
        private SchemaBundle bundle;
        private boolean onConsume;
        private System.Logger logger;

        /** Default {@link Mode#OFF}. */
        public Options mode(Mode v) {
            this.mode = v != null ? v : Mode.OFF;
            return this;
        }

        /**
         * El bundle generado por {@code scripts/bundle-schemas.mjs}.
         *
         * <p>Se pasa como DATO: el SDK no resuelve el {@code dataschema} por HTTP ni con
         * el modo estricto puesto — 00-protocol.md §5, "Resolucion de esquemas: bundle, no
         * HTTP".
         */
        public Options bundle(SchemaBundle v) {
            this.bundle = v;
            return this;
        }

        /**
         * Validar tambien al CONSUMIR. Default {@code false}.
         *
         * <p>Un fallo se clasifica PERMANENT: el evento es sintacticamente correcto pero
         * incumple su contrato, y reintentarlo dara exactamente el mismo resultado
         * — 04-errors.md §1.2.
         */
        public Options onConsume(boolean v) {
            this.onConsume = v;
            return this;
        }

        /**
         * Donde se registran los avisos de {@link Mode#WARN}.
         *
         * <p>{@code null} usa {@code System.getLogger("flux")}, no el silencio: un modo que
         * se llama {@code warn} y no avisa de nada es indistinguible de {@link Mode#OFF}, y
         * quien lo configura cree que esta viendo los incumplimientos.
         */
        public Options logger(System.Logger v) {
            this.logger = v;
            return this;
        }

        /** El modo configurado. */
        public Mode mode() {
            return mode;
        }

        /** El bundle configurado, o {@code null}. */
        public SchemaBundle bundle() {
            return bundle;
        }

        /** Si se valida tambien al consumir. */
        public boolean onConsume() {
            return onConsume;
        }

        System.Logger logger() {
            return logger;
        }
    }

    // ─── Validador ───────────────────────────────────────────────────────────

    /** Comprueba un evento contra el esquema que su {@code dataschema} declara. */
    @FunctionalInterface
    public interface Validator {
        /**
         * Valida el {@code data} del evento.
         *
         * @param event   el evento ya construido (al publicar) o ya parseado (al consumir).
         * @param subject subject del evento, solo para el mensaje de error.
         * @throws SchemaValidationException en {@link Mode#STRICT}, si el payload no cumple.
         * @throws SchemaNotFoundException   en {@link Mode#STRICT}, si el bundle no trae su
         *                                   esquema.
         */
        void check(FluxEvent event, String subject);
    }

    /**
     * Compila los validadores del bundle UNA vez.
     *
     * <p>Lo llama {@code FluxBus.connect()} y no la ruta caliente: compilar un JSON Schema
     * por evento tiraria el throughput y no aportaria nada, porque el bundle es inmutable
     * durante la vida del proceso.
     *
     * @return {@code null} en {@link Mode#OFF}. Un {@code null} aqui es la forma de que L2
     *         no pague absolutamente nada por L3.
     * @throws IllegalArgumentException si el modo no es OFF y falta el bundle.
     * @throws IllegalStateException    si falta la libreria de validacion en el classpath.
     */
    public static Validator create(Options options) {
        Options o = options != null ? options : new Options();
        if (o.mode() == Mode.OFF) {
            return null;
        }
        if (o.bundle() == null) {
            throw new IllegalArgumentException(
                    "flux: el modo de validacion " + o.mode() + " exige un bundle. Generalo con "
                            + "`node scripts/bundle-schemas.mjs` y pasalo con "
                            + "`new Validation.Options().bundle(SchemaBundle.fromPath(...))`");
        }

        System.Logger logger = o.logger() != null ? o.logger() : System.getLogger("flux");
        try {
            // La clase se carga AQUI, en el arranque, y solo si hace falta. Es el
            // equivalente del `await import("ajv/dist/2020.js")` del SDK de Node: la
            // dependencia es opcional, asi que su ausencia tiene que dar un mensaje que
            // diga que instalar — y no un NoClassDefFoundError en la primera publicacion.
            return new NetworkntValidator(o.mode(), o.bundle(), logger);
        } catch (NoClassDefFoundError | ExceptionInInitializerError e) {
            throw new IllegalStateException(
                    "flux: el modo de validacion " + o.mode() + " necesita "
                            + "com.networknt:json-schema-validator en el classpath. Es una "
                            + "dependencia OPCIONAL del SDK porque L3 es opt-in y su coste tambien:\n"
                            + "  <dependency>\n"
                            + "    <groupId>com.networknt</groupId>\n"
                            + "    <artifactId>json-schema-validator</artifactId>\n"
                            + "    <version>" + NetworkntValidator.LIBRARY_VERSION + "</version>\n"
                            + "  </dependency>\n"
                            + "Causa: " + e, e);
        }
    }
}
