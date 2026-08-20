/*
 * Firma de eventos con Ed25519.
 * Contrato normativo: specification/07-signing.md
 *
 * Extension OPCIONAL de v1: el default es `off` y un evento SIN firma sigue siendo valido.
 * Un SDK L1/L2/L3 conforme no necesita implementarla.
 *
 * Sin dependencias nuevas: `java.security` trae Ed25519 desde el JDK 15 (proveedor SunEC,
 * JEP 339) y el pom compila con release 17, asi que esta garantizado. NO se usa
 * BouncyCastle a proposito: una extension opcional del protocolo no debe anadir una
 * dependencia transitiva a todo servicio que use el SDK solo para publicar un evento sin
 * firmar. Es la ventaja que Java tiene sobre .NET aqui — ver sdk-dotnet/README.md.
 *
 * Lo que hace que esto funcione entre lenguajes no vive en este fichero: firmar exige que
 * el evento tenga UNA UNICA representacion en bytes, y eso lo fijan 01-envelope.md §1.1
 * (UTF-8 literal, sin escapes de la forma backslash-u), §2.2 (`time` con exactamente 3
 * decimales) y §6
 * (orden de claves, `data` al final). Las tres se escribieron para otros problemas
 * —replay verbatim, fixtures compartidos— y juntas resultaron ser exactamente la
 * canonicalizacion que la firma necesitaba. NO hay una forma canonica aparte para firmar:
 * es el mismo `Envelope.serialize()` que usa el productor (07-signing.md §2).
 */
package com.flux;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Firma y verificacion Ed25519 de eventos.
 *
 * <p>Clase de utilidad: no se instancia.
 *
 * <pre>{@code
 * Signing.KeyPairPem par = Signing.generateKeyPair();
 *
 * FluxBus bus = FluxBus.connect(new FluxBus.ConnectOptions()
 *         // …
 *         .signing(new Signing.SigningOptions()
 *                 .privateKeyPem(par.privateKeyPem())
 *                 .keyId("pedidos-api-1")
 *                 .publicKey("pedidos-api-1", par.publicKeyPem())
 *                 .verify(Signing.VerificationMode.REQUIRE)));
 * }</pre>
 */
public final class Signing {

    private Signing() {
        throw new AssertionError("clase de utilidad");
    }

    /**
     * El unico algoritmo. Sin negociacion, sin parametros, sin curvas que elegir.
     *
     * <p>La razon es de superficie de error, no de rendimiento: los formatos de firma con
     * algoritmo negociable acumulan una familia de vulnerabilidades bien conocida —desde
     * {@code alg: none} hasta la confusion entre HMAC y RSA— que solo existe porque hay
     * algo que negociar. Aqui no lo hay (07-signing.md §3).
     */
    public static final String ALGORITHM = "Ed25519";

    // ─── Politica de verificacion — 07-signing.md §7 ──────────────────────────

    /**
     * Que hacer ante un evento sin firma o con una firma invalida.
     *
     * <p>{@link #WARN} existe porque adoptar la firma en un ecosistema en marcha exige un
     * periodo en el que unos productores firman y otros no. Pasar directo a
     * {@link #REQUIRE} convierte en POISON todo evento de un servicio aun no migrado.
     */
    public enum VerificationMode {
        /** Default. No se mira la firma. No se construye verificador: no se paga lo que no se usa. */
        OFF,
        /** Se registra y se acepta. El modo de la migracion. */
        WARN,
        /** POISON. Solo cuando TODOS los productores del subject firman. */
        REQUIRE
    }

    /** Falta {@code signature} en modo {@code require} — 07-signing.md §7. */
    public static final String CODE_MISSING_SIGNATURE = "MISSING_SIGNATURE";

    /** La firma no verifica. */
    public static final String CODE_INVALID_SIGNATURE = "INVALID_SIGNATURE";

    /** El {@code signkeyid} no esta entre las claves publicas conocidas. */
    public static final String CODE_UNKNOWN_SIGNING_KEY = "UNKNOWN_SIGNING_KEY";

    /**
     * Clave mal formada, de otro algoritmo, o configuracion de firma incoherente.
     *
     * <p>Es un bug de configuracion LOCAL, no un mensaje ajeno corrupto: por eso no es una
     * {@link FluxErrors.PoisonException}. Se detecta al construir el firmante o el
     * verificador, es decir en {@code connect()}, y no en la ruta caliente.
     */
    public static final class SigningKeyException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public SigningKeyException(String message) {
            super(message);
        }

        public SigningKeyException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // ─── Opciones ────────────────────────────────────────────────────────────

    /** Configuracion de la extension de firma. */
    public static final class SigningOptions {
        private String privateKeyPem;
        private String keyId;
        private final Map<String, String> publicKeys = new LinkedHashMap<>();
        private VerificationMode verify = VerificationMode.OFF;
        private Consumer<String> onWarn;

        /**
         * Clave privada Ed25519 en PEM (PKCS#8) para firmar al publicar. Omitir para solo
         * verificar.
         *
         * <p><b>NUNCA se versiona</b> — 06-security.md §2.
         */
        public SigningOptions privateKeyPem(String v) {
            this.privateKeyPem = v;
            return this;
        }

        /** Id de la clave, formato {@code <servicio>-<n>}. Obligatorio si se firma. */
        public SigningOptions keyId(String v) {
            this.keyId = v;
            return this;
        }

        /**
         * Registra una clave publica conocida: {@code signkeyid} → PEM (SPKI).
         *
         * <p><b>Incluye aqui las claves RETIRADAS mientras exista algun evento firmado con
         * ellas.</b> El minimo es 90 dias —la retencion de la DLQ— mas el margen de los
         * backups. Retirar una clave impide EMITIR con ella, no VERIFICAR lo ya emitido:
         * tratar una clave retirada como invalida convierte una rotacion rutinaria en la
         * invalidacion retroactiva de todo el historial (07-signing.md §6).
         */
        public SigningOptions publicKey(String signKeyId, String pem) {
            this.publicKeys.put(signKeyId, pem);
            return this;
        }

        /** Politica de verificacion. Default {@link VerificationMode#OFF}. */
        public SigningOptions verify(VerificationMode v) {
            this.verify = v != null ? v : VerificationMode.OFF;
            return this;
        }

        /**
         * Donde va el aviso del modo {@link VerificationMode#WARN}. {@code null} usa
         * {@link System.Logger} con el nombre {@code flux.signing}.
         *
         * <p>No se reutiliza {@code ConnectOptions.logger} porque el verificador se
         * construye tambien suelto —en tests y en herramientas de verificacion offline—
         * donde no hay un bus del que colgarlo.
         */
        public SigningOptions onWarn(Consumer<String> v) {
            this.onWarn = v;
            return this;
        }

        /** El modo configurado. */
        public VerificationMode verifyMode() {
            return verify;
        }
    }

    // ─── Bytes firmados ──────────────────────────────────────────────────────

    /**
     * Los bytes canonicos que se firman: el evento SIN {@code signature} y SIN las
     * extensiones {@code dlq*} — 07-signing.md §5.
     *
     * <p>{@code signkeyid} SI entra. Si quedara fuera, un atacante podria cambiarlo para
     * que la verificacion buscara otra clave —una suya— y la firma seguiria "verificando".
     *
     * <p>Las {@code dlq*} se anaden DESPUES de firmar, asi que no estan cubiertas. Que se
     * quiten aqui es lo que hace que <b>un evento que ha pasado por la DLQ conserve su
     * firma valida</b>, y eso es lo correcto: el replay redistribuye un hecho ya emitido,
     * no crea uno nuevo (07-signing.md §5.1).
     */
    public static byte[] signablePayload(FluxEvent event) {
        return Envelope.serialize(event.withoutDlq().withoutSignature());
    }

    // ─── Firmante ────────────────────────────────────────────────────────────

    /** Anade {@code signkeyid} y {@code signature} a un evento. */
    @FunctionalInterface
    public interface Signer {
        /** Devuelve una copia firmada. El evento original no se toca. */
        FluxEvent sign(FluxEvent event);
    }

    /** Aplica la politica de {@link VerificationMode} sobre un evento entrante. */
    @FunctionalInterface
    public interface Verifier {
        /**
         * Comprueba la firma segun el modo configurado.
         *
         * @throws FluxErrors.PoisonException en modo {@link VerificationMode#REQUIRE}.
         */
        void check(FluxEvent event);
    }

    /**
     * Construye el firmante, o {@code null} si no hay clave privada configurada.
     *
     * <p>{@code null} y no un firmante que no hace nada: un servicio que solo verifica no
     * debe pagar ni una rama en la ruta de publicacion.
     */
    public static Signer createSigner(SigningOptions options) {
        if (options == null || options.privateKeyPem == null || options.privateKeyPem.isEmpty()) {
            return null;
        }
        if (options.keyId == null || options.keyId.isEmpty()) {
            throw new SigningKeyException(
                    "signing.privateKeyPem requiere signing.keyId: sin el, un verificador no sabe que "
                            + "clave publica usar (07-signing.md §4)");
        }

        PrivateKey key = decodePrivateKey(options.privateKeyPem);
        String keyId = options.keyId;

        return event -> {
            // `signkeyid` va DENTRO de lo firmado — 07-signing.md §5.
            FluxEvent conKeyId = event.withSigning(keyId, null);
            byte[] firma = sign(key, signablePayload(conKeyId));
            // base64url SIN padding — 07-signing.md §4.
            return conKeyId.withSigning(keyId, Base64.getUrlEncoder().withoutPadding().encodeToString(firma));
        };
    }

    /**
     * Construye el verificador, o {@code null} si el modo es {@link VerificationMode#OFF}.
     *
     * @throws SigningKeyException si se pide verificar sin claves publicas, o si alguna no
     *                             es Ed25519.
     */
    public static Verifier createVerifier(SigningOptions options) {
        VerificationMode mode = options != null ? options.verify : VerificationMode.OFF;
        if (mode == null || mode == VerificationMode.OFF) {
            return null;
        }

        Map<String, PublicKey> keys = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : options.publicKeys.entrySet()) {
            keys.put(e.getKey(), decodePublicKey(e.getKey(), e.getValue()));
        }
        if (keys.isEmpty()) {
            throw new SigningKeyException(
                    "signing.verify = " + mode + " requiere al menos una clave publica "
                            + "(SigningOptions.publicKey(...)). Incluye tambien las claves RETIRADAS "
                            + "mientras existan eventos firmados con ellas: el minimo son 90 dias, la "
                            + "retencion de la DLQ (07-signing.md §6)");
        }

        Consumer<String> warn = options.onWarn != null
                ? options.onWarn
                : message -> System.getLogger("flux.signing").log(System.Logger.Level.WARNING, message);

        return event -> {
            String firma = event.signature();
            if (firma == null || firma.isEmpty()) {
                fail(mode, warn, CODE_MISSING_SIGNATURE, "evento " + event.id() + " sin firma");
                return;
            }
            String keyId = event.signkeyid();
            PublicKey key = keyId != null ? keys.get(keyId) : null;
            if (key == null) {
                // Una clave DESCONOCIDA no es lo mismo que una RETIRADA: si el id no esta
                // en el mapa, el operador olvido conservar la publica al rotar, o el evento
                // viene de fuera del ecosistema.
                fail(mode, warn, CODE_UNKNOWN_SIGNING_KEY,
                        "evento " + event.id() + " firmado con signkeyid=" + quote(keyId)
                                + ", que no esta entre las claves conocidas. ¿Se retiro sin conservar "
                                + "la publica? Una clave retirada DEBE seguir verificando "
                                + "(07-signing.md §6)");
                return;
            }
            if (!verify(key, signablePayload(event), decodeSignature(firma))) {
                fail(mode, warn, CODE_INVALID_SIGNATURE,
                        "la firma del evento " + event.id() + " no verifica");
            }
        };
    }

    /**
     * En {@code require} lanza; en {@code warn} registra y deja pasar.
     *
     * <p>La {@link FluxErrors.PoisonException} es lo correcto y no un PERMANENT: un evento
     * cuya firma no cuadra no es un evento que el handler deba ver, igual que uno cuyo JSON
     * no parsea. Y es el unico caso que debe despertar a alguien — 04-errors.md §1.3.
     */
    private static void fail(VerificationMode mode, Consumer<String> warn, String code, String message) {
        if (mode == VerificationMode.REQUIRE) {
            throw new FluxErrors.PoisonException(message, code);
        }
        warn.accept("[flux] " + code + ": " + message);
    }

    private static String quote(String value) {
        return value == null ? "<ausente>" : "\"" + value + "\"";
    }

    // ─── Claves ──────────────────────────────────────────────────────────────

    /**
     * Un par Ed25519 en PEM (PKCS#8 privada, SPKI publica).
     *
     * @param privateKeyPem NUNCA se versiona ni se registra en logs — 06-security.md §2.
     * @param publicKeyPem  se conserva mientras exista un evento firmado con su par.
     */
    public record KeyPairPem(String privateKeyPem, String publicKeyPem) {
    }

    /** Genera un par Ed25519 en PEM. Comodidad para pruebas y para {@code flux keygen}. */
    public static KeyPairPem generateKeyPair() {
        try {
            KeyPair pair = KeyPairGenerator.getInstance(ALGORITHM).generateKeyPair();
            return new KeyPairPem(
                    toPem("PRIVATE KEY", pair.getPrivate().getEncoded()),
                    toPem("PUBLIC KEY", pair.getPublic().getEncoded()));
        } catch (GeneralSecurityException e) {
            throw new SigningKeyException("no se pudo generar el par Ed25519: " + e.getMessage(), e);
        }
    }

    private static PrivateKey decodePrivateKey(String pem) {
        try {
            // PKCS8EncodedKeySpec + KeyFactory("Ed25519"): si el PEM lleva una RSA, el
            // KeyFactory de Ed25519 la rechaza aqui, en connect(), y no en la primera
            // publicacion. El mensaje nombra el algoritmo esperado porque el del JDK
            // ("invalid key format") no dice cual era.
            return KeyFactory.getInstance(ALGORITHM)
                    .generatePrivate(new PKCS8EncodedKeySpec(decodePem(pem)));
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            throw new SigningKeyException(
                    "la clave privada no es una Ed25519 valida en PEM PKCS#8: " + e.getMessage()
                            + ". El protocolo NO negocia algoritmo a proposito: los formatos con "
                            + "algoritmo negociable acumulan una familia de vulnerabilidades que solo "
                            + "existe porque hay algo que negociar (07-signing.md §3)", e);
        }
    }

    private static PublicKey decodePublicKey(String signKeyId, String pem) {
        try {
            return KeyFactory.getInstance(ALGORITHM)
                    .generatePublic(new X509EncodedKeySpec(decodePem(pem)));
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            throw new SigningKeyException(
                    "la clave publica \"" + signKeyId + "\" no es una Ed25519 valida en PEM SPKI: "
                            + e.getMessage(), e);
        }
    }

    /**
     * Quita la armadura PEM y decodifica el DER.
     *
     * <p>{@code getMimeDecoder} y no {@code getDecoder}: el segundo revienta ante los
     * saltos de linea que todo PEM lleva cada 64 caracteres.
     */
    private static byte[] decodePem(String pem) {
        StringBuilder base64 = new StringBuilder();
        for (String line : pem.split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("-----")) {
                continue;
            }
            base64.append(trimmed);
        }
        if (base64.length() == 0) {
            throw new IllegalArgumentException("el PEM no contiene ningun bloque base64");
        }
        return Base64.getMimeDecoder().decode(base64.toString());
    }

    private static String toPem(String label, byte[] der) {
        String body = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(der);
        return "-----BEGIN " + label + "-----\n" + body + "\n-----END " + label + "-----\n";
    }

    /**
     * base64url SIN padding — 07-signing.md §4.
     *
     * <p>Se tolera el padding al DECODIFICAR aunque el protocolo no lo emita: rechazar un
     * {@code =} de mas convertiria en POISON un evento perfectamente verificable escrito por
     * un productor con una libreria menos cuidadosa. Emitir es estricto, leer es tolerante.
     */
    private static byte[] decodeSignature(String value) {
        try {
            String sinPadding = value.endsWith("=") ? value.replace("=", "") : value;
            return Base64.getUrlDecoder().decode(sinPadding);
        } catch (IllegalArgumentException e) {
            // Una firma que ni siquiera es base64url no puede verificar, y decirlo como
            // INVALID_SIGNATURE es mas util que propagar el error del decodificador.
            return new byte[0];
        }
    }

    // ─── Primitivas ──────────────────────────────────────────────────────────

    private static byte[] sign(PrivateKey key, byte[] payload) {
        try {
            Signature signature = Signature.getInstance(ALGORITHM);
            signature.initSign(key);
            signature.update(payload);
            return signature.sign();
        } catch (GeneralSecurityException e) {
            throw new SigningKeyException("fallo al firmar el evento: " + e.getMessage(), e);
        }
    }

    private static boolean verify(PublicKey key, byte[] payload, byte[] signature) {
        if (signature.length == 0) {
            return false;
        }
        try {
            Signature verifier = Signature.getInstance(ALGORITHM);
            verifier.initVerify(key);
            verifier.update(payload);
            return verifier.verify(signature);
        } catch (GeneralSecurityException e) {
            // Una firma de longitud equivocada hace que verify() lance en vez de devolver
            // false. Para el protocolo son el mismo caso: la firma no verifica.
            return false;
        }
    }

    /** El payload firmado como texto, util al depurar por que dos SDKs no coinciden. */
    static String signablePayloadAsString(FluxEvent event) {
        return new String(signablePayload(event), StandardCharsets.UTF_8);
    }
}
