/*
 * Firma Ed25519 de eventos.
 * Contrato normativo: specification/07-signing.md
 *
 * Los casos son los mismos que los del SDK de Node (test/signing.test.ts). Que la lista
 * coincida no es burocracia: la firma es lo unico del protocolo que solo funciona si dos
 * lenguajes producen exactamente los mismos bytes, asi que un caso que un SDK no cubra es
 * un caso donde la interoperabilidad no esta comprobada.
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.KeyPairGenerator;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SigningTest {

    private static final Signing.KeyPairPem PAR = Signing.generateKeyPair();

    private static final String KEY_ID = "pedidos-api-1";

    private static final Signing.Signer SIGNER = Signing.createSigner(new Signing.SigningOptions()
            .privateKeyPem(PAR.privateKeyPem())
            .keyId(KEY_ID));

    private static final Signing.Verifier VERIFIER = Signing.createVerifier(new Signing.SigningOptions()
            .publicKey(KEY_ID, PAR.publicKeyPem())
            .verify(Signing.VerificationMode.REQUIRE));

    /** El mismo evento de referencia que usa el SDK de Node en su suite de firma. */
    private static FluxEvent evento() {
        return evento(Map.of("pedidoId", "ped-123"));
    }

    private static FluxEvent evento(Object data) {
        return new Envelope.BuildEventInput()
                .subject("pedidos.pedido.v1.creado")
                .data(data)
                .id("01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55")
                .source("/produccion/pedidos-api")
                .time(Instant.ofEpochMilli(1_755_685_539_410L))
                .producerVersion("3.4.1")
                .tenantId("acme")
                .dataClassification(FluxEvent.DataClassification.INTERNAL)
                .dataSchema("https://schemas.internal/pedidos/pedido/creado/1.0.0.json")
                .correlationId("01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55")
                .build();
    }

    /** Reserializa el evento manipulando un atributo raiz, como haria un atacante. */
    private static FluxEvent manipular(FluxEvent firmado, String atributo, String valor) {
        ObjectNode raiz = (ObjectNode) Envelope.mapper().valueToTree(firmado);
        raiz.put(atributo, valor);
        return Envelope.parseEvent(raiz.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static String codigo(Throwable t) {
        return ((FluxErrors.PoisonException) t).code();
    }

    @Nested
    @DisplayName("firma")
    class Firma {

        @Test
        @DisplayName("anade signkeyid y signature, ambos antes de `data`")
        void anadeAtributosEnOrden() {
            FluxEvent firmado = SIGNER.sign(evento());

            assertEquals(KEY_ID, firmado.signkeyid());
            assertNotNull(firmado.signature());
            // base64url SIN padding — 07-signing.md §4.
            assertTrue(firmado.signature().matches("^[A-Za-z0-9_-]+$"),
                    "la firma debe ser base64url sin padding, llego: " + firmado.signature());

            List<String> claves = new ArrayList<>();
            Envelope.mapper().valueToTree(firmado).fieldNames().forEachRemaining(claves::add);
            assertEquals("data", claves.get(claves.size() - 1),
                    "`data` sigue siendo el ultimo — 01-envelope.md §6");
            assertTrue(claves.indexOf("signkeyid") < claves.indexOf("signature"));
            assertTrue(claves.indexOf("signature") < claves.indexOf("data"));
        }

        @Test
        @DisplayName("una firma valida verifica")
        void firmaValidaVerifica() {
            assertDoesNotThrow(() -> VERIFIER.check(SIGNER.sign(evento())));
        }

        @Test
        @DisplayName("es determinista: el mismo evento produce la misma firma")
        void determinista() {
            // Solo es cierto porque 01-envelope.md §1.1, §2.2 y §6 fijan una unica
            // representacion en bytes. Sin ellas firmar seria imposible entre lenguajes:
            // el mismo evento daria dos secuencias de bytes y dos firmas distintas.
            assertEquals(SIGNER.sign(evento()).signature(), SIGNER.sign(evento()).signature());
        }

        @Test
        @DisplayName("sobrevive a un round-trip de serializacion")
        void sobreviveAlRoundTrip() {
            FluxEvent firmado = SIGNER.sign(evento());
            FluxEvent ida = Envelope.parseEvent(Envelope.serialize(firmado));
            assertDoesNotThrow(() -> VERIFIER.check(ida));
        }

        @Test
        @DisplayName("lo firmado es el evento sin `signature` — el mismo serialize() del productor")
        void loFirmadoEsElSerializeDelProductor() {
            // No hay canonicalizacion aparte para firmar (07-signing.md §2). Este test lo
            // fija: si alguien introdujera una forma canonica propia, las firmas de Java
            // dejarian de verificar en Node sin que ningun otro test se enterase.
            FluxEvent firmado = SIGNER.sign(evento());
            String payload = Signing.signablePayloadAsString(firmado);
            assertTrue(payload.contains("\"signkeyid\":\"" + KEY_ID + "\""),
                    "signkeyid VA firmado — 07-signing.md §5");
            assertTrue(payload.endsWith("\"data\":{\"pedidoId\":\"ped-123\"}}"));
            assertTrue(!payload.contains("\"signature\""),
                    "una firma no puede cubrirse a si misma");
        }
    }

    @Nested
    @DisplayName("deteccion de manipulacion")
    class Manipulacion {

        @Test
        @DisplayName("alterar `data` invalida la firma")
        void alterarData() {
            ObjectNode raiz = (ObjectNode) Envelope.mapper().valueToTree(SIGNER.sign(evento()));
            ((ObjectNode) raiz.get("data")).put("pedidoId", "ped-999");
            FluxEvent falso = Envelope.parseEvent(raiz.toString().getBytes(StandardCharsets.UTF_8));

            Throwable e = assertThrows(FluxErrors.PoisonException.class, () -> VERIFIER.check(falso));
            assertEquals(Signing.CODE_INVALID_SIGNATURE, codigo(e));
        }

        @Test
        @DisplayName("alterar el tenantid invalida la firma")
        void alterarTenantId() {
            // El caso que la ACL del broker NO cubre: un evento sacado del stream, editado
            // y reinyectado. La ACL dice quien puede escribir en el dominio, no quien
            // escribio este evento concreto — 07-signing.md §1 y 09-multitenancy.md §4.
            FluxEvent falso = manipular(SIGNER.sign(evento()), "tenantid", "otro");
            Throwable e = assertThrows(FluxErrors.PoisonException.class, () -> VERIFIER.check(falso));
            assertEquals(Signing.CODE_INVALID_SIGNATURE, codigo(e));
        }

        @Test
        @DisplayName("cambiar signkeyid no permite eludir la verificacion")
        void cambiarSignKeyId() {
            // signkeyid va DENTRO de lo firmado justo para esto: si quedara fuera, un
            // atacante lo cambiaria por el id de una clave suya — 07-signing.md §5.
            FluxEvent falso = manipular(SIGNER.sign(evento()), "signkeyid", "otro-1");
            Throwable e = assertThrows(FluxErrors.PoisonException.class, () -> VERIFIER.check(falso));
            assertEquals(Signing.CODE_UNKNOWN_SIGNING_KEY, codigo(e));
        }

        @Test
        @DisplayName("una firma de otra clave no verifica aunque el signkeyid coincida")
        void impostorConElMismoKeyId() {
            Signing.KeyPairPem otra = Signing.generateKeyPair();
            Signing.Signer impostor = Signing.createSigner(new Signing.SigningOptions()
                    .privateKeyPem(otra.privateKeyPem())
                    .keyId(KEY_ID));

            Throwable e = assertThrows(FluxErrors.PoisonException.class,
                    () -> VERIFIER.check(impostor.sign(evento())));
            assertEquals(Signing.CODE_INVALID_SIGNATURE, codigo(e));
        }

        @Test
        @DisplayName("una firma que no es base64url valido es INVALID_SIGNATURE, no una excepcion cruda")
        void firmaBasura() {
            FluxEvent falso = manipular(SIGNER.sign(evento()), "signature", "no-es-base64!!");
            Throwable e = assertThrows(FluxErrors.PoisonException.class, () -> VERIFIER.check(falso));
            assertEquals(Signing.CODE_INVALID_SIGNATURE, codigo(e));
        }
    }

    @Nested
    @DisplayName("DLQ y replay")
    class DlqYReplay {

        @Test
        @DisplayName("la firma sigue verificando tras pasar por la DLQ")
        void sobreviveALaDlq() {
            // Las extensiones dlq* se anaden DESPUES de firmar y no estan cubiertas. Si la
            // verificacion no las ignorara, TODO evento de la DLQ pareceria manipulado y la
            // firma seria incompatible con el mecanismo de errores del propio protocolo.
            FluxEvent enDlq = Envelope.toDlqEvent(SIGNER.sign(evento()), new Envelope.DlqInfo(
                    FluxEvent.DlqReason.PERMANENT, 1,
                    "facturacion-api__pedidos_pedido_v1_creado", "PEDIDO_YA_CANCELADO"));
            assertDoesNotThrow(() -> VERIFIER.check(enDlq));
        }

        @Test
        @DisplayName("un evento reproducido desde la DLQ conserva su firma valida")
        void replayConservaLaFirma() {
            // El replay redistribuye un hecho ya emitido, no crea uno nuevo — §5.1.
            FluxEvent enDlq = Envelope.toDlqEvent(SIGNER.sign(evento()), new Envelope.DlqInfo(
                    FluxEvent.DlqReason.RETRYABLE, 6, "c", "x"));
            FluxEvent reproducido = Envelope.stripDlqExtensions(
                    Envelope.parseEvent(Envelope.serialize(enDlq)));
            assertDoesNotThrow(() -> VERIFIER.check(reproducido));
        }

        @Test
        @DisplayName("en la DLQ, signkeyid y signature van ANTES de las extensiones dlq*")
        void ordenEnLaDlq() {
            // Byte a byte con Node: alli toDlqEvent expande el resto del evento —que ya
            // lleva la firma— y anade las dlq* despues. Si Java las intercalara al reves, el
            // mismo evento de DLQ tendria dos representaciones y el replay verbatim dejaria
            // de serlo (01-envelope.md §6).
            FluxEvent enDlq = Envelope.toDlqEvent(SIGNER.sign(evento()), new Envelope.DlqInfo(
                    FluxEvent.DlqReason.POISON, 1, "c", "x"));
            String json = Envelope.serializeToString(enDlq);
            assertTrue(json.indexOf("\"signature\"") < json.indexOf("\"dlqreason\""));
            assertTrue(json.indexOf("\"dlqtime\"") < json.indexOf("\"data\""));
        }
    }

    @Nested
    @DisplayName("modos de verificacion")
    class Modos {

        @Test
        @DisplayName("require: un evento sin firma es POISON")
        void requireSinFirma() {
            Throwable e = assertThrows(FluxErrors.PoisonException.class, () -> VERIFIER.check(evento()));
            assertEquals(Signing.CODE_MISSING_SIGNATURE, codigo(e));
        }

        @Test
        @DisplayName("warn: registra y acepta")
        void warnRegistraYAcepta() {
            // `warn` existe porque adoptar la firma en un ecosistema en marcha exige un
            // periodo en el que unos productores firman y otros no. Pasar directo a
            // `require` convierte en POISON todo evento de un servicio aun no migrado.
            List<String> avisos = new ArrayList<>();
            Signing.Verifier v = Signing.createVerifier(new Signing.SigningOptions()
                    .publicKey(KEY_ID, PAR.publicKeyPem())
                    .verify(Signing.VerificationMode.WARN)
                    .onWarn(avisos::add));

            assertDoesNotThrow(() -> v.check(evento()));
            assertEquals(1, avisos.size());
            assertTrue(avisos.get(0).contains(Signing.CODE_MISSING_SIGNATURE));

            assertDoesNotThrow(() -> v.check(manipular(SIGNER.sign(evento()), "tenantid", "otro")));
            assertEquals(2, avisos.size());
            assertTrue(avisos.get(1).contains(Signing.CODE_INVALID_SIGNATURE));
        }

        @Test
        @DisplayName("off no construye verificador — no se paga lo que no se usa")
        void offNoConstruyeVerificador() {
            assertNull(Signing.createVerifier(new Signing.SigningOptions()));
            assertNull(Signing.createVerifier(
                    new Signing.SigningOptions().verify(Signing.VerificationMode.OFF)));
            assertNull(Signing.createVerifier(null));
        }

        @Test
        @DisplayName("sin clave privada no se construye firmante")
        void sinClavePrivadaNoHayFirmante() {
            assertNull(Signing.createSigner(new Signing.SigningOptions()));
            assertNull(Signing.createSigner(null));
        }
    }

    @Nested
    @DisplayName("gestion de claves")
    class Claves {

        @Test
        @DisplayName("firmar sin keyId falla con un mensaje accionable")
        void firmarSinKeyId() {
            Signing.SigningKeyException e = assertThrows(Signing.SigningKeyException.class,
                    () -> Signing.createSigner(
                            new Signing.SigningOptions().privateKeyPem(PAR.privateKeyPem())));
            assertTrue(e.getMessage().contains("keyId"));
        }

        @Test
        @DisplayName("rechaza una clave que no sea Ed25519")
        void rechazaClaveNoEd25519() throws Exception {
            // El protocolo NO negocia algoritmo: los formatos con algoritmo negociable
            // acumulan una familia de vulnerabilidades que solo existe porque hay algo que
            // negociar (07-signing.md §3). Una RSA se rechaza al construir, no al firmar.
            byte[] rsa = KeyPairGenerator.getInstance("RSA").generateKeyPair().getPrivate().getEncoded();
            String pem = "-----BEGIN PRIVATE KEY-----\n"
                    + Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(rsa)
                    + "\n-----END PRIVATE KEY-----\n";

            Signing.SigningKeyException e = assertThrows(Signing.SigningKeyException.class,
                    () -> Signing.createSigner(
                            new Signing.SigningOptions().privateKeyPem(pem).keyId("x-1")));
            assertTrue(e.getMessage().contains("Ed25519"));
        }

        @Test
        @DisplayName("verificar sin claves publicas falla explicando la retencion")
        void verificarSinClaves() {
            Signing.SigningKeyException e = assertThrows(Signing.SigningKeyException.class,
                    () -> Signing.createVerifier(
                            new Signing.SigningOptions().verify(Signing.VerificationMode.REQUIRE)));
            assertTrue(e.getMessage().contains("RETIRADAS"));
        }

        @Test
        @DisplayName("una clave RETIRADA sigue verificando si se conserva la publica")
        void claveRetiradaSigueVerificando() {
            // Es la regla que mas se equivoca. Retirar una clave impide EMITIR con ella, no
            // VERIFICAR lo ya emitido: un evento es un hecho del pasado y su firma era
            // valida cuando se emitio. Tratarla como invalida convierte una rotacion
            // rutinaria en la invalidacion retroactiva de todo el historial — §6.
            Signing.KeyPairPem vieja = Signing.generateKeyPair();
            Signing.KeyPairPem nueva = Signing.generateKeyPair();

            FluxEvent firmadoConLaVieja = Signing.createSigner(new Signing.SigningOptions()
                    .privateKeyPem(vieja.privateKeyPem())
                    .keyId("pedidos-api-1"))
                    .sign(evento());

            Signing.Verifier v = Signing.createVerifier(new Signing.SigningOptions()
                    .publicKey("pedidos-api-1", vieja.publicKeyPem())   // retirada, conservada
                    .publicKey("pedidos-api-2", nueva.publicKeyPem())   // activa
                    .verify(Signing.VerificationMode.REQUIRE));

            assertDoesNotThrow(() -> v.check(firmadoConLaVieja));
        }

        @Test
        @DisplayName("el par generado es PKCS#8 / SPKI y da la vuelta completa")
        void parGeneradoEsPemEstandar() {
            Signing.KeyPairPem par = Signing.generateKeyPair();
            assertTrue(par.privateKeyPem().startsWith("-----BEGIN PRIVATE KEY-----"));
            assertTrue(par.publicKeyPem().startsWith("-----BEGIN PUBLIC KEY-----"));

            Signing.Signer s = Signing.createSigner(
                    new Signing.SigningOptions().privateKeyPem(par.privateKeyPem()).keyId("k-1"));
            Signing.Verifier v = Signing.createVerifier(new Signing.SigningOptions()
                    .publicKey("k-1", par.publicKeyPem())
                    .verify(Signing.VerificationMode.REQUIRE));
            assertDoesNotThrow(() -> v.check(s.sign(evento())));
        }
    }
}
