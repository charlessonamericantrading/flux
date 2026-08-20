/*
 * Arnes de conformidad cruzada — SDK de Java.
 * Contrato: conformance/harness/README.md
 *
 * Lee UNA operacion por stdin, escribe UN resultado por stdout, sale con 0 SIEMPRE.
 * Deliberadamente delgado: toda logica aqui es logica que no esta en el SDK y que el
 * runner, por tanto, no verifica.
 *
 * ⚠️ Vive en el paquete `com.flux` por una razon concreta y no por comodidad:
 * `FluxEvent.withDlq(...)` es package-private y es lo UNICO que permite fijar el
 * `dlqtime` que da el vector. `Envelope.toDlqEvent` lo sella con `Instant.now()`, asi que
 * sin este acceso el arnes tendria que reescribir el JSON ya serializado —es decir,
 * reimplementar el envelope— y entonces el runner compararia el arnes en vez del SDK.
 * Ver la nota de `case "dlq"`.
 *
 * Se compila con `node conformance/harness/build-java.mjs` (javac + jar, sin Maven) y se
 * invoca como dice conformance/harnesses.json:
 *
 *   cd sdk-java && java -cp "target/harness/*" com.flux.ConformanceHarness
 */
package com.flux;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

/** Punto de entrada del arnes. No forma parte de la API del SDK. */
public final class ConformanceHarness {

    private ConformanceHarness() {
        throw new AssertionError("clase de utilidad");
    }

    /**
     * El mapper del SDK, no uno nuevo.
     *
     * <p>La entrada del arnes no es un envelope, pero el {@code data} del vector SI acaba
     * dentro de uno: leerlo con otro mapper le aplicaria otras reglas de numeros
     * ({@code USE_BIG_DECIMAL_FOR_FLOATS}) y el arnes emitiria bytes que el SDK nunca
     * emitiria.
     */
    private static final ObjectMapper M = Envelope.mapper();

    public static void main(String[] args) throws IOException {
        ObjectNode salida;
        try {
            salida = ejecutar(M.readTree(System.in.readAllBytes()));
        } catch (Throwable t) {
            // Un fallo de la operacion se REPORTA, no se propaga: exit != 0 significaria
            // que el arnes esta roto, no que el caso fallo.
            salida = fallo(codigoDe(t)).put("detail", String.valueOf(t.getMessage()));
        }
        // Bytes crudos y no println: en Windows System.out convertiria el UTF-8 a la
        // codepage de la consola, que es justo lo que este arnes existe para comprobar.
        byte[] bytes = M.writeValueAsBytes(salida);
        System.out.write(bytes, 0, bytes.length);
        System.out.flush();
    }

    private static ObjectNode ejecutar(JsonNode entrada) {
        String op = texto(entrada, "op");
        switch (op) {
            case "build":
                return ok(Envelope.serialize(construir(entrada.get("event"))));

            case "dlq": {
                FluxEvent evento = construir(entrada.get("event"));
                if (entrada.path("signFirst").asBoolean(false) && entrada.hasNonNull("signing")) {
                    evento = firmante(entrada.get("signing")).sign(evento);
                }
                JsonNode d = entrada.get("dlq");
                FluxEvent conDlq = Envelope.toDlqEvent(evento, new Envelope.DlqInfo(
                        FluxEvent.DlqReason.fromWire(texto(d, "reason")),
                        d.get("attempts").intValue(),
                        texto(d, "consumer"),
                        texto(d, "error")));
                // `dlqtime` lo fija el vector: si lo pusiera el SDK —y lo pone,
                // `toDlqEvent` sella `Instant.now()`— los bytes no serian comparables
                // entre ejecuciones, y mucho menos entre lenguajes. Se reescribe con
                // `withDlq` para no tocar nada mas del evento: el orden de claves, el
                // recorte de `dlqerror` y el resto siguen siendo los del SDK.
                conDlq = conDlq.withDlq(conDlq.dlqreason(), conDlq.dlqattempts(),
                        conDlq.dlqconsumer(), conDlq.dlqerror(), texto(d, "dlqtime"));
                return ok(Envelope.serialize(conDlq));
            }

            case "sign":
                return ok(Envelope.serialize(
                        firmante(entrada.get("signing")).sign(construir(entrada.get("event")))));

            case "verify": {
                FluxEvent evento = Envelope.parseEvent(decodificar(texto(entrada, "bytes")));
                Signing.SigningOptions opciones = new Signing.SigningOptions()
                        .verify(Signing.VerificationMode.valueOf(
                                entrada.path("mode").asText("require").toUpperCase(Locale.ROOT)));
                for (Iterator<Map.Entry<String, JsonNode>> it = entrada.get("publicKeys").fields();
                        it.hasNext(); ) {
                    Map.Entry<String, JsonNode> e = it.next();
                    opciones.publicKey(e.getKey(), e.getValue().asText());
                }
                try {
                    Signing.createVerifier(opciones).check(evento);
                    return ok();
                } catch (FluxErrors.FluxException e) {
                    return fallo(e.code());
                }
            }

            case "parse":
                Envelope.parseEvent(decodificar(texto(entrada, "bytes")));
                return ok();

            default:
                return fallo("UNSUPPORTED_OP").put("detail", op);
        }
    }

    /**
     * El arnes NO rellena nada: {@code id}, {@code time} y las extensiones vienen del
     * vector, o los bytes no serian comparables entre SDKs.
     */
    private static FluxEvent construir(JsonNode e) {
        Envelope.BuildEventInput entrada = new Envelope.BuildEventInput()
                .subject(texto(e, "subject"))
                .data(e.get("data"))
                .id(texto(e, "id"))
                .source(texto(e, "source"))
                .dataSchema(texto(e, "dataschema"))
                .correlationId(texto(e, "correlationid"))
                .tenantId(texto(e, "tenantid"))
                .producerVersion(texto(e, "producerversion"))
                .dataClassification(
                        FluxEvent.DataClassification.fromWire(texto(e, "dataclassification")));

        if (e.hasNonNull("time")) {
            entrada.time(Envelope.parseTime(texto(e, "time")));
        }
        if (e.hasNonNull("aggregateId")) {
            entrada.aggregateId(texto(e, "aggregateId"));
        }
        if (e.hasNonNull("causationid")) {
            entrada.causationId(texto(e, "causationid"));
        }
        if (e.hasNonNull("partitionkey")) {
            entrada.partitionKey(texto(e, "partitionkey"));
        }
        if (e.hasNonNull("traceparent")) {
            entrada.traceparent(texto(e, "traceparent"));
        }
        if (e.hasNonNull("tracestate")) {
            entrada.tracestate(texto(e, "tracestate"));
        }
        return entrada.build();
    }

    private static Signing.Signer firmante(JsonNode signing) {
        return Signing.createSigner(new Signing.SigningOptions()
                .privateKeyPem(texto(signing, "privateKeyPem"))
                .keyId(texto(signing, "keyId")));
    }

    // ─── Entrada y salida ────────────────────────────────────────────────────

    private static String texto(JsonNode nodo, String campo) {
        JsonNode valor = nodo.get(campo);
        return valor == null || valor.isNull() ? null : valor.asText();
    }

    private static byte[] decodificar(String base64) {
        return Base64.getDecoder().decode(base64.getBytes(StandardCharsets.US_ASCII));
    }

    private static ObjectNode ok() {
        return M.createObjectNode().put("ok", true);
    }

    /** {@code bytes} va en base64 ESTANDAR (con relleno), como el {@code Buffer} de Node. */
    private static ObjectNode ok(byte[] evento) {
        return ok().put("bytes", Base64.getEncoder().encodeToString(evento));
    }

    private static ObjectNode fallo(String code) {
        return M.createObjectNode().put("ok", false).put("code", code);
    }

    /**
     * El {@code code} del error, que es lo que el runner compara entre los siete SDKs.
     *
     * <p>Sin codigo propio se cae al nombre de la clase: un arnes que devolviera siempre
     * el mismo codigo generico haria pasar por identicos SDKs que clasifican distinto.
     */
    private static String codigoDe(Throwable t) {
        return t instanceof FluxErrors.FluxException f ? f.code() : t.getClass().getSimpleName();
    }
}
