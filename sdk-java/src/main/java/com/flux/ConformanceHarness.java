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
 * Ademas de `build`, `dlq`, `sign`, `verify` y `parse`, implementa las cuatro operaciones de la
 * capacidad `state` (`state-build`, `state-tombstone`, `state-subject`, `state-dlq`), las cinco de la
 * capacidad `rpc` (`rpc-request`, `rpc-reply`, `rpc-error`, `rpc-operation`, `rpc-timeout`) y
 * `external-id` (el `id` determinista de un hecho importado, 01-envelope.md §2.6.1). Todas llaman a
 * las funciones PURAS del SDK ({@link StateEnvelope}, {@link RpcEnvelope}, {@link ExternalId}): el
 * arnes no sabe construir un sobre.
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
import java.time.Instant;
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
            salida = procesar(M.readTree(System.in.readAllBytes()));
        } catch (Throwable t) {
            // La entrada ni siquiera es JSON: el arnes esta roto, no el caso. Se reporta igual.
            salida = fallo("HARNESS_INPUT").put("detail", String.valueOf(t.getMessage()));
        }
        // Bytes crudos y no println: en Windows System.out convertiria el UTF-8 a la
        // codepage de la consola, que es justo lo que este arnes existe para comprobar.
        byte[] bytes = M.writeValueAsBytes(salida);
        System.out.write(bytes, 0, bytes.length);
        System.out.flush();
    }

    /**
     * Una operacion: la entrada JSON y su resultado. NUNCA lanza: un fallo de la operacion se REPORTA,
     * no se propaga (exit != 0 significaria que el arnes esta roto, no que el caso fallo). Es lo que
     * `VectorsTest` ejecuta en proceso con los mismos vectores que el runner cruzado.
     */
    static ObjectNode procesar(JsonNode entrada) {
        try {
            return ejecutar(entrada);
        } catch (Throwable t) {
            return fallo(codigoDe(t)).put("detail", String.valueOf(t.getMessage()));
        }
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
                        conDlq.dlqconsumer(), conDlq.dlqerror(),
                        StateEnvelope.canonicalTimeOrThrow(texto(d, "dlqtime")));
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

            // El `id` determinista de un hecho importado (01-envelope.md §2.6.1;
            // conformance/cases/cross-sdk-external-id.json). `externalKey: { repeat, times }` se expande para no
            // pegar 513 caracteres en el caso. Devuelve `{ ok: true, id }` o `{ ok: false }`.
            case "external-id": {
                JsonNode e = entrada.has("input") ? entrada.get("input") : entrada;
                try {
                    JsonNode k = e.get("externalKey");
                    String clave = k != null && k.isObject()
                            ? texto(k, "repeat").repeat(k.get("times").intValue())
                            : texto(e, "externalKey");
                    Instant time = CanonicalTime.instant(texto(e, "time"));
                    return ok().put("id", ExternalId.eventId(texto(e, "source"), clave, time));
                } catch (RuntimeException ex) {
                    return M.createObjectNode().put("ok", false);
                }
            }

            // ─── Estado replicado (capacidad `state`) ────────────────────────────────────────────
            case "state-build":
                return ok(Envelope.serialize(StateEnvelope.buildEvent(eventoDeEstado(entrada))));

            case "state-tombstone":
                return ok(Envelope.serialize(StateEnvelope.buildTombstone(new StateEnvelope.TombstoneInput()
                        .contract(texto(entrada, "contract"))
                        .tenantId(texto(entrada, "tenantId"))
                        .key(texto(entrada, "key"))
                        .id(texto(entrada, "id"))
                        .source(texto(entrada, "source"))
                        .time(texto(entrada, "time"))
                        .correlationid(texto(entrada, "correlationid"))
                        .producerversion(texto(entrada, "producerversion"))
                        .aggregateVersion(entrada.get("aggregateVersion"))
                        .causationid(texto(entrada, "causationid"))
                        .traceparent(texto(entrada, "traceparent"))
                        .tracestate(texto(entrada, "tracestate")))));

            case "state-subject":
                return M.createObjectNode().put("ok", true).put("subject", StateEnvelope.subject(
                        texto(entrada, "contract"), texto(entrada, "tenantId"), texto(entrada, "key")));

            case "state-dlq": {
                JsonNode d = entrada.get("dlq");
                FluxEvent original = StateEnvelope.buildEvent(eventoDeEstado(entrada.get("original")));
                return ok(Envelope.serialize(StateEnvelope.toDlqEvent(original, new StateEnvelope.DlqInfo(
                        original.aggregateId(), entrada.get("revision").longValue(),
                        FluxEvent.DlqReason.fromWire(texto(d, "reason")), d.get("attempts").intValue(),
                        texto(d, "consumer"), texto(d, "error"), texto(d, "dlqtime")))));
            }

            // ─── Peticion/respuesta (capacidad `rpc`) ────────────────────────────────────────────
            case "rpc-request":
                return ok(Envelope.serializeUnbounded(RpcEnvelope.buildRequest(new RpcEnvelope.RequestInput()
                        .operation(texto(entrada, "operation"))
                        .tenantId(texto(entrada, "tenantId"))
                        .aggregateId(texto(entrada, "aggregateId"))
                        .id(texto(entrada, "id"))
                        .source(texto(entrada, "source"))
                        .time(texto(entrada, "time"))
                        .correlationid(texto(entrada, "correlationid"))
                        .causationid(texto(entrada, "causationid"))
                        .producerversion(texto(entrada, "producerversion"))
                        .classification(texto(entrada, "classification"))
                        .data(entrada.get("data"))
                        .traceparent(texto(entrada, "traceparent"))
                        .tracestate(texto(entrada, "tracestate")))));

            case "rpc-reply":
                return ok(Envelope.serializeUnbounded(RpcEnvelope.buildReply(new RpcEnvelope.ReplyInput()
                        .request(referencia(entrada.get("request")))
                        .id(texto(entrada, "id"))
                        .source(texto(entrada, "source"))
                        .time(texto(entrada, "time"))
                        .producerversion(texto(entrada, "producerversion"))
                        .classification(texto(entrada, "classification"))
                        .data(entrada.get("data"))
                        .traceparent(texto(entrada, "traceparent")))));

            case "rpc-error":
                return ok(Envelope.serializeUnbounded(RpcEnvelope.buildError(new RpcEnvelope.ErrorInput()
                        .request(referencia(entrada.get("request")))
                        .id(texto(entrada, "id"))
                        .source(texto(entrada, "source"))
                        .time(texto(entrada, "time"))
                        .producerversion(texto(entrada, "producerversion"))
                        .error(entrada.get("error"))
                        .traceparent(texto(entrada, "traceparent")))));

            // El subject de NATS (`{ ok, subject }`) o el codigo estable del rechazo (`{ ok: false, code }`:
            // INVALID_RPC_CALL o RESERVED_DOMAIN).
            case "rpc-operation":
                return M.createObjectNode().put("ok", true).put("subject",
                        RpcEnvelope.subject(texto(entrada, "operation")));

            // Los milisegundos efectivos, o INVALID_RPC_CALL. `timeoutMs` ausente es 5000; `null` NO es «ausente».
            case "rpc-timeout":
                return M.createObjectNode().put("ok", true).put("effectiveMs",
                        RpcEnvelope.resolveTimeout(entrada.get("timeoutMs")));

            default:
                return fallo("UNSUPPORTED_OP").put("detail", op);
        }
    }

    /**
     * El arnes NO rellena nada: {@code id}, {@code time} y las extensiones vienen del
     * vector, o los bytes no serian comparables entre SDKs.
     */
    private static FluxEvent construir(JsonNode e) {
        String clasificacion = texto(e, "dataclassification");
        FluxEvent.DataClassification dataClassification = null;
        for (FluxEvent.DataClassification c : FluxEvent.DataClassification.values()) {
            if (c.wire().equals(clasificacion)) {
                dataClassification = c;
            }
        }
        if (dataClassification == null) {
            throw new StateErrors.InvalidDataClassificationException(clasificacion, false);
        }
        Envelope.BuildEventInput entrada = new Envelope.BuildEventInput()
                .subject(texto(e, "subject"))
                .data(e.get("data"))
                .id(texto(e, "id"))
                .source(texto(e, "source"))
                .dataSchema(texto(e, "dataschema"))
                .correlationId(texto(e, "correlationid"))
                .tenantId(texto(e, "tenantid"))
                .producerVersion(texto(e, "producerversion"))
                .dataClassification(dataClassification);

        if (e.hasNonNull("time")) {
            // `time` se NORMALIZA a la forma canonica (3 decimales y `Z`) o se rechaza: copiarlo tal cual
            // dejaria pasar `…39Z` o `…+02:00`, que no son los mismos bytes.
            entrada.time(CanonicalTime.instant(StateEnvelope.canonicalTimeOrThrow(texto(e, "time"))));
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

    /**
     * Estado replicado: lo mismo, el arnes NO rellena nada. `id` y `time` vienen del vector. `dataschema`
     * NO viene: lo deriva el SDK del contrato, que es parte de lo que se compara.
     */
    private static StateEnvelope.EventInput eventoDeEstado(JsonNode e) {
        return new StateEnvelope.EventInput()
                .contract(texto(e, "contract"))
                .tenantId(texto(e, "tenantId"))
                .key(texto(e, "key"))
                .id(texto(e, "id"))
                .source(texto(e, "source"))
                .time(texto(e, "time"))
                .correlationid(texto(e, "correlationid"))
                .producerversion(texto(e, "producerversion"))
                .classification(texto(e, "classification"))
                .data(e.get("data"))
                .causationid(texto(e, "causationid"))
                .traceparent(texto(e, "traceparent"))
                .tracestate(texto(e, "tracestate"));
    }

    /**
     * Lo que la respuesta y el error HEREDAN de la peticion. El `correlationid` de primer nivel que traen
     * los vectores es redundante y aqui no se usa.
     */
    private static RpcEnvelope.RequestRef referencia(JsonNode r) {
        return new RpcEnvelope.RequestRef(texto(r, "operation"), texto(r, "id"), texto(r, "correlationid"),
                texto(r, "tenantid"), texto(r, "subject"), texto(r, "partitionkey"));
    }

    private static Signing.Signer firmante(JsonNode signing) {
        return Signing.createSigner(new Signing.SigningOptions()
                .privateKeyPem(texto(signing, "privateKeyPem"))
                .keyId(texto(signing, "keyId")));
    }

    // ─── Entrada y salida ────────────────────────────────────────────────────

    private static String texto(JsonNode nodo, String campo) {
        JsonNode valor = nodo.get(campo);
        if (valor == null || valor.isNull()) {
            return null;
        }
        // Tipos cerrados: un valor de otro tipo JSON no se convierte en texto (`5` no es «5»). Es del
        // ARNES, no de la API publica, que es tipada (conformance/harness/README.md → «Lo que no se admite»).
        if (!valor.isTextual()) {
            throw new IllegalArgumentException("HARNESS_INPUT: `" + campo + "` debe ser un texto");
        }
        return valor.textValue();
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
        if (t instanceof FluxErrors.FluxException f) {
            return f.code();
        }
        if (t instanceof Envelope.EnvelopeException e && e.code() != null) {
            return e.code();
        }
        if (t instanceof IllegalArgumentException && String.valueOf(t.getMessage()).startsWith("HARNESS_INPUT")) {
            return "HARNESS_INPUT";
        }
        return t.getClass().getSimpleName();
    }
}
