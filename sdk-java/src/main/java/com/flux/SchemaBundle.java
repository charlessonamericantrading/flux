/*
 * Bundle de JSON Schemas desplegado CON el servicio.
 * Contrato normativo: specification/00-protocol.md §5, "Resolucion de esquemas: bundle, no HTTP".
 *
 * El `dataschema` de un evento es una URI, pero un SDK L3 NO DEBE resolverla por red al
 * publicar. La razon no es el coste de la peticion —que tambien—, sino la ventana de
 * inconsistencia: una cache con TTL hace que dos servicios validen contra versiones
 * distintas del MISMO esquema durante los minutos que dura el TTL, y ese fallo no produce
 * ningun error: produce dos verdades.
 *
 * En su lugar los esquemas se empaquetan (`scripts/bundle-schemas.mjs`) y viajan con el
 * artefacto del servicio. Asi la version del esquema queda clavada a la version del
 * servicio, que es justo lo que `producerversion` promete poder acotar.
 */
package com.flux;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * El {@code schemas/bundle.json} que genera {@code scripts/bundle-schemas.mjs}, ya leido.
 *
 * <p>Se pasa como DATO a {@link Validation.Options#bundle(SchemaBundle)}; el SDK no lo
 * descarga ni lo busca por su cuenta. Lo habitual es empaquetarlo como recurso del jar:
 *
 * <pre>{@code
 * SchemaBundle bundle;
 * try (InputStream in = App.class.getResourceAsStream("/bundle.json")) {
 *     bundle = SchemaBundle.fromStream(in);
 * }
 * }</pre>
 *
 * <p>Es inmutable y seguro de compartir entre hilos.
 */
public final class SchemaBundle {

    /** subject → URI del esquema con el MINOR mas alto de su mayor. */
    private final Map<String, String> subjects;

    /** URI → JSON Schema. */
    private final Map<String, JsonNode> schemas;

    private SchemaBundle(Map<String, String> subjects, Map<String, JsonNode> schemas) {
        this.subjects = Collections.unmodifiableMap(subjects);
        this.schemas = Collections.unmodifiableMap(schemas);
    }

    /** Lee el bundle desde su texto JSON. */
    public static SchemaBundle fromJson(String json) {
        if (json == null || json.isEmpty()) {
            throw new IllegalArgumentException("flux: el bundle de esquemas esta vacio");
        }
        return fromBytes(json.getBytes(StandardCharsets.UTF_8));
    }

    /** Lee el bundle desde sus bytes UTF-8. */
    public static SchemaBundle fromBytes(byte[] json) {
        JsonNode root;
        try {
            root = Envelope.mapper().readTree(json);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "flux: el bundle de esquemas no es JSON valido. Regeneralo con "
                            + "`node scripts/bundle-schemas.mjs`", e);
        }
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException(
                    "flux: el bundle de esquemas no es un objeto JSON");
        }

        Map<String, String> subjects = new LinkedHashMap<>();
        JsonNode subjectsNode = root.get("subjects");
        if (subjectsNode != null && subjectsNode.isObject()) {
            for (Iterator<String> it = subjectsNode.fieldNames(); it.hasNext(); ) {
                String subject = it.next();
                JsonNode uri = subjectsNode.get(subject);
                if (uri != null && uri.isTextual()) {
                    subjects.put(subject, uri.textValue());
                }
            }
        }

        Map<String, JsonNode> schemas = new LinkedHashMap<>();
        JsonNode schemasNode = root.get("schemas");
        if (schemasNode != null && schemasNode.isObject()) {
            for (Iterator<String> it = schemasNode.fieldNames(); it.hasNext(); ) {
                String uri = it.next();
                schemas.put(uri, schemasNode.get(uri));
            }
        }

        if (schemas.isEmpty()) {
            // Un bundle vacio no es un bundle: con `mode = STRICT` haria que TODO evento
            // fallara con SchemaNotFoundException, y el operador buscaria el problema en el
            // evento en vez de en el fichero que no se genero.
            throw new IllegalArgumentException(
                    "flux: el bundle no contiene ningun esquema (clave `schemas` ausente o vacia). "
                            + "Regeneralo con `node scripts/bundle-schemas.mjs`");
        }

        return new SchemaBundle(subjects, schemas);
    }

    /** Lee el bundle desde un recurso del classpath o cualquier otro flujo. */
    public static SchemaBundle fromStream(InputStream in) {
        if (in == null) {
            throw new IllegalArgumentException(
                    "flux: no se encontro el bundle de esquemas (el flujo es null). "
                            + "Comprueba la ruta del recurso");
        }
        try {
            return fromBytes(in.readAllBytes());
        } catch (IOException e) {
            throw new UncheckedIOException("flux: no se pudo leer el bundle de esquemas", e);
        }
    }

    /** Lee el bundle desde un fichero. */
    public static SchemaBundle fromPath(Path path) {
        try {
            return fromBytes(Files.readAllBytes(path));
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "flux: no se pudo leer el bundle de esquemas en " + path, e);
        }
    }

    /**
     * La URI de {@code dataschema} de un subject, o {@code null} si el bundle no lo conoce.
     *
     * <p>El bundle resuelve el MINOR exacto y no el {@code .0.0} del mayor: dentro de un
     * mayor todo es BACKWARD-compatible, asi que el MINOR mas alto acepta todo lo que
     * aceptan los anteriores — 00-protocol.md §5.
     */
    public String schemaUriFor(String subject) {
        return subjects.get(subject);
    }

    /** El esquema indexado por esa URI, o {@code null}. */
    public JsonNode schema(String uri) {
        return schemas.get(uri);
    }

    /** Mapa subject → URI, inmutable. */
    public Map<String, String> subjects() {
        return subjects;
    }

    /** Las URIs de esquema que contiene el bundle, inmutables. */
    public Set<String> schemaUris() {
        return schemas.keySet();
    }

    /** Cuantos esquemas trae. */
    public int size() {
        return schemas.size();
    }
}
