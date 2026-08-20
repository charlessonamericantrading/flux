/*
 * La UNICA clase del SDK que toca la libreria de validacion de JSON Schema.
 * Contrato normativo: specification/00-protocol.md §5 (nivel L3).
 *
 * Esta aislada a proposito: `com.networknt:json-schema-validator` es una dependencia
 * OPCIONAL del pom, y la carga de esta clase es lo unico que la exige. Un servicio en L2
 * —modo OFF— nunca llega aqui y por tanto nunca necesita el jar. Ver Validation.create().
 *
 * ⚠️ Version del meta-esquema. Los esquemas de flux declaran
 * `$schema: https://json-schema.org/draft/2020-12/schema`. Un validador configurado para
 * draft-07 NO falla con un error de version: falla con `no schema with key or ref
 * ".../draft/2020-12/schema"`, que no dice nada util y manda al operador a buscar un
 * fichero que no existe. De ahi que la version se fije explicitamente abajo
 * (`VersionFlag.V202012`) en vez de dejar que la detecte una heuristica.
 */
package com.flux;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import com.networknt.schema.resource.DisallowSchemaLoader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Implementacion de {@link Validation.Validator} sobre networknt json-schema-validator. */
final class NetworkntValidator implements Validation.Validator {

    /**
     * La version que declara el pom. Es una constante de compilacion a proposito: javac la
     * inlinea, asi que {@link Validation#create} puede citarla en su mensaje de error SIN
     * cargar esta clase — que es justo el caso en el que el jar no esta.
     */
    static final String LIBRARY_VERSION = "1.5.9";

    private final Validation.Mode mode;
    private final Map<String, JsonSchema> compiled;
    private final System.Logger logger;

    NetworkntValidator(Validation.Mode mode, SchemaBundle bundle, System.Logger logger) {
        this.mode = mode;
        this.logger = logger;

        // El texto de cada esquema, indexado por su URI. Es lo que alimenta al cargador de
        // abajo para que un `$ref` entre esquemas del bundle se resuelva DENTRO del bundle.
        Map<String, String> textos = new LinkedHashMap<>();
        for (String uri : bundle.schemaUris()) {
            try {
                textos.put(uri, Envelope.mapper().writeValueAsString(bundle.schema(uri)));
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(
                        "flux: el esquema " + uri + " del bundle no se pudo serializar", e);
            }
        }

        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(
                SpecVersion.VersionFlag.V202012,
                builder -> builder.schemaLoaders(loaders -> loaders.values(lista -> {
                    // Se REEMPLAZA la lista por completo, no se le anade: los cargadores por
                    // defecto incluyen uno que resuelve URIs por HTTP, y 00-protocol.md §5
                    // lo prohibe explicitamente ("NO DEBE resolverla por red"). Validar esta
                    // en la ruta caliente, y una cache con TTL abre una ventana en la que
                    // dos servicios validan contra versiones distintas del mismo esquema.
                    //
                    // El meta-esquema de 2020-12 no pasa por aqui: la libreria lo trae
                    // compilado, asi que bloquear la red no impide compilar nada.
                    lista.clear();
                    lista.add(new com.networknt.schema.resource.MapSchemaLoader(textos));
                    // Cualquier otra URI es un error explicito y no un silencio: un $ref a
                    // un esquema que no esta en el bundle debe romper el arranque, no
                    // colarse validando a medias.
                    lista.add(DisallowSchemaLoader.getInstance());
                })));

        this.compiled = new LinkedHashMap<>();
        for (String uri : bundle.schemaUris()) {
            // Se compila en connect(), UNA vez por esquema. Un fallo aqui —un esquema
            // corrupto— debe romper el arranque del servicio, no la primera publicacion.
            this.compiled.put(uri, factory.getSchema(bundle.schema(uri)));
        }
    }

    @Override
    public void check(FluxEvent event, String subject) {
        JsonSchema schema = compiled.get(event.dataschema());
        if (schema == null) {
            SchemaNotFoundException error = new SchemaNotFoundException(subject, event.dataschema());
            if (mode == Validation.Mode.STRICT) {
                throw error;
            }
            logger.log(System.Logger.Level.WARNING, "[flux] " + error.getMessage());
            return;
        }

        JsonNode data = event.data();
        Set<ValidationMessage> messages = schema.validate(data);
        if (messages.isEmpty()) {
            return;
        }

        SchemaValidationException error =
                new SchemaValidationException(subject, event.dataschema(), format(messages));
        if (mode == Validation.Mode.STRICT) {
            throw error;
        }
        logger.log(System.Logger.Level.WARNING, "[flux] " + error.getMessage());
    }

    /**
     * TODOS los fallos, ordenados y sin repetidos.
     *
     * <p>No se corta en el primero a proposito: es requisito explicito de L3
     * (00-protocol.md §5). De uno en uno, arreglar un payload con tres campos mal cuesta
     * tres despliegues.
     *
     * <p>El orden es alfabetico y no el de la libreria porque el de la libreria depende del
     * recorrido interno del validador: dos versiones del mismo jar pueden emitirlos en
     * distinto orden y un test que compare mensajes empezaria a fallar sin que nada
     * cambiara de verdad.
     */
    private static List<String> format(Set<ValidationMessage> messages) {
        Set<String> ordenados = new TreeSet<>();
        for (ValidationMessage message : messages) {
            // getMessage() ya viene con la ruta de la instancia delante
            // (`$.totalCents: string found, integer expected`), que es lo que el operador
            // necesita para saber QUE campo arreglar.
            ordenados.add(message.getMessage());
        }
        return new ArrayList<>(ordenados);
    }
}
