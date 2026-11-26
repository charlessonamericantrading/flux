/*
 * Donde esta el repositorio: los tests leen `protocol.json`, los vectores y los casos de conformancia
 * de la raiz, no copias.
 */
package com.flux;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

final class Repo {

    private Repo() {
        throw new AssertionError("clase de utilidad");
    }

    /** La raiz del repositorio: el primer ancestro con `protocol.json`. */
    static Path root() {
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null) {
            if (Files.exists(dir.resolve("protocol.json"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("no se encontro la raiz del repo (protocol.json)");
    }

    static JsonNode json(String relative) {
        try {
            return Envelope.mapper().readTree(Files.readAllBytes(root().resolve(relative)));
        } catch (IOException e) {
            throw new IllegalStateException("no se pudo leer " + relative, e);
        }
    }
}
