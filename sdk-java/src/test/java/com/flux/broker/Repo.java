package com.flux.broker;

import java.nio.file.Files;
import java.nio.file.Path;

/** La raiz del repositorio: el primer ancestro con `protocol.json`. */
final class Repo {

    private Repo() {
        throw new AssertionError("clase de utilidad");
    }

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

    static Path path(String relative) {
        return root().resolve(relative);
    }
}
