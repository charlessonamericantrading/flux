<?php

declare(strict_types=1);

namespace Flux\Validation;

/**
 * Los esquemas del servicio, empaquetados y desplegados **con él**.
 * Contrato normativo: specification/00-protocol.md §5, "Resolución de esquemas: bundle,
 * no HTTP".
 *
 * ## Por qué un bundle y no una URL
 *
 * El `dataschema` es una URI, y la tentación evidente es resolverla por red. Un SDK L3
 * **NO DEBE** hacerlo, por dos razones que no se arreglan con más ingeniería:
 *
 * 1. **Validar está en la ruta caliente.** Una petición HTTP por evento publicado es
 *    inaceptable en cualquier servicio con volumen.
 * 2. **Una caché con TTL abre una ventana de incoherencia.** Durante el TTL, dos servicios
 *    del mismo ecosistema validan contra versiones distintas del mismo esquema y ninguno
 *    de los dos se entera. El fallo no se manifiesta como un error: se manifiesta como que
 *    un evento pasa la validación en un servicio y la falla en otro.
 *
 * En su lugar los esquemas se empaquetan con `scripts/bundle-schemas.mjs` y se despliegan
 * junto al servicio. Así **la versión del esquema queda clavada a la versión del
 * servicio**, que es justo lo que `producerversion` promete poder acotar (01-envelope.md
 * §2.4): dado un evento sospechoso en la DLQ, su `producerversion` identifica el
 * despliegue, y el despliegue identifica el esquema exacto contra el que se validó.
 *
 * ## Qué más resuelve
 *
 * El bundle es además el ÚNICO sitio donde este SDK conoce el MINOR real de un subject.
 * Dentro de un mayor todo es BACKWARD-compatible (05-compatibility.md), así que el MINOR
 * más alto acepta todo lo que aceptan los anteriores y es el que hay que poner en
 * `dataschema`. Sin bundle solo se puede asumir el `.0.0` del mayor — suficiente para L2,
 * donde el atributo es informativo, pero no para L3, donde se valida contra él.
 */
final readonly class SchemaBundle
{
    /**
     * @param array<string,string> $subjects subject → URI del esquema con el MINOR más
     *        alto de su mayor.
     * @param array<string,array<string,mixed>> $schemas URI → JSON Schema.
     */
    public function __construct(
        public array $subjects = [],
        public array $schemas = [],
    ) {
    }

    /**
     * Carga el bundle desde el fichero que genera `scripts/bundle-schemas.mjs`.
     *
     * Se lee **una vez al arrancar**, no por evento: es un fichero en disco, pero un
     * `file_get_contents` por publicación en un worker con volumen se nota.
     *
     * @throws \InvalidArgumentException si el fichero no existe o no es legible
     * @throws \JsonException si el contenido no es JSON válido
     */
    public static function fromFile(string $path): self
    {
        $json = @file_get_contents($path);

        if ($json === false) {
            throw new \InvalidArgumentException(
                "no se pudo leer el bundle de esquemas en \"{$path}\". Genéralo con "
                . '`node scripts/bundle-schemas.mjs` y despliégalo CON el servicio: el bundle '
                . 'no se descarga en tiempo de ejecución a propósito (00-protocol.md §5).'
            );
        }

        return self::fromJson($json);
    }

    /**
     * Construye el bundle desde su JSON.
     *
     * @throws \JsonException si no es JSON válido
     */
    public static function fromJson(string $json): self
    {
        /** @var array<string,mixed> $decoded */
        $decoded = json_decode($json, true, 512, JSON_THROW_ON_ERROR);

        return self::fromArray($decoded);
    }

    /**
     * Construye el bundle desde el array ya decodificado.
     *
     * Las claves que no sean `subjects` ni `schemas` se ignoran: el fichero generado lleva
     * además `$comment`, `generatedFrom` y `count`, que son metadatos para el humano que
     * abra el fichero y no contrato para el SDK.
     *
     * @param array<string,mixed> $bundle
     */
    public static function fromArray(array $bundle): self
    {
        /** @var array<string,string> $subjects */
        $subjects = is_array($bundle['subjects'] ?? null) ? $bundle['subjects'] : [];
        /** @var array<string,array<string,mixed>> $schemas */
        $schemas = is_array($bundle['schemas'] ?? null) ? $bundle['schemas'] : [];

        return new self($subjects, $schemas);
    }

    /**
     * La URI de `dataschema` de un subject, o `null` si el bundle no lo conoce.
     *
     * `null` NO es un error aquí: un servicio puede publicar subjects cuyo esquema no
     * empaquetó. Quien decide qué hacer con esa ausencia es el modo de validación.
     */
    public function uriFor(string $subject): ?string
    {
        $uri = $this->subjects[$subject] ?? null;

        return ($uri === null || $uri === '') ? null : $uri;
    }

    /**
     * El JSON Schema de una URI, o `null` si no está empaquetado.
     *
     * @return array<string,mixed>|null
     */
    public function schemaFor(string $uri): ?array
    {
        return $this->schemas[$uri] ?? null;
    }

    /** ¿Hay algo que compilar? Un bundle vacío es casi siempre un bundle mal generado. */
    public function isEmpty(): bool
    {
        return $this->schemas === [];
    }
}
