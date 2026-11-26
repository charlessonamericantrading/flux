<?php

declare(strict_types=1);

namespace Flux;

/**
 * La forma canónica de `data` y del sobre — 01-envelope.md §1.1 y §4.1.
 *
 * Dos SDKs que reciben el **mismo** `data` deben escribir **los mismos bytes**: el orden de claves y
 * los bytes son normativos para el replay, la firma y la dedupe por hash. La forma canónica es la de
 * `JSON.stringify` con UTF-8 literal, que en PHP se consigue así:
 *
 * - **`JSON_UNESCAPED_UNICODE`**: sin ella `json_encode` escapa TODO lo no-ASCII (`é` → `é`).
 *   Ambas formas son JSON válido, pero no son los mismos BYTES.
 * - **`JSON_UNESCAPED_LINE_TERMINATORS`**: con la anterior, PHP sigue escapando U+2028 y U+2029
 *   (` `) por compatibilidad con JavaScript de 2010. `JSON.stringify` los escribe literales, y los
 *   vectores `ajuste-data-u2028-literal` lo comprueban.
 * - **`JSON_UNESCAPED_SLASHES`**: sin ella `/` → `\/`, y un `dataschema` con URL diferiría de los otros
 *   seis SDKs.
 * - **SIN `JSON_PRESERVE_ZERO_FRACTION`**: con ella `4995.0` sale `4995.0`; `JSON.stringify` escribe
 *   `4995`. Un flotante en `data` no tiene forma común entre lenguajes (01-envelope.md §4.1: «El dinero
 *   va en enteros»), pero un valor de punto flotante entero (`49.0`) es el caso más corriente y la
 *   referencia (Node) lo escribe sin `.0`.
 * - **`serialize_precision = -1`**: la representación más corta que vuelve a dar el mismo `double`, la
 *   misma que `JSON.stringify`. Con un `php.ini` que lo fije a 17, `0.1` saldría `0.10000000000000001`.
 */
final class Json
{
    public const FLAGS = JSON_UNESCAPED_UNICODE
        | JSON_UNESCAPED_SLASHES
        | JSON_UNESCAPED_LINE_TERMINATORS
        | JSON_THROW_ON_ERROR;

    private function __construct()
    {
    }

    /**
     * Serializa un valor a su forma canónica.
     *
     * @throws \JsonException
     */
    public static function encode(mixed $value): string
    {
        $previous = ini_get('serialize_precision');
        $changed = false;
        if ($previous !== '-1') {
            $changed = @ini_set('serialize_precision', '-1') !== false;
        }

        try {
            return json_encode($value, self::FLAGS);
        } finally {
            if ($changed) {
                @ini_set('serialize_precision', (string) $previous);
            }
        }
    }

    /**
     * Decodifica en **modo objeto**: `{}` y `[]` siguen siendo distintos, y una clave repetida conserva la
     * posición de la primera aparición y el valor de la última (que es lo que `JSON.parse` hace y lo que
     * §4.1 manda). Con `assoc = true` PHP colapsa `{}` y `[]` en el mismo `array` vacío.
     *
     * @throws \JsonException
     */
    public static function decodeObjects(string $json): mixed
    {
        return json_decode($json, false, 512, JSON_THROW_ON_ERROR);
    }

    /**
     * Convierte un árbol de `json_decode(…, false)` en el que acepta `json_encode` **sin perder qué era un
     * objeto**: un `{}` se queda en `new \stdClass` (si fuese `[]` saldría `[]`), y un objeto cuyas claves
     * serían una lista (`{"0":…}`) también. El resto pasa a `array`, que es lo que los handlers esperan
     * (`$evento->data['pedidoId']`).
     */
    public static function fromObjectTree(mixed $value): mixed
    {
        if ($value instanceof \stdClass) {
            $out = [];
            foreach (get_object_vars($value) as $k => $v) {
                $out[$k] = self::fromObjectTree($v);
            }
            if ($out === [] || array_is_list($out)) {
                return $out === [] ? new \stdClass() : $value;
            }

            return $out;
        }
        if (is_array($value)) {
            return array_map(self::fromObjectTree(...), $value);
        }

        return $value;
    }
}
