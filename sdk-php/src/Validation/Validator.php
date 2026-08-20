<?php

declare(strict_types=1);

namespace Flux\Validation;

use Flux\FluxEvent;
use Opis\JsonSchema\Errors\ErrorFormatter;
use Opis\JsonSchema\Helper;
use Opis\JsonSchema\Validator as OpisValidator;
use Psr\Log\LoggerInterface;

/**
 * Validación L3 del payload contra el JSON Schema que declara su `dataschema`.
 * Contrato normativo: specification/00-protocol.md §5 (nivel L3).
 *
 * Cierra el hueco más grande que quedaba en L2: sin esto, un productor puede publicar un
 * payload que viola su propio `dataschema` y nadie se entera hasta que un consumidor
 * —posiblemente de otro equipo, otro lenguaje y otra semana— se atraganta. El error
 * aparece lejísimos de su causa, y para entonces el evento malo ya está en el stream.
 * Validar en `publish()` lo convierte en un fallo del servicio que lo provocó.
 */
final class Validator
{
    /**
     * Techo de errores por payload. Cualquier número alto vale: lo que importa es que
     * **no sea 1**, que es el default de la librería. Ver `newOpisValidator()`.
     */
    private const MAX_ERRORS = 100;

    /**
     * @param array<string,true> $known URIs que el bundle empaqueta, para distinguir
     *        "no valida" de "no lo puedo comprobar" sin preguntarle al loader.
     */
    private function __construct(
        private readonly OpisValidator $opis,
        private readonly ValidationMode $mode,
        private readonly array $known,
        private readonly ?LoggerInterface $logger,
    ) {
    }

    /**
     * Compila los validadores del bundle, o devuelve `null` si no hay nada que validar.
     *
     * Se llama **una vez al conectar**, nunca por evento: parsear un JSON Schema no es
     * gratis y hacerlo en la ruta caliente tiraría el throughput del productor.
     *
     * @throws \RuntimeException si el modo exige validar y falta el bundle o la librería
     */
    public static function fromOptions(?ValidationOptions $options, ?LoggerInterface $logger = null): ?self
    {
        // Nivel L2: sin coste. No se compila nada y `opis/json-schema` no hace ninguna
        // falta — que es justo el motivo de que sea una dependencia opcional.
        if ($options === null || $options->mode === ValidationMode::Off) {
            return null;
        }

        $mode = $options->mode->value;

        if ($options->bundle === null || $options->bundle->isEmpty()) {
            throw new \RuntimeException(
                "validation.mode = \"{$mode}\" requiere un bundle de esquemas con contenido. "
                . 'Genéralo con `node scripts/bundle-schemas.mjs` y cárgalo con '
                . 'SchemaBundle::fromFile(). El SDK NO resuelve el `dataschema` por red: está '
                . 'en la ruta caliente y una caché con TTL abre una ventana en la que dos '
                . 'servicios validan contra versiones distintas del mismo esquema '
                . '(00-protocol.md §5).'
            );
        }

        self::assertLibraryAvailable($mode);

        $opis = self::newOpisValidator();
        $resolver = $opis->resolver();

        if ($resolver === null) {
            // No puede pasar con un Validator recién construido, pero un `?->` silencioso
            // dejaría el registro vacío y CADA validación fallaría con un "Schema not found"
            // que apunta al sitio equivocado. Mejor caer aquí, al arrancar.
            throw new \RuntimeException(
                'opis/json-schema devolvió un validador sin resolver, así que los esquemas del '
                . 'bundle no se pueden registrar. Revisa la versión instalada (se espera ^2.4).'
            );
        }

        $known = [];

        foreach ($options->bundle->schemas as $uri => $schema) {
            // Registrar TODOS los esquemas del bundle antes de validar nada tiene un efecto
            // que no es cosmético: un `$ref` entre esquemas del bundle se resuelve contra el
            // registro y **nunca** contra la red. Opis no trae ningún manejador de `https://`,
            // así que una URI no registrada falla al resolverse en vez de salir a Internet —
            // exactamente la propiedad que 00-protocol.md §5 exige.
            $resolver->registerRaw(Helper::toJSON($schema), $uri);
            $known[$uri] = true;
        }

        return new self($opis, $options->mode, $known, $logger);
    }

    /**
     * Comprueba que `opis/json-schema` está instalada.
     *
     * Es una dependencia **opcional**, por el mismo motivo que `ext-sodium` lo es para la
     * firma: L3 es opt-in, así que su coste también debe serlo. Un servicio en L2 no debería
     * arrastrar un validador de JSON Schema —con su árbol de dependencias y su superficie de
     * auditoría— que no va a ejecutar nunca.
     *
     * Sin esta comprobación el fallo sería `Class "Opis\JsonSchema\Validator" not found`,
     * que no dice qué instalar ni por qué falta.
     *
     * @throws \RuntimeException
     */
    private static function assertLibraryAvailable(string $mode): void
    {
        if (class_exists(OpisValidator::class)) {
            return;
        }

        throw new \RuntimeException(
            "validation.mode = \"{$mode}\" necesita el paquete `opis/json-schema`, que es una "
            . 'dependencia OPCIONAL de este SDK: L3 es opt-in, así que su coste también.' . "\n"
            . '  composer require opis/json-schema' . "\n"
            . 'Hace falta una versión con soporte de draft 2020-12 (^2.4): los esquemas de flux '
            . 'declaran `$schema: .../draft/2020-12/schema`, y un validador de draft-07 NO da un '
            . 'error de versión — da `no schema with key or ref`, que no dice nada útil '
            . '(00-protocol.md §5).'
        );
    }

    /**
     * Un validador de opis configurado para **reportar TODOS los errores**.
     *
     * ⚠️ Los dos ajustes son obligatorios y los dos vienen mal por defecto: opis arranca con
     * `maxErrors = 1`. Sin tocarlo, un payload con tres campos mal reporta uno, el
     * desarrollador lo arregla, despliega, y descubre el segundo — **tres despliegues para
     * arreglar un evento**, que es exactamente lo que 00-protocol.md §5 prohíbe ("DEBE
     * reportar TODOS los errores de validación, no solo el primero").
     *
     * `stopAtFirstError` se deja en `true` **a propósito**, que es lo contrario de lo que
     * parece: no limita el número de errores reportados —eso lo hace `maxErrors`— sino que
     * evita que los aplicadores sigan evaluando después de fallar. Ponerlo en `false` hace
     * que `additionalProperties` reporte el conjunto entero de propiedades examinadas, así
     * que un payload con dos campos mal produce además un error que dice que `pedidoId`,
     * `clienteId` y todos los demás campos **válidos** "no están permitidos". Verificado
     * contra opis 2.6.0: es ruido activamente engañoso en el sitio donde alguien está
     * intentando entender por qué su evento no pasa.
     */
    private static function newOpisValidator(): OpisValidator
    {
        $opis = new OpisValidator();
        $opis->setMaxErrors(self::MAX_ERRORS);
        $opis->setStopAtFirstError(true);

        return $opis;
    }

    /**
     * Valida el `data` del evento contra su `dataschema`.
     *
     * En `Strict` lanza; en `Warn` registra y devuelve. El subject se pasa aparte porque el
     * evento solo lleva el `type` de CloudEvents, y el mensaje de error tiene que nombrar
     * lo que el desarrollador escribió.
     *
     * @throws SchemaValidationError si el payload no cumple y el modo es `Strict`
     * @throws SchemaNotFoundError si el esquema no está en el bundle y el modo es `Strict`
     */
    public function validate(FluxEvent $event, string $subject): void
    {
        if (!isset($this->known[$event->dataschema])) {
            $error = new SchemaNotFoundError($subject, $event->dataschema);

            if ($this->mode === ValidationMode::Strict) {
                throw $error;
            }

            $this->logger?->warning('[flux] ' . $error->getMessage());

            return;
        }

        $result = $this->opis->validate(self::payloadOf($event), $event->dataschema);

        if ($result->isValid()) {
            return;
        }

        /** @var \Opis\JsonSchema\Errors\ValidationError $rootError */
        $rootError = $result->error();
        $error = new SchemaValidationError($subject, $event->dataschema, self::flatten($rootError));

        if ($this->mode === ValidationMode::Strict) {
            throw $error;
        }

        $this->logger?->warning('[flux] ' . $error->getMessage());
    }

    /**
     * El `data` del evento tal y como viajará por el cable.
     *
     * ⚠️ El caso vacío no es un detalle: `Envelope::serialize()` convierte un `data === []`
     * en `{}` porque el payload de un evento es un objeto JSON en la raíz (01-envelope.md
     * §4), y en PHP un array vacío es ambiguo entre objeto y lista. Si aquí no se hiciera lo
     * mismo, el validador vería una LISTA donde el cable lleva un OBJETO y reportaría un
     * "must match the type: object" que no se corresponde con nada de lo publicado — el peor
     * tipo de falso positivo, porque manda a alguien a buscar un bug que no existe.
     */
    private static function payloadOf(FluxEvent $event): mixed
    {
        if ($event->data === []) {
            return new \stdClass();
        }

        return Helper::toJSON($event->data);
    }

    /**
     * Aplana el árbol de errores de opis a una línea por incumplimiento, con su ruta.
     *
     * `formatKeyed` indexa por puntero JSON del **dato** (`/lineas/0/cantidad`) en vez de
     * por el del esquema, que es lo que le sirve a quien tiene que arreglar el payload.
     * La raíz se nombra `(raíz)` y no `/` por lo mismo: `"/ The required properties … are
     * missing"` se lee como una ruta a medias.
     *
     * @return list<string>
     */
    private static function flatten(\Opis\JsonSchema\Errors\ValidationError $error): array
    {
        $formatted = (new ErrorFormatter())->formatKeyed($error);

        $lines = [];
        foreach ($formatted as $pointer => $messages) {
            $where = ($pointer === '/' || $pointer === '') ? '(raíz)' : $pointer;
            foreach ($messages as $message) {
                $lines[] = "{$where} {$message}";
            }
        }

        // No debería ocurrir —un error sin mensaje no es un error— pero un array vacío
        // produciría un `SchemaValidationError` que dice que algo falla sin decir qué, y eso
        // es peor que no validar.
        return $lines === [] ? ['el payload no cumple el esquema (sin detalle)'] : $lines;
    }
}
