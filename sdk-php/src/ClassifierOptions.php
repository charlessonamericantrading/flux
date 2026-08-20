<?php

declare(strict_types=1);

namespace Flux;

/**
 * Política de clasificación de errores — 04-errors.md §2.
 *
 * Es política, no infraestructura, y por eso es un parámetro y no una constante: el
 * equilibrio correcto depende de cómo fallen las dependencias de cada ecosistema.
 */
final readonly class ClassifierOptions
{
    /**
     * @param 'permanent'|'retryable'|'retryable-bounded' $unknownErrorPolicy
     *        Qué hacer con un error que no encaja en ninguna regla conocida.
     *
     *        `retryable-bounded` (default de la spec) — reintenta, pero con un presupuesto
     *        reducido (`unknownRetryBudget`, 2 entregas) en vez de los 6 completos. Un
     *        transitorio se recupera en el segundo intento; un sistemático llega a la DLQ
     *        en ~30 s sin atascar la cola. **Domina a las otras dos**: cuesta 30 s de
     *        latencia sobre los permanentes genuinos y elimina los dos modos de fallo.
     *
     *        `permanent` — a la DLQ sin gastar reintentos. Falla rápido, pero un hipo de
     *        red manda a la DLQ un evento perfectamente válido y alguien lo reproduce a
     *        mano cada mañana.
     *
     *        `retryable` — backoff completo, 51 minutos. Solo si vuestras dependencias
     *        internas tienen hipos frecuentes y podéis asumir que un modo de fallo nuevo
     *        atasque la cola y se amplifique con cada mensaje siguiente.
     * @param int $unknownRetryBudget Entregas máximas para un error desconocido con la
     *        política acotada. Incluye la primera entrega, así que 2 = un reintento.
     * @param 'permanent'|'retryable' $timeoutPolicy ¿Un timeout es "el mundo va lento" o
     *        "esta operación no cabe en la ventana"? El default es `retryable`.
     * @param list<callable(\Throwable):?Classification> $rules Reglas propias, evaluadas
     *        antes que todo lo demás salvo las excepciones tipadas de flux. Devuelven
     *        `null` para ceder al siguiente criterio.
     * @param list<class-string|string> $transientExceptions Clases/interfaces cuya
     *        presencia significa "fallo del entorno". Se comprueban con `instanceof`, así
     *        que un nombre de una librería no instalada simplemente no casa nunca.
     * @param list<class-string|string> $permanentExceptions Ídem para "esto no mejora
     *        esperando".
     */
    public function __construct(
        public string $unknownErrorPolicy = 'retryable-bounded',
        public int $unknownRetryBudget = 2,
        public string $timeoutPolicy = 'retryable',
        public array $rules = [],
        public array $transientExceptions = Classifier::DEFAULT_TRANSIENT_EXCEPTIONS,
        public array $permanentExceptions = Classifier::DEFAULT_PERMANENT_EXCEPTIONS,
    ) {
    }
}
