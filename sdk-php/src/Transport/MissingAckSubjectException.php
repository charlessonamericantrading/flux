<?php

declare(strict_types=1);

namespace Flux\Transport;

/**
 * Un mensaje llegó del broker **sin subject de respuesta** (`$JS.ACK.…`).
 *
 * Es fatal, no transitorio: sin ese subject no se puede hacer ack, nak ni term, y el
 * número de entrega es indeterminable — con lo que todo el presupuesto de reintentos de
 * 04-errors.md deja de funcionar y los mensajes se reentregarían para siempre.
 *
 * Tiene tipo propio precisamente para poder distinguirlo dentro del `catch` genérico de
 * `fetch()`, que trata cualquier otro fallo como "no había nada que entregar" — un fetch
 * vacío llega como error de timeout en la mayoría de clientes. Sin este tipo, el fallo se
 * confundiría con la cola vacía y el worker giraría eternamente sin consumir nada, que es
 * exactamente el modo de fallo silencioso que el SDK debe evitar.
 */
final class MissingAckSubjectException extends \RuntimeException
{
}
