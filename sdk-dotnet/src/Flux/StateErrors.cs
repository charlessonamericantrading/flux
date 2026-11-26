// Errores de la capacidad `state`.
// Contrato normativo: specification/04-errors.md §6 y specification/10-state.md.
//
// La lista es CERRADA: los siete códigos de abajo y ninguno más. Un caso que no encaje en ninguno es un hueco de la spec,
// no un código nuevo que este SDK pueda inventar.
//
// Reglas comunes (04-errors.md §6, «Qué DEBE decir el mensaje»):
//   · el `code` es fijo y la aplicación no puede sustituirlo: es lo que agrupa el panel y encabeza `dlqerror`;
//   · el mensaje dice QUÉ está mal y CÓMO se arregla;
//   · NINGÚN mensaje lleva valores de `data`: acaba en logs y en `dlqerror` durante 90 días, y en estado `data` es dato
//     personal. Una clave y un tenant sí pueden citarse: son identificadores opacos.

using System.Text.Json;

namespace Flux;

/// <summary>Cita un identificador opaco sin dejar que un valor largo o raro llene un log.</summary>
internal static class Cita
{
    public static string De(object? valor)
    {
        if (valor is not string s)
        {
            return valor is null ? "un valor nulo" : "un valor de tipo " + valor.GetType().Name;
        }

        var json = JsonSerializer.Serialize(s, Envelope.JsonOptions);
        return json.Length > 70 ? json[..67] + "…\"" : json;
    }
}

/// <summary>
/// Un contrato que no es <c>&lt;dominio&gt;.&lt;agregado&gt;.v&lt;major&gt;</c>. PERMANENT. Un dominio reservado NO es este error:
/// es <see cref="ReservedDomainException"/>, que se evalúa antes (10-state.md §3.6).
/// </summary>
public sealed class InvalidStateContractException : ClassifiedException
{
    /// <summary>Construye la excepción.</summary>
    public InvalidStateContractException(string? contract, string reason)
        : base(
            $"contrato de estado inválido {Cita.De(contract)}: {reason}. Un contrato es `<dominio>.<agregado>.v<major>` " +
            "—el subject del estado sin `state.`, sin tenant y sin clave—, en minúsculas y kebab-case: " +
            "`clientes.cliente.v1` (10-state.md §2.2).",
            "INVALID_STATE_CONTRACT",
            null)
    {
        Contract = contract;
    }

    /// <summary>El contrato rechazado.</summary>
    public string? Contract { get; }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Permanent;
}

/// <summary>El tenant no es válido para estado, o no se pudo resolver ninguno. PERMANENT.</summary>
public sealed class InvalidTenantForStateException : ClassifiedException
{
    /// <summary>Construye la excepción.</summary>
    public InvalidTenantForStateException(string? tenant)
        : base(
            tenant is null
                ? "no hay tenant para esta operación de estado. En estado NO hay tenant por defecto (ni \"system\" implícito): " +
                  "el tenant va en el subject, y uno equivocado no es una etiqueta mal puesta sino un dato guardado en otro " +
                  "sitio. Pásalo en la opción `TenantId` o fija `TenantId` en ConnectAsync (10-state.md §2.4)."
                : $"el tenant {Cita.De(tenant)} no es válido para estado: debe cumplir ^[a-z0-9]+(-[a-z0-9]+)*$ y tener como " +
                  "máximo 64 caracteres. El tenant de un EVENTO puede ser cualquier cadena; el de un estado, no, porque va " +
                  "dentro del subject de NATS (10-state.md §2.4).",
            "INVALID_TENANT_FOR_STATE",
            null)
    {
        Tenant = tenant;
    }

    /// <summary>El tenant rechazado, o <see langword="null"/> si no había ninguno.</summary>
    public string? Tenant { get; }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Permanent;
}

/// <summary>La clave no cabe en un token de NATS. PERMANENT.</summary>
public sealed class InvalidStateKeyException : ClassifiedException
{
    /// <summary>Construye la excepción.</summary>
    public InvalidStateKeyException(string? key)
        : base(
            $"la clave {Cita.De(key)} no es válida: debe cumplir ^[A-Za-z0-9][A-Za-z0-9_-]{{0,127}}$ (un único token de NATS: " +
            "sin puntos, comodines ni espacios, de 1 a 128 caracteres y sin empezar por «_», que está reservado al protocolo). " +
            "El broker NO la valida —acepta `*`, `a.b` y 2000 caracteres— y una clave sin validar es un subject fantasma que " +
            "ningún observador recibe, sin error. Tampoco se escapa sola: dos escapes distintos serían dos claves para el " +
            "mismo agregado. Usa un identificador opaco, nunca un email (10-state.md §2.3).",
            "INVALID_STATE_KEY",
            null)
    {
        Key = key;
    }

    /// <summary>La clave rechazada.</summary>
    public string? Key { get; }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Permanent;
}

/// <summary>
/// <c>data</c> no lleva un <c>aggregateVersion</c> válido. PERMANENT. El mensaje dice QUÉ tipo de valor llegó, nunca el
/// valor de un campo de texto: <c>data</c> es dato personal. Un número sí se cita (una versión no es PII).
/// </summary>
public sealed class MissingAggregateVersionException : ClassifiedException
{
    /// <summary>Construye la excepción.</summary>
    /// <param name="received">Lo que llegó, ya descrito sin valores de texto.</param>
    public MissingAggregateVersionException(string received)
        : base(
            "`data.aggregateVersion` debe ser un entero ≥ 1 y ≤ 9007199254740991 (2^53 − 1); " + received + ". Lo pone el DUEÑO del " +
            "agregado —es su contador monotónico, el mismo que llevan sus eventos—: el SDK ni lo inventa ni lo incrementa. " +
            "Ausente, null, una cadena, un decimal, 0 o un negativo son todos este error (10-state.md §3.4).",
            "MISSING_AGGREGATE_VERSION",
            null)
    {
    }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Permanent;
}

/// <summary>
/// El sobre de una entrada no coincide con el subject de NATS en el que llegó. <b>POISON</b>: el mensaje es legible pero
/// MIENTE sobre su identidad —su tenant, su clave o su tipo—, y un productor que lo hace está roto o es hostil.
/// </summary>
public sealed class StateSubjectMismatchException : ClassifiedException
{
    /// <summary>Construye la excepción.</summary>
    public StateSubjectMismatchException(string attribute, string? envelopeValue, string subjectValue)
        : base(
            $"el atributo `{attribute}` del sobre ({Cita.De(envelopeValue)}) no coincide con el del subject de NATS en el que " +
            $"llegó la entrada ({Cita.De(subjectValue)}). La identidad de una entrada está en el subject; el sobre la repite, y " +
            "si discrepan uno de los dos miente y quien consume no puede saber cuál. La entrada NO se aplica: la clave " +
            "conserva su valor anterior (10-state.md §5.4).",
            "STATE_SUBJECT_MISMATCH",
            null)
    {
        Attribute = attribute;
    }

    /// <summary><c>tenantid</c>, <c>subject</c> o <c>type</c>.</summary>
    public string Attribute { get; }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Poison;
}

/// <summary>
/// El chequeo de secuencia agotó sus tres intentos. <b>RETRYABLE</b>: el conflicto es del entorno —otra instancia
/// escribe la misma clave—, no de la llamada.
/// </summary>
public sealed class StateConflictException : ClassifiedException
{
    /// <summary>Construye la excepción.</summary>
    public StateConflictException(string contract, string key, int attempts)
        : base(
            $"la clave {Cita.De(key)} de \"{contract}\" cambió entre la lectura y la publicación las {attempts} veces (otra " +
            $"instancia del dueño escribe la misma clave): se agotaron los {attempts} intentos del put condicional. Es " +
            "reintentable: un nuevo intento relee y suele resolverlo, o termina en `stale` (10-state.md §3.2).",
            "STATE_CONFLICT",
            null)
    {
        Contract = contract;
        Key = key;
        Attempts = attempts;
    }

    /// <summary>El contrato.</summary>
    public string Contract { get; }

    /// <summary>La clave.</summary>
    public string Key { get; }

    /// <summary>Los intentos gastados.</summary>
    public int Attempts { get; }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Retryable;
}

/// <summary>
/// Un subject, un contrato o una operación usa un dominio reservado (<c>dlq</c>, <c>state</c>, <c>rpc</c>, <c>system</c>).
/// PERMANENT. Cada uno es un espacio de nombres del protocolo, y un dominio con ese nombre colisiona con él EN EL BROKER:
/// <c>subjects overlap with an existing stream</c> (A-43).
/// </summary>
public sealed class ReservedDomainException : ClassifiedException
{
    /// <summary>Los dominios reservados — protocol.json → naming.reservedDomains.</summary>
    public static readonly IReadOnlyList<string> Reserved = new[] { "dlq", "state", "rpc", "system" };

    /// <summary>Construye la excepción.</summary>
    /// <param name="domain">El dominio.</param>
    /// <param name="what"><c>subject</c>, <c>contrato</c> u <c>operación</c>.</param>
    public ReservedDomainException(string domain, string what = "subject")
        : base(
            $"el dominio \"{domain}\" está reservado al protocolo (reservados: {string.Join(", ", Reserved)}) y no puede usarse en " +
            $"{(what == "operación" ? "una" : "un")} {what}. Cada uno es un espacio de nombres propio —`state.`, `dlq.`, `rpc.`—, y " +
            "un evento publicado ahí no pasaría por el chequeo de secuencia del estado (10-state.md §2.7). Elige otro dominio." +
            (what == "operación" && domain == "rpc"
                ? " Si lo que escribiste es el subject de NATS (`rpc.clientes.cliente.v1.obtener`), quita el prefijo `rpc.`: lo " +
                  "pone el SDK, y el nombre de la operación es `clientes.cliente.v1.obtener` (11-request-reply.md §2.2)."
                : string.Empty),
            "RESERVED_DOMAIN",
            null)
    {
        Domain = domain;
    }

    /// <summary>El dominio reservado.</summary>
    public string Domain { get; }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Permanent;

    /// <summary>Lanza si el primer token de <paramref name="subjectOrContract"/> es un dominio reservado.</summary>
    public static void AssertNot(string subjectOrContract, string what = "subject")
    {
        var domain = subjectOrContract.Split('.')[0];
        if (Reserved.Contains(domain))
        {
            throw new ReservedDomainException(domain, what);
        }
    }
}

/// <summary>
/// Una clasificación no admisible: en estado <c>restricted</c> (o una fuera del enum); en una petición, una fuera del enum.
/// PERMANENT. Al consumir una entrada de estado con <c>restricted</c> es POISON (<see cref="RestrictedStateEntryException"/>).
/// </summary>
public sealed class InvalidDataClassificationException : ClassifiedException
{
    /// <summary>Construye la excepción.</summary>
    /// <param name="classification">Lo que llegó.</param>
    /// <param name="rpc">En una petición/respuesta (admite las cuatro) y no en estado.</param>
    public InvalidDataClassificationException(string? classification, bool rpc = false)
        : base(
            rpc
                ? $"la clasificación {Cita.De(classification)} no es válida: public, internal, confidential o restricted " +
                  "(01-envelope.md §3.1). El error de una petición/respuesta es siempre `internal` y no se elige."
                : $"la clasificación {Cita.De(classification)} no se admite en estado: solo public, internal o confidential. " +
                  "`restricted` (credenciales, salud, financiero: 24 h de retención) no cabe en un almacén que no caduca, y no es " +
                  "una decisión que deba tomarse por omisión (10-state.md §7).",
            "INVALID_DATACLASSIFICATION",
            null)
    {
    }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Permanent;
}

/// <summary>
/// Una entrada de estado que LLEGA con <c>dataclassification: "restricted"</c>. <b>POISON</b>: al consumir es un mensaje
/// que no debería existir. Mismo código que <see cref="InvalidDataClassificationException"/>, otra clase.
/// </summary>
public sealed class RestrictedStateEntryException : ClassifiedException
{
    /// <summary>Construye la excepción.</summary>
    public RestrictedStateEntryException()
        : base(
            "una entrada de estado no puede llevar dataclassification \"restricted\": credenciales, salud o datos financieros " +
            "(24 h de retención) no caben en un almacén que no caduca. La entrada NO se aplica (10-state.md §7).",
            "INVALID_DATACLASSIFICATION",
            null)
    {
    }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Poison;
}
