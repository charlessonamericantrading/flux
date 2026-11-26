// Seguridad de la conexión: TLS, credenciales y los cinco errores de la capacidad `security`.
// Contrato normativo: specification/06-security.md §2 y §3, specification/04-errors.md §5
//
// Este fichero es la parte PÚBLICA: las opciones y las excepciones. La parte que habla con
// NATS.Net (traducir opciones, reconocer lo que devuelve la librería) vive en
// ConnectSecurity.cs y Denials.cs, que son internal: ningún tipo de NATS sale de aquí.

namespace Flux;

/// <summary>
/// TLS del listener — 06-security.md §2: es obligatorio en producción.
/// </summary>
/// <remarks>
/// Pasar un <see cref="TlsOptions"/> (aunque sea vacío) <b>exige</b> TLS: si el servidor no lo
/// ofrece, <c>ConnectAsync</c> falla con <see cref="ConnectionRejectedException"/> (motivo
/// <see cref="RejectionReason.Tls"/>) y NO baja a texto plano. Sin <see cref="TlsOptions"/> la
/// librería de NATS usa TLS si el servidor lo exige y lo ofrece, y texto plano si no — eso es
/// bueno para un compose de desarrollo y malo para producción, y por eso existe esta opción.
/// <para>
/// Los ficheros son RUTAS en PEM. Se comprueban ANTES de abrir ningún socket: una ruta que no
/// existe, un fichero que no es PEM o un certificado de cliente sin su clave son
/// <see cref="InvalidTlsOptionsException"/>, no un reintento infinito.
/// </para>
/// </remarks>
public sealed record TlsOptions
{
    /// <summary>
    /// CA que firma el certificado del servidor NATS, si no es una CA pública. <b>Sustituye</b>
    /// a las del sistema: con una CA propia, solo esa CA es de fiar.
    /// </summary>
    public string? CaFile { get; init; }

    /// <summary>Certificado de cliente (mTLS), en PEM. Exige <see cref="KeyFile"/>.</summary>
    public string? CertFile { get; init; }

    /// <summary>Clave privada del certificado de cliente, en PEM. Exige <see cref="CertFile"/>.</summary>
    public string? KeyFile { get; init; }
}

/// <summary>Por qué el broker rechazó la primera conexión — 04-errors.md §5.3.</summary>
public enum RejectionReason
{
    /// <summary>El broker rechazó las credenciales (<c>auth</c>).</summary>
    Auth,

    /// <summary>
    /// El certificado del servidor no verifica (CA ausente o equivocada, host fuera de los
    /// SAN, caducado), se exigió TLS y el servidor no lo ofrece, o el broker exige certificado
    /// de cliente y no se presentó ninguno (<c>tls</c>).
    /// </summary>
    Tls,
}

/// <summary>
/// El broker <b>contestó</b> y rechazó la primera conexión por algo que esperar no arregla
/// — 04-errors.md §5.3. Código <c>CONNECTION_REJECTED</c>, PERMANENT.
/// </summary>
/// <remarks>
/// Lo distingue de <see cref="InvalidCredentialsException"/> e
/// <see cref="InvalidTlsOptionsException"/> que hubo conversación: la opción estaba bien
/// formada y no es lo que el broker espera. Una credencial bien formada pero equivocada es
/// esto (<c>auth</c>), nunca <c>INVALID_CREDENTIALS</c>; un certificado que el broker no acepta
/// es esto (<c>tls</c>), nunca <c>INVALID_TLS_OPTIONS</c>. Un broker que aún no ha arrancado
/// no es ninguno de los tres: se espera.
/// </remarks>
public sealed class ConnectionRejectedException : ClassifiedException
{
    /// <summary>El código estable.</summary>
    public const string ErrorCode = "CONNECTION_REJECTED";

    /// <summary>Construye la excepción.</summary>
    /// <param name="reason">Motivo: <c>auth</c> o <c>tls</c>.</param>
    /// <param name="message">Qué pasó y qué revisar. Nunca lleva una credencial.</param>
    /// <param name="innerException">El error de la librería de NATS.</param>
    public ConnectionRejectedException(RejectionReason reason, string message, Exception? innerException = null)
        : base(message, "CONNECTION_REJECTED", innerException)
    {
        Reason = reason;
    }

    /// <summary>Motivo del rechazo.</summary>
    public RejectionReason Reason { get; }

    /// <summary>El motivo como lo escribe el protocolo: <c>auth</c> o <c>tls</c>.</summary>
    public string ReasonCode => Reason == RejectionReason.Auth ? "auth" : "tls";

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Permanent;
}

/// <summary>
/// Las opciones de credenciales están mal formadas, y se sabe <b>sin hablar con el broker</b>
/// — 04-errors.md §5.3. Código <c>INVALID_CREDENTIALS</c>, PERMANENT. Se lanza antes de
/// abrir ningún socket.
/// </summary>
/// <remarks>El mensaje nunca lleva el valor de una credencial: acaba en logs.</remarks>
public sealed class InvalidCredentialsException : ClassifiedException
{
    /// <summary>Construye la excepción.</summary>
    /// <param name="reason">Qué opción está mal y por qué. Una ruta sí; el valor, nunca.</param>
    /// <param name="innerException">La causa, si la hay.</param>
    public InvalidCredentialsException(string reason, Exception? innerException = null)
        : base(
            "credenciales inválidas en ConnectAsync: " + reason +
            ". Reintentar no lo arregla: revisa lo que pasas en `Credentials` (06-security.md §2)",
            "INVALID_CREDENTIALS",
            innerException)
    {
    }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Permanent;
}

/// <summary>
/// Las opciones TLS están mal formadas, y se sabe <b>sin red</b> — 04-errors.md §5.3. Código
/// <c>INVALID_TLS_OPTIONS</c>, PERMANENT. Se lanza antes de abrir ningún socket.
/// </summary>
public sealed class InvalidTlsOptionsException : ClassifiedException
{
    /// <summary>Construye la excepción.</summary>
    /// <param name="reason">Qué opción está mal y por qué. Una ruta sí.</param>
    /// <param name="innerException">La causa, si la hay.</param>
    public InvalidTlsOptionsException(string reason, Exception? innerException = null)
        : base(
            "opciones TLS inválidas en ConnectAsync: " + reason +
            ". Reintentar no lo arregla: revisa lo que pasas en `Tls` (06-security.md §2)",
            "INVALID_TLS_OPTIONS",
            innerException)
    {
    }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Permanent;
}

/// <summary>Qué intentaba hacer el SDK cuando el broker denegó.</summary>
public enum DeniedOperation
{
    /// <summary>Publicar (también una petición de la API de JetStream, que es una publicación).</summary>
    Publish,

    /// <summary>Suscribirse.</summary>
    Subscribe,
}

/// <summary>
/// El broker denegó una operación por permisos (<c>Permissions Violation</c>) — 04-errors.md §5.
/// Código <c>PERMISSION_DENIED</c>, PERMANENT.
/// </summary>
/// <remarks>
/// Como un HTTP 403: reintentar una denegación da la misma denegación. Un handler que la
/// provoque va a la DLQ en la primera entrega, sin atascar la cola.
/// <para>
/// El broker NO contesta a lo que deniega: descarta el mensaje y avisa aparte con un
/// <c>-ERR</c>. NATS.Net 2.5.16 no entrega ese aviso (solo lo escribe en su <c>ILogger</c>), así
/// que sin este SDK quien llama vería un timeout de 5 a 30 s sin una palabra sobre permisos
/// (medido contra nats-server 2.14.5, A-210).
/// </para>
/// </remarks>
public sealed class PermissionDeniedException : ClassifiedException
{
    /// <summary>Construye la excepción.</summary>
    /// <param name="service">El servicio al que se denegó.</param>
    /// <param name="operation">Publicar o suscribirse.</param>
    /// <param name="deniedSubject">El subject denegado, completo y sin normalizar.</param>
    /// <param name="attempt">Lo que el SDK intentaba, en términos de flux («abrir el stream `EVT_PEDIDOS`»).</param>
    /// <param name="hint">Cómo se arregla ESTA denegación, si el SDK lo sabe.</param>
    /// <param name="innerException">La causa, si la hay.</param>
    public PermissionDeniedException(
        string service,
        DeniedOperation operation,
        string deniedSubject,
        string? attempt = null,
        string? hint = null,
        Exception? innerException = null)
        : base(
            "el broker denegó " + (operation == DeniedOperation.Publish ? "publicar en" : "suscribirse a") +
            " \"" + deniedSubject + "\" a \"" + service + "\"" +
            (attempt is null ? string.Empty : " (al " + attempt + ")") +
            ": su ACL no concede ese permiso. " +
            (hint ?? DefaultHint(deniedSubject)),
            "PERMISSION_DENIED",
            innerException)
    {
        Service = service;
        Operation = operation;
        DeniedSubject = deniedSubject;
    }

    /// <summary>El servicio al que se denegó.</summary>
    public string Service { get; }

    /// <summary>Publicar o suscribirse.</summary>
    public DeniedOperation Operation { get; }

    /// <summary>El subject denegado, tal como lo comunicó el broker.</summary>
    public string DeniedSubject { get; }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Permanent;

    private static string DefaultHint(string subject) =>
        subject.StartsWith("$JS.", StringComparison.Ordinal)
            ? "Es un permiso de JetStream, y se concede exacto por durable: una ACL generada antes " +
              "de la v1.1 (que solo habla de subjects de dominio) no los incluye. Regenera las ACLs " +
              "con scripts/gen-acl.mjs (06-security.md §3)."
            : "Ver 06-security.md §3.";
}

/// <summary>Qué recurso de JetStream falta.</summary>
public enum ResourceKind
{
    /// <summary>Un stream.</summary>
    Stream,

    /// <summary>Un consumidor durable.</summary>
    Consumer,
}

/// <summary>
/// Falta un stream o un consumidor y este servicio no tiene permiso para crearlo — 04-errors.md
/// §5. Código <c>NOT_PROVISIONED</c>, PERMANENT.
/// </summary>
/// <remarks>
/// En producción los streams y los consumidores se aprovisionan FUERA del SDK (06-security.md
/// §3.6): dejar que cualquier servicio cree el stream compartido de un dominio es una carrera
/// esperando a ocurrir. La frontera con <see cref="PermissionDeniedException"/> es si se sabe
/// que el recurso falta: crear → denegado <b>y</b> consultar → no existe.
/// </remarks>
public sealed class NotProvisionedException : ClassifiedException
{
    /// <summary>Construye la excepción.</summary>
    /// <param name="kind">Stream o consumidor.</param>
    /// <param name="name">Nombre del recurso.</param>
    /// <param name="stream">Para un consumidor, su stream.</param>
    /// <param name="service">El servicio al que se denegó.</param>
    /// <param name="deniedSubject">El subject denegado (el de la creación).</param>
    /// <param name="innerException">La causa, si la hay.</param>
    /// <param name="detail">Sustituye al texto entero del mensaje (lo usa el estado: la señal es que nadie contesta a la lectura directa).</param>
    public NotProvisionedException(
        ResourceKind kind,
        string name,
        string? stream,
        string service,
        string deniedSubject,
        Exception? innerException = null,
        string? detail = null)
        : base(
            // `detail` sustituye al texto entero. Lo usa el estado (04-errors.md §6): ahí la señal no es un `CREATE` denegado sino
            // que NADIE contesta a la lectura directa —un stream que no existe y uno sin `allow_direct` se ven igual (A-32)—, y el
            // mensaje DEBE nombrar las dos causas, no una denegación que no ha habido.
            detail ??
            (kind == ResourceKind.Stream
                ? "el stream \"" + name + "\""
                : "el consumidor \"" + name + "\" del stream \"" + stream + "\"") +
            " no existe y \"" + service + "\" no tiene permiso para crearlo (el broker denegó publicar en \"" +
            deniedSubject + "\"). En producción se aprovisiona con `flux provision <services.json>`, " +
            "usando credenciales de operación, no las del servicio (06-security.md §3.6).",
            "NOT_PROVISIONED",
            innerException)
    {
        Kind = kind;
        Name = name;
        Stream = stream;
        Service = service;
        DeniedSubject = deniedSubject;
    }

    /// <summary>Stream o consumidor.</summary>
    public ResourceKind Kind { get; }

    /// <summary>Nombre del recurso que falta.</summary>
    public string Name { get; }

    /// <summary>Para un consumidor, el stream al que pertenece.</summary>
    public string? Stream { get; }

    /// <summary>El servicio al que se denegó.</summary>
    public string Service { get; }

    /// <summary>El subject cuya publicación denegó el broker.</summary>
    public string DeniedSubject { get; }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Permanent;
}
