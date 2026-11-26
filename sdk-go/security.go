// Seguridad de la conexión: inbox privado, TLS, credenciales, errores de permisos y primera conexión.
// Contrato normativo: specification/06-security.md §2 y §3, 04-errors.md §5, 03-delivery.md §6.1
//
// Este fichero traduce las opciones de `ConnectOptions` a lo que entiende `nats.go`, y lo hace ANTES de
// abrir ningún socket: un valor mal formado debe fallar en `Connect()` con su nombre y no ignorarse en
// silencio (es exactamente lo que pasó con `credentials.creds` en el SDK de Node: el cliente no conocía
// la opción y la ignoraba, y el servicio conectaba SIN credenciales sin que nada lo dijera).
//
// ⚠️ Lo que se MIDIÓ de `nats.go` v1.37.0 contra nats-server 2.14.5, y por qué este fichero existe
// (PLAN.md, Apéndice A, A-190 a A-199):
//
//   - `RetryOnFailedConnect(true)` hace que `Connect()` devuelva AL INSTANTE una conexión en estado
//     RECONNECTING y sin error ante una credencial rechazada, una CA que no firmó el certificado, un host
//     fuera de los SAN, TLS exigido sin TLS en el broker y un mTLS sin certificado de cliente. Solo la
//     rechazada por credenciales acaba cerrándose, tras dos intentos y sin avisar a nadie; las demás se
//     quedan reintentando para siempre, y la causa solo se ve en `nc.LastError()`. Un servicio «arrancado
//     y mudo»: el healthcheck de proceso en verde y la primera publicación esperando su timeout.
//   - Con `RetryOnFailedConnect(false)` los mismos cinco fallos son un error en 1–3 ms, con la causa
//     (`ErrAuthorization`, `*tls.CertificateVerificationError`, `ErrSecureConnWanted`…). Y un broker caído
//     es `ErrNoServers`. Por eso el bucle de la primera conexión lo escribe este SDK (03-delivery.md §6.1).
//   - Las denegaciones de permisos NO llegan como error de la operación: el servidor descarta el mensaje y
//     manda un `-ERR 'Permissions Violation …'` aparte que `nats.go` reparte SOLO al `ErrorHandler`
//     asíncrono. Una petición a un subject denegado es un `ErrTimeout`. Ver denials.go.

package flux

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// ─── Errores ─────────────────────────────────────────────────────────────────

// baseError aporta lo común a los errores de las capacidades `security`, `state` y `rpc`: una clase y un
// código FIJOS —la aplicación no puede sustituirlos: es lo que agrupa el panel (04-errors.md §5.2)— y la
// causa encadenada. Es el mecanismo de Go para «un tipo por código»: errors.As recorre la cadena.
type baseError struct {
	code  string
	class ErrorClass
	cause error
}

// FluxClass implementa ClassifiedError.
func (e baseError) FluxClass() ErrorClass { return e.class }

// FluxCode implementa ClassifiedError.
func (e baseError) FluxCode() string { return e.code }

// Unwrap expone la causa a errors.Is y errors.As.
func (e baseError) Unwrap() error { return e.cause }

// Códigos estables de la capacidad `security` — 04-errors.md §5.
const (
	CodeNotProvisioned     = "NOT_PROVISIONED"
	CodePermissionDenied   = "PERMISSION_DENIED"
	CodeConnectionRejected = "CONNECTION_REJECTED"
	CodeInvalidCredentials = "INVALID_CREDENTIALS"
	CodeInvalidTLSOptions  = "INVALID_TLS_OPTIONS"
)

// RejectionReason es el motivo de un CONNECTION_REJECTED. Solo hay dos, porque la librería de NATS no da
// más: que el TLS falle por un certificado que no verifica, por un servidor sin TLS o por un mTLS sin
// certificado de cliente llega como el mismo error (A-24, A-30); esos matices viajan en el mensaje.
type RejectionReason string

const (
	// RejectionAuth: el broker rechazó las credenciales.
	RejectionAuth RejectionReason = "auth"
	// RejectionTLS: el certificado del servidor no verifica (CA ausente o equivocada, host fuera de los
	// SAN, caducado), se exigió TLS y el servidor no lo ofrece, o el broker exige un certificado de
	// cliente y no se presentó ninguno.
	RejectionTLS RejectionReason = "tls"
)

// ConnectionRejectedError: el broker CONTESTÓ y rechazó la primera conexión por algo que esperar no
// arregla. PERMANENT — 04-errors.md §5.3. Lo arregla, normalmente, quien opera el broker o los
// certificados; la opción estaba bien formada.
type ConnectionRejectedError struct {
	baseError
	// Reason es `auth` o `tls`.
	Reason  RejectionReason
	Servers string
	detail  string
}

func (e *ConnectionRejectedError) Error() string { return e.detail }

// InvalidCredentialsError: las opciones de credenciales están MAL FORMADAS y eso se sabe sin hablar con
// el broker. PERMANENT. Nunca contiene el valor de una credencial: el mensaje acaba en logs.
type InvalidCredentialsError struct {
	baseError
	detail string
}

func (e *InvalidCredentialsError) Error() string {
	return "credenciales inválidas en Connect(): " + e.detail
}

// InvalidTLSOptionsError: las opciones TLS están MAL FORMADAS y se sabe sin red. PERMANENT.
type InvalidTLSOptionsError struct {
	baseError
	detail string
}

func (e *InvalidTLSOptionsError) Error() string {
	return "opciones TLS inválidas en Connect(): " + e.detail
}

func newInvalidCredentials(detail string, cause error) *InvalidCredentialsError {
	return &InvalidCredentialsError{baseError{CodeInvalidCredentials, ClassPermanent, cause}, detail}
}

func newInvalidTLS(detail string) *InvalidTLSOptionsError {
	return &InvalidTLSOptionsError{baseError{CodeInvalidTLSOptions, ClassPermanent, nil}, detail}
}

// PermissionDeniedError: el broker denegó una operación por permisos (`Permissions Violation`).
//
// PERMANENT: reintentar una denegación da la misma denegación, como un HTTP 403 — 04-errors.md §1.2.
// Un handler que la provoque va a la DLQ sin atascar la cola.
type PermissionDeniedError struct {
	baseError
	Service string
	// Operation es "publish" o "subscribe".
	Operation string
	// Subject es el subject denegado, COMPLETO y sin normalizar (04-errors.md §5.2).
	Subject string
	hint    string
}

func (e *PermissionDeniedError) Error() string {
	accion := "publicar en"
	if e.Operation == "subscribe" {
		accion = "suscribirse a"
	}
	explicacion := "Ver 06-security.md §3."
	switch {
	case e.hint != "":
		explicacion = e.hint
	case strings.HasPrefix(e.Subject, "$JS."):
		explicacion = "Es un permiso de JetStream, y se concede exacto por durable: una ACL generada " +
			"antes de la v1.1 (que solo habla de subjects de dominio) no los incluye. " +
			"Ver «Seguridad: permisos, inbox, TLS y credenciales» en sdk-go/README.md (06-security.md §3)."
	}
	return fmt.Sprintf("el broker denegó %s %q a %q: su ACL no concede ese permiso. %s",
		accion, e.Subject, e.Service, explicacion)
}

func newPermissionDenied(service, operation, subject, hint string, cause error) *PermissionDeniedError {
	return &PermissionDeniedError{
		baseError: baseError{CodePermissionDenied, ClassPermanent, cause},
		Service:   service, Operation: operation, Subject: subject, hint: hint,
	}
}

// ResourceKind es lo que falta cuando el error es NOT_PROVISIONED.
type ResourceKind string

const (
	ResourceStream   ResourceKind = "stream"
	ResourceConsumer ResourceKind = "consumer"
)

// NotProvisionedError: falta un stream o un consumidor y este servicio no tiene permiso para crearlo.
//
// En producción los streams y los consumidores se aprovisionan FUERA del SDK (`flux provision`, con
// credenciales de operación): dejar que cualquier servicio cree el stream compartido de un dominio es una
// carrera esperando a ocurrir, y su permiso de creación es justo el que la ACL exacta le quita. Sin este
// error lo que se vería es un timeout, que no dice nada. PERMANENT: reintentar no hace aparecer el recurso.
type NotProvisionedError struct {
	baseError
	Resource ResourceKind
	Name     string
	// Stream es el stream del consumidor, si Resource es ResourceConsumer.
	Stream        string
	Service       string
	DeniedSubject string
	detail        string
}

func (e *NotProvisionedError) Error() string {
	if e.detail != "" {
		return e.detail
	}
	que := fmt.Sprintf("el stream %q", e.Name)
	if e.Resource == ResourceConsumer {
		que = fmt.Sprintf("el consumidor %q del stream %q", e.Name, e.Stream)
	}
	return fmt.Sprintf("%s no existe y %q no tiene permiso para crearlo (el broker denegó publicar en %q). "+
		"En producción se aprovisiona con `flux provision <services.json>`, usando credenciales de "+
		"operación, no las del servicio (06-security.md §3).", que, e.Service, e.DeniedSubject)
}

func newNotProvisioned(kind ResourceKind, name, stream, service, denied, detail string, cause error) *NotProvisionedError {
	return &NotProvisionedError{
		baseError: baseError{CodeNotProvisioned, ClassPermanent, cause},
		Resource:  kind, Name: name, Stream: stream, Service: service, DeniedSubject: denied, detail: detail,
	}
}

// ─── Inbox privado ───────────────────────────────────────────────────────────

// inboxPrefixFor devuelve `_INBOX.<servicio>`. Las respuestas a las peticiones de JetStream —y las
// entregas de un pull consumer— viajan por el inbox. Con el `_INBOX.>` común, un servicio que se suscribe
// ahí lee los eventos de dominios que no tiene concedidos (PLAN.md, H2).
//
// No es una opción: lo deriva el SDK del nombre del servicio. Si fuera configurable, volver al `_INBOX.>`
// común sería un valor por defecto olvidado.
func inboxPrefixFor(service string) (string, error) {
	if !ServicePattern.MatchString(service) {
		return "", &InvalidServiceNameError{Service: service}
	}
	return "_INBOX." + service, nil
}

// ─── Credenciales → opciones de nats.go ──────────────────────────────────────

// Credentials son las credenciales de NATS. NUNCA versionadas — 06-security.md §2. Se admite UN solo método:
//
//	| Método                              | Campo                      |
//	| .creds (JWT + nkey), el recomendado | CredsFile o CredsContent   |
//	| nkey                                | NKeySeed                   |
//	| token                               | Token                      |
//	| usuario y contraseña                | User y Password            |
//
// `CredsFile` es una RUTA y se relee en cada (re)conexión: rotar no exige reiniciar.
//
// ⚠️ El cero de un string en Go es «no lo he puesto» y no se distingue de «lo puse vacío». Con una variable
// de entorno sin definir (`Credentials{CredsFile: os.Getenv("NATS_CREDS")}`) el campo llega vacío y la
// conexión se haría SIN credenciales. Para que eso falle en `Connect()` con INVALID_CREDENTIALS, usa los
// constructores (`CredentialsFromFile`, `CredentialsFromNKeySeed`…): recuerdan qué método se pidió.
type Credentials struct {
	CredsFile    string
	CredsContent string
	NKeySeed     string
	Token        string
	User         string
	Password     string

	// pedidos son los métodos que un constructor declaró explícitamente, aunque su valor llegue vacío.
	pedidos []string
}

// CredentialsFromFile pide autenticar con un fichero `.creds`.
func CredentialsFromFile(path string) Credentials {
	return Credentials{CredsFile: path, pedidos: []string{"CredsFile"}}
}

// CredentialsFromContent pide autenticar con el contenido de un `.creds` (de un gestor de secretos).
func CredentialsFromContent(content string) Credentials {
	return Credentials{CredsContent: content, pedidos: []string{"CredsContent"}}
}

// CredentialsFromNKeySeed pide autenticar con la semilla de una nkey de USUARIO (`SU…`).
func CredentialsFromNKeySeed(seed string) Credentials {
	return Credentials{NKeySeed: seed, pedidos: []string{"NKeySeed"}}
}

// CredentialsFromToken pide autenticar con un token.
func CredentialsFromToken(token string) Credentials {
	return Credentials{Token: token, pedidos: []string{"Token"}}
}

// CredentialsFromUserPass pide autenticar con usuario y contraseña.
func CredentialsFromUserPass(user, password string) Credentials {
	return Credentials{User: user, Password: password, pedidos: []string{"User", "Password"}}
}

// metodos devuelve los métodos pedidos (campos dados, o vacíos que un constructor declaró).
func (c Credentials) metodos() (dados []string, vacios []string) {
	valores := map[string]string{
		"CredsFile": c.CredsFile, "CredsContent": c.CredsContent, "NKeySeed": c.NKeySeed,
		"Token": c.Token, "User": c.User, "Password": c.Password,
	}
	for _, campo := range []string{"CredsFile", "CredsContent", "NKeySeed", "Token", "User", "Password"} {
		pedido := false
		for _, p := range c.pedidos {
			pedido = pedido || p == campo
		}
		switch {
		case strings.TrimSpace(valores[campo]) != "":
			dados = append(dados, campo)
		case pedido:
			vacios = append(vacios, campo)
		}
	}
	return dados, vacios
}

// authOptions traduce `Credentials` a opciones de nats.go, o nil si no hay. Falla en alto ante cualquier
// valor que el cliente ignoraría o que no podrá usar, ANTES de abrir ningún socket.
func authOptions(c Credentials) ([]nats.Option, error) {
	dados, vacios := c.metodos()
	if len(vacios) > 0 {
		return nil, newInvalidCredentials(fmt.Sprintf(
			"`%s` está vacío (casi siempre, una variable de entorno sin definir). Pasa un valor, o quita el "+
				"campo para conectar sin credenciales", vacios[0]), nil)
	}
	if len(dados) == 0 {
		return nil, nil
	}
	if (c.User != "") != (c.Password != "") {
		return nil, newInvalidCredentials("`User` y `Password` van juntos", nil)
	}
	metodos := map[string]bool{}
	for _, d := range dados {
		switch d {
		case "User", "Password":
			metodos["User+Password"] = true
		default:
			metodos[d] = true
		}
	}
	if len(metodos) > 1 {
		// Combinarlos es ambiguo: el servidor aceptaría uno y el otro quedaría como ruido.
		nombres := make([]string, 0, len(metodos))
		for _, campo := range []string{"CredsFile", "CredsContent", "NKeySeed", "Token", "User+Password"} {
			if metodos[campo] {
				nombres = append(nombres, campo)
			}
		}
		return nil, newInvalidCredentials("elige UN método y no "+strings.Join(nombres, " + "), nil)
	}

	switch {
	case c.CredsFile != "":
		// Se comprueba ahora que el fichero es legible y trae un JWT y una semilla de usuario, y se le
		// pasa a nats.go la RUTA: la relee en cada (re)conexión, y la rotación no exige reiniciar.
		contenido, err := os.ReadFile(c.CredsFile)
		if err != nil {
			return nil, newInvalidCredentials(fmt.Sprintf(
				"`CredsFile` apunta a %q, que no se puede leer (%v)", c.CredsFile, errSinRuta(err)), err)
		}
		if err := comprobarCreds(contenido); err != nil {
			return nil, newInvalidCredentials(fmt.Sprintf("`CredsFile` (%q) no es utilizable: %v", c.CredsFile, err), err)
		}
		return []nats.Option{nats.UserCredentials(c.CredsFile)}, nil
	case c.CredsContent != "":
		contenido := []byte(c.CredsContent)
		if err := comprobarCreds(contenido); err != nil {
			return nil, newInvalidCredentials(fmt.Sprintf("`CredsContent` no es utilizable: %v", err), err)
		}
		jwt, _ := nkeys.ParseDecoratedJWT(contenido)
		kp, _ := nkeys.ParseDecoratedUserNKey(contenido)
		seed, _ := kp.Seed()
		return []nats.Option{nats.UserJWTAndSeed(jwt, string(seed))}, nil
	case c.NKeySeed != "":
		if !strings.HasPrefix(c.NKeySeed, "SU") {
			return nil, newInvalidCredentials("`NKeySeed` debe ser la semilla de una nkey de USUARIO (empieza por «SU»)", nil)
		}
		kp, err := nkeys.FromSeed([]byte(c.NKeySeed))
		if err != nil {
			return nil, newInvalidCredentials("`NKeySeed` no es una semilla nkey válida", err)
		}
		pub, err := kp.PublicKey()
		if err != nil || !nkeys.IsValidPublicUserKey(pub) {
			return nil, newInvalidCredentials("`NKeySeed` no es la semilla de una nkey de usuario", err)
		}
		seed := []byte(c.NKeySeed)
		return []nats.Option{nats.Nkey(pub, func(nonce []byte) ([]byte, error) {
			par, err := nkeys.FromSeed(seed)
			if err != nil {
				return nil, err
			}
			defer par.Wipe()
			return par.Sign(nonce)
		})}, nil
	case c.Token != "":
		return []nats.Option{nats.Token(c.Token)}, nil
	default:
		return []nats.Option{nats.UserInfo(c.User, c.Password)}, nil
	}
}

// comprobarCreds exige lo que un `.creds` necesita: un JWT y una semilla de USUARIO.
func comprobarCreds(contenido []byte) error {
	jwt, err := nkeys.ParseDecoratedJWT(contenido)
	if err != nil || strings.TrimSpace(jwt) == "" {
		return errors.New("no contiene un JWT de usuario")
	}
	if _, err := nkeys.ParseDecoratedUserNKey(contenido); err != nil {
		return fmt.Errorf("no contiene una semilla de usuario (`SU…`): %w", err)
	}
	return nil
}

// errSinRuta quita del error de os la ruta, que ya va en el mensaje.
func errSinRuta(err error) string {
	var pe *os.PathError
	if errors.As(err, &pe) {
		return pe.Err.Error()
	}
	return err.Error()
}

// ─── TLS → opciones de nats.go ───────────────────────────────────────────────

// TLSOptions es el TLS del listener — 06-security.md §2: es obligatorio en producción. Los ficheros son
// RUTAS. `ConnectOptions.TLS == nil` no pide TLS (aunque el servidor lo exija, `nats.go` sube a TLS con las
// CA del sistema); `&TLSOptions{}` EXIGE TLS y verifica contra las CA del sistema.
//
// Una CA propia es lo normal en un bus interno: un SDK que no pudiera fijar la suya obligaría a desactivar
// la verificación, que es la forma más corriente de acabar con TLS «obligatorio» y sin protección. Aquí no
// hay forma de desactivarla.
type TLSOptions struct {
	// CAFile es la CA que firma el certificado del servidor NATS, si no es una CA pública.
	CAFile string
	// CertFile es el certificado de cliente (mTLS). Exige KeyFile.
	CertFile string
	// KeyFile es la clave del certificado de cliente. Exige CertFile.
	KeyFile string
}

// tlsOptions traduce `TLSOptions` a opciones de nats.go, o nil si no se pidió TLS.
func tlsOptions(t *TLSOptions) ([]nats.Option, error) {
	if t == nil {
		return nil, nil
	}
	if (t.CertFile == "") != (t.KeyFile == "") {
		return nil, newInvalidTLS("`CertFile` y `KeyFile` van juntos (certificado de cliente)")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if t.CAFile != "" {
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, newInvalidTLS(fmt.Sprintf("`CAFile` apunta a %q, que no se puede leer (%v)", t.CAFile, errSinRuta(err)))
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, newInvalidTLS(fmt.Sprintf("`CAFile` (%q) no contiene ningún certificado en formato PEM", t.CAFile))
		}
		cfg.RootCAs = pool
	}
	opts := []nats.Option{nats.Secure(cfg)}
	if t.CertFile != "" {
		// Se comprueba ahora que el par se lee y casa; nats.go lo vuelve a leer en cada handshake.
		if _, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile); err != nil {
			return nil, newInvalidTLS(fmt.Sprintf(
				"`CertFile` (%q) y `KeyFile` (%q) no son un par certificado/clave PEM utilizable (%v)",
				t.CertFile, t.KeyFile, errSinRuta(err)))
		}
		opts = append(opts, nats.ClientCert(t.CertFile, t.KeyFile))
	}
	return opts, nil
}

// ─── La primera conexión ─────────────────────────────────────────────────────

// permanentConnectError reconoce un fallo de la PRIMERA conexión que esperar no arregla, y lo explica;
// o devuelve nil si es transitorio (servidor caído, DNS, timeout) y merece otro intento.
//
// Por qué existe: `RetryOnFailedConnect` de la librería devuelve una conexión «viva» sin error ante estos
// cinco fallos (medido, ver la cabecera). Aquí el bucle es nuestro para poder distinguir.
func permanentConnectError(err error, servers string) *ConnectionRejectedError {
	rechazo := func(motivo RejectionReason, detalle string) *ConnectionRejectedError {
		return &ConnectionRejectedError{
			baseError: baseError{CodeConnectionRejected, ClassPermanent, err},
			Reason:    motivo, Servers: servers, detail: detalle,
		}
	}
	switch {
	case errors.Is(err, nats.ErrAuthorization), errors.Is(err, nats.ErrAuthExpired),
		errors.Is(err, nats.ErrAuthRevoked), errors.Is(err, nats.ErrAccountAuthExpired):
		return rechazo(RejectionAuth, fmt.Sprintf(
			"el broker %q rechazó las credenciales (%v). Reintentar no lo arregla: revisa `Credentials` y que "+
				"el usuario exista en la account del servicio", servers, err))
	case errors.Is(err, nats.ErrSecureConnWanted):
		// Se pidió TLS y el servidor no lo ofrece. La librería NO degrada a texto plano (bien).
		return rechazo(RejectionTLS, fmt.Sprintf(
			"se pidió `TLS` pero el servidor NATS %q no ofrece TLS. Reintentar no lo arregla, y el SDK no baja a "+
				"texto plano: habilita TLS en el listener (06-security.md §2) o quita `TLS`", servers))
	}
	var verificacion *tls.CertificateVerificationError
	var desconocida x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalido x509.CertificateInvalidError
	if errors.As(err, &verificacion) || errors.As(err, &desconocida) || errors.As(err, &hostname) || errors.As(err, &invalido) {
		return rechazo(RejectionTLS, fmt.Sprintf(
			"no se pudo verificar el certificado del servidor NATS %q (%v). Reintentar no lo arregla. Si el "+
				"servidor usa una CA propia, indícala en `TLS.CAFile`; si el certificado no cubre el host al que te "+
				"conectas, revisa sus SAN", servers, err))
	}
	if alerta := alertaTLSDelServidor(err); alerta != "" {
		// El servidor rechazó NUESTRO certificado de cliente, o no presentamos ninguno y lo exige (mTLS).
		return rechazo(RejectionTLS, fmt.Sprintf(
			"el servidor NATS %q cortó el handshake TLS (%s). Si el broker exige certificado de cliente (mTLS), "+
				"indica `TLS.CertFile` y `TLS.KeyFile`; si ya los pasas, revisa que su CA los reconozca. Reintentar no lo arregla",
			servers, alerta))
	}
	return nil
}

// alertaTLSDelServidor devuelve la alerta TLS que el servidor envió y que reintentar no arregla (un
// certificado de cliente exigido, o rechazado), o "". La librería de TLS no exporta el tipo de la alerta
// recibida: se reconoce por su texto, que es el único sitio donde hacerlo está justificado.
func alertaTLSDelServidor(err error) string {
	var op *net.OpError
	if !errors.As(err, &op) || op.Op != "remote error" {
		return ""
	}
	texto := op.Err.Error()
	for _, alerta := range []string{
		"certificate required", "bad certificate", "unknown certificate authority",
		"certificate unknown", "certificate expired", "certificate revoked",
	} {
		if strings.Contains(texto, alerta) {
			return "tls: " + alerta
		}
	}
	return ""
}

// esTransitorio: ¿merece otro intento la primera conexión? Solo lo que una espera arregla: un broker que
// aún no ha arrancado (conexión rechazada, sin ruta, DNS, timeout, corte). Lo desconocido NO se reintenta:
// un bucle infinito sobre un error que no cambia es justo el «servicio mudo» que esta capa existe para evitar.
func esTransitorio(err error) bool {
	if errors.Is(err, nats.ErrNoServers) || errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne)
}
