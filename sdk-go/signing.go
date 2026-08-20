// Firma de eventos con Ed25519.
// Contrato normativo: specification/07-signing.md
//
// Extensión OPCIONAL de v1: un evento sin firma sigue siendo válido y el default de la
// verificación es `off`. Traslada la autenticidad DEL CANAL AL EVENTO — un evento
// firmado sigue siendo verificable dentro de un fichero, un backup o un correo, donde
// ya no hay ninguna ACL que lo respalde.
//
// Usa crypto/ed25519 de la biblioteca estándar: cero dependencias nuevas. Es una de las
// razones por las que el protocolo fija Ed25519 y no negocia algoritmo (07-signing.md §3).

package flux

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"log/slog"
)

// VerificationMode es la política ante un evento sin firma o con firma inválida
// — 07-signing.md §7.
type VerificationMode string

const (
	// VerifyOff no mira la firma. Es el DEFAULT: la extensión es opcional.
	VerifyOff VerificationMode = "off"

	// VerifyWarn registra y acepta.
	//
	// Existe porque adoptar la firma en un ecosistema en marcha exige un periodo en el
	// que unos productores firman y otros no. Pasar directo a VerifyRequire convierte en
	// POISON todo evento de un servicio aún no migrado.
	VerifyWarn VerificationMode = "warn"

	// VerifyRequire trata como POISON tanto la ausencia de firma como una firma que no
	// verifica.
	VerifyRequire VerificationMode = "require"
)

// SigningOptions configura la extensión de firma.
type SigningOptions struct {
	// PrivateKeyPEM es la clave privada Ed25519 en PEM (PKCS#8) para firmar al
	// publicar. Vacía = solo verificar. NUNCA versionada — 06-security.md §2.
	PrivateKeyPEM string

	// KeyID identifica la clave pública, formato `<servicio>-<n>`. Obligatorio si se
	// firma: sin él, un verificador no sabe qué clave usar.
	KeyID string

	// PublicKeys son las claves conocidas: signkeyid → PEM (SPKI).
	//
	// Incluye aquí las claves RETIRADAS mientras exista algún evento firmado con ellas
	// (mínimo 90 días, la retención de la DLQ). Retirar una clave impide EMITIR con
	// ella, no VERIFICAR lo ya emitido — 07-signing.md §6.
	PublicKeys map[string]string

	// Verify es la política. El cero del tipo es "" y se interpreta como VerifyOff, que
	// es además el default del protocolo.
	Verify VerificationMode
}

// SigningKeyError señala una clave ausente, mal formada o de otro algoritmo.
//
// Es un fallo de configuración local, no un mensaje ajeno corrupto: por eso no es
// PoisonError. Rompe el arranque del servicio, que es donde debe romper.
type SigningKeyError struct {
	Message string
	Cause   error
}

func (e *SigningKeyError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

func (e *SigningKeyError) Unwrap() error { return e.Cause }

// ─── Payload firmado ─────────────────────────────────────────────────────────

// signablePayload devuelve los bytes canónicos que se firman: el evento SIN el atributo
// `signature` y SIN las extensiones dlq* — 07-signing.md §5.
//
// No hay una canonicalización aparte para firmar: es el mismo Serialize que usa el
// productor, y funciona porque 01-envelope.md §1.1 (UTF-8 literal), §2.2 (3 decimales en
// `time`) y §6 (orden de claves) fijan una única representación en bytes. Una
// canonicalización propia sería una segunda implementación del mismo formato, y las dos
// divergirían en la primera esquina.
func signablePayload(e Event) ([]byte, error) {
	e = StripDLQExtensions(e)
	e.Signature = ""
	return Serialize(e)
}

// ─── Firmar ──────────────────────────────────────────────────────────────────

// Signer firma eventos con una clave privada Ed25519.
type Signer struct {
	key   ed25519.PrivateKey
	keyID string
}

// KeyID es el `signkeyid` que este firmante estampa.
func (s *Signer) KeyID() string { return s.keyID }

// Sign devuelve una COPIA del evento con signkeyid y signature, ambos antes de `data`.
//
// Event se pasa y devuelve por valor, así que el evento original nunca se muta.
func (s *Signer) Sign(e Event) (Event, error) {
	// signkeyid va DENTRO de lo firmado: si quedara fuera, un atacante podría cambiarlo
	// para que la verificación buscara otra clave — 07-signing.md §5.
	e.SignKeyID = s.keyID
	e.Signature = ""

	payload, err := signablePayload(e)
	if err != nil {
		return Event{}, err
	}
	// base64url SIN padding — 07-signing.md §4.
	e.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(s.key, payload))
	return e, nil
}

// NewSigner construye el firmante. Devuelve (nil, nil) si no hay clave privada: firmar
// es opcional y un servicio que solo verifica no tiene por qué configurar una.
func NewSigner(opts SigningOptions) (*Signer, error) {
	if opts.PrivateKeyPEM == "" {
		return nil, nil
	}
	if opts.KeyID == "" {
		return nil, &SigningKeyError{Message: "SigningOptions.PrivateKeyPEM requiere KeyID: " +
			"sin él, un verificador no sabe qué clave pública usar (07-signing.md §4)"}
	}

	block, _ := pem.Decode([]byte(opts.PrivateKeyPEM))
	if block == nil {
		return nil, &SigningKeyError{Message: "la clave privada no es un PEM válido; " +
			"se espera PKCS#8 (-----BEGIN PRIVATE KEY-----)"}
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, &SigningKeyError{Message: "clave privada inválida", Cause: err}
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return nil, &SigningKeyError{Message: fmt.Sprintf(
			"la clave es %T, se esperaba ed25519.PrivateKey. El protocolo no negocia "+
				"algoritmo a propósito: los formatos con algoritmo negociable acumulan una "+
				"familia de vulnerabilidades que solo existe porque hay algo que negociar "+
				"(07-signing.md §3)", parsed)}
	}
	return &Signer{key: key, keyID: opts.KeyID}, nil
}

// ─── Verificar ───────────────────────────────────────────────────────────────

// Verifier aplica la política de verificación de 07-signing.md §7.
type Verifier struct {
	keys   map[string]ed25519.PublicKey
	mode   VerificationMode
	logger *slog.Logger
}

// Mode es la política configurada.
func (v *Verifier) Mode() VerificationMode { return v.mode }

// Check devuelve un *PoisonError en modo require; en modo warn registra y devuelve nil.
func (v *Verifier) Check(e Event) error {
	if e.Signature == "" {
		return v.fail("MISSING_SIGNATURE", fmt.Sprintf("evento %s sin firma", e.ID))
	}

	key, ok := v.keys[e.SignKeyID]
	if !ok {
		// Una clave desconocida NO es lo mismo que una retirada: si el id no está en el
		// mapa, el operador olvidó conservarla o el evento viene de fuera.
		return v.fail("UNKNOWN_SIGNING_KEY", fmt.Sprintf(
			"evento %s firmado con signkeyid=%q, que no está entre las claves conocidas. "+
				"¿Se retiró sin conservar la pública?", e.ID, e.SignKeyID))
	}

	sig, err := base64.RawURLEncoding.DecodeString(e.Signature)
	if err != nil {
		// Un base64 corrupto y una firma que no verifica son el mismo hecho para el
		// operador —esos bytes no los firmó esa clave— y el código debe ser estable.
		return v.fail("INVALID_SIGNATURE", fmt.Sprintf(
			"la firma del evento %s no es base64url válido", e.ID))
	}
	payload, err := signablePayload(e)
	if err != nil {
		return v.fail("INVALID_SIGNATURE", fmt.Sprintf(
			"el evento %s no se pudo reserializar para verificar su firma", e.ID))
	}
	if !ed25519.Verify(key, payload, sig) {
		return v.fail("INVALID_SIGNATURE", fmt.Sprintf("la firma del evento %s no verifica", e.ID))
	}
	return nil
}

func (v *Verifier) fail(code, message string) error {
	if v.mode == VerifyRequire {
		return NewPoisonError(message, WithCode(code))
	}
	if v.logger != nil {
		v.logger.Warn("[flux] " + message)
	}
	return nil
}

// NewVerifier construye el verificador. Devuelve (nil, nil) en modo off: no se paga lo
// que no se usa, y el default del protocolo es off.
//
// El logger solo se usa en modo warn. Nil significa silencio, como en ConnectOptions.
func NewVerifier(opts SigningOptions, logger *slog.Logger) (*Verifier, error) {
	mode := opts.Verify
	if mode == "" {
		mode = VerifyOff
	}
	if mode == VerifyOff {
		return nil, nil
	}
	if mode != VerifyWarn && mode != VerifyRequire {
		return nil, &SigningKeyError{Message: fmt.Sprintf(
			"modo de verificación %q desconocido: usa off, warn o require (07-signing.md §7)", mode)}
	}

	keys := make(map[string]ed25519.PublicKey, len(opts.PublicKeys))
	for id, pemText := range opts.PublicKeys {
		block, _ := pem.Decode([]byte(pemText))
		if block == nil {
			return nil, &SigningKeyError{Message: fmt.Sprintf(
				"la clave pública %q no es un PEM válido; se espera SPKI "+
					"(-----BEGIN PUBLIC KEY-----)", id)}
		}
		parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, &SigningKeyError{
				Message: fmt.Sprintf("clave pública %q inválida", id), Cause: err}
		}
		key, ok := parsed.(ed25519.PublicKey)
		if !ok {
			return nil, &SigningKeyError{Message: fmt.Sprintf(
				"la clave pública %q es %T, se esperaba ed25519.PublicKey", id, parsed)}
		}
		keys[id] = key
	}

	if len(keys) == 0 {
		return nil, &SigningKeyError{Message: fmt.Sprintf(
			"Verify = %q requiere PublicKeys. Incluye también las claves RETIRADAS mientras "+
				"existan eventos firmados con ellas (07-signing.md §6)", mode)}
	}
	return &Verifier{keys: keys, mode: mode, logger: logger}, nil
}

// ─── Utilidad ────────────────────────────────────────────────────────────────

// KeyPair es un par Ed25519 en PEM: PKCS#8 la privada, SPKI la pública.
type KeyPair struct {
	PrivateKeyPEM string
	PublicKeyPEM  string
}

// GenerateKeyPair genera un par Ed25519. Comodidad para pruebas y para `flux keygen`.
func GenerateKeyPair() (KeyPair, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return KeyPair{}, err
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return KeyPair{}, err
	}
	pubDER, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return KeyPair{}, err
	}
	return KeyPair{
		PrivateKeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER})),
		PublicKeyPEM:  string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})),
	}, nil
}
