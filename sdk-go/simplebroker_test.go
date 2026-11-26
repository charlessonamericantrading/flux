package flux

// Brokers pequeños para las filas de `03-delivery.md §6.1` que el entorno principal no puede dar: uno SIN TLS, uno cuyo
// certificado no cubre el host, uno que exige certificado de cliente, con usuario y contraseña, con token, y uno en modo
// operador/JWT para probar `.creds` de verdad. Se arrancan cuando el test lo pide —no antes—, que es lo que permite
// probar «el broker aún no ha arrancado».

import (
	"crypto/sha512"
	"encoding/base32"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nkeys"
)

type simpleBroker struct {
	t       testing.TB
	bin     string
	dir     string
	Port    int
	URL     string
	PKI     testPKI
	conf    string
	cmd     *exec.Cmd
	JWT     *testJWTEnv
	logPath string
}

// newSimpleBroker prepara un broker de un tipo (sin arrancarlo): plain, tls, tls-san, mtls, tls-userpass, tls-token, jwt.
func newSimpleBroker(t *testing.T, kind string) *simpleBroker {
	t.Helper()
	bin := requireInfra(t)
	dir := t.TempDir()
	b := &simpleBroker{t: t, bin: bin, dir: dir, Port: freePort(t), PKI: newPKI(t, dir)}
	b.URL = fmt.Sprintf("nats://127.0.0.1:%d", b.Port)
	b.logPath = filepath.Join(dir, "server.log")
	tlsBlock := func(cert, key string, extra string) string {
		return fmt.Sprintf("tls {\n  cert_file: %q\n  key_file: %q\n  timeout: 2\n%s}\n", cert, key, extra)
	}
	body := ""
	switch kind {
	case "plain":
	case "tls":
		body = tlsBlock(b.PKI.ServerCert, b.PKI.ServerKey, "")
	case "tls-san":
		body = tlsBlock(b.PKI.WrongSANCert, b.PKI.WrongSANKey, "")
	case "mtls":
		body = tlsBlock(b.PKI.ServerCert, b.PKI.ServerKey, fmt.Sprintf("  ca_file: %q\n  verify: true\n", b.PKI.CA))
	case "tls-userpass":
		body = tlsBlock(b.PKI.ServerCert, b.PKI.ServerKey, "") + "authorization {\n  user: svc\n  password: secreto\n}\n"
	case "tls-token":
		body = tlsBlock(b.PKI.ServerCert, b.PKI.ServerKey, "") + "authorization {\n  token: tk-secreto\n}\n"
	case "jwt":
		b.JWT = newJWTEnv(t)
		body = tlsBlock(b.PKI.ServerCert, b.PKI.ServerKey, "") + b.JWT.conf
	default:
		t.Fatalf("broker de tipo desconocido %q", kind)
	}
	b.conf = fmt.Sprintf("host: \"127.0.0.1\"\nport: %d\n%s", b.Port, body)
	return b
}

// Start arranca el servidor y espera a que acepte conexiones.
func (b *simpleBroker) Start() {
	t := b.t
	t.Helper()
	confPath := filepath.Join(b.dir, "server.conf")
	if err := os.WriteFile(confPath, []byte(b.conf), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(b.bin, "-c", confPath, "-l", b.logPath)
	if err := cmd.Start(); err != nil {
		t.Fatalf("no se pudo lanzar nats-server: %v", err)
	}
	b.cmd = cmd
	t.Cleanup(b.Stop)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if log, _ := os.ReadFile(b.logPath); strings.Contains(string(log), "Server is ready") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("nats-server no llegó a estar listo")
}

func (b *simpleBroker) Stop() {
	if b.cmd != nil && b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
		_, _ = b.cmd.Process.Wait()
		b.cmd = nil
	}
}

// silentPort devuelve un puerto en el que NO escucha nadie.
func silentPort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return p
}

// ─── Un broker en modo operador/JWT ──────────────────────────────────────────

// testJWTEnv monta a mano un broker en modo OPERADOR/JWT, para probar `.creds` de verdad.
//
// `nsc` es la herramienta oficial para esto, pero no está en el CI ni en la mayoría de máquinas, y añadirla solo para un
// test es una dependencia que se rompe en silencio. Un JWT de NATS es un ed25519 sobre
// `base64url(cabecera).base64url(claims)`: con `nkeys` —que el cliente ya trae— basta. El broker real decide si lo
// acepta, que es lo que se quiere probar: si el JWT estuviera mal firmado, `.creds` no conectaría.
type testJWTEnv struct {
	conf string
	// Creds es un `.creds` válido de un usuario de la cuenta PROD, como lo escribiría `nsc`.
	Creds string
	// CredsForeignSeed lleva el JWT del usuario pero la semilla de OTRA clave: el JWT no corresponde a quien firma.
	CredsForeignSeed string
}

var base32NoPad = base32.StdEncoding.WithPadding(base32.NoPadding)

func b64u(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func signJWT(t testing.TB, claims map[string]any, issuer nkeys.KeyPair) string {
	t.Helper()
	c := map[string]any{"iat": time.Now().Unix()}
	for k, v := range claims {
		c[k] = v
	}
	raw, _ := json.Marshal(c)
	sum := sha512.Sum512_256(raw)
	c["jti"] = base32NoPad.EncodeToString(sum[:])
	payload, _ := json.Marshal(c)
	h := b64u([]byte(`{"typ":"JWT","alg":"ed25519-nkey"}`))
	p := b64u(payload)
	sig, err := issuer.Sign([]byte(h + "." + p))
	if err != nil {
		t.Fatal(err)
	}
	return h + "." + p + "." + b64u(sig)
}

func newJWTEnv(t testing.TB) *testJWTEnv {
	t.Helper()
	must := func(kp nkeys.KeyPair, err error) nkeys.KeyPair {
		if err != nil {
			t.Fatal(err)
		}
		return kp
	}
	pub := func(kp nkeys.KeyPair) string { p, _ := kp.PublicKey(); return p }
	seed := func(kp nkeys.KeyPair) string { s, _ := kp.Seed(); return string(s) }

	operador, sistema, prod := must(nkeys.CreateOperator()), must(nkeys.CreateAccount()), must(nkeys.CreateAccount())
	usuario, ajeno := must(nkeys.CreateUser()), must(nkeys.CreateUser())

	jwtOperador := signJWT(t, map[string]any{
		"iss": pub(operador), "name": "op-test", "sub": pub(operador),
		"nats": map[string]any{"type": "operator", "version": 2, "system_account": pub(sistema)},
	}, operador)
	limites := func(mem, disk int) map[string]any {
		return map[string]any{
			"subs": -1, "data": -1, "payload": -1, "imports": -1, "exports": -1, "wildcards": true, "conn": -1,
			"leaf": -1, "mem_storage": mem, "disk_storage": disk, "streams": -1, "consumer": -1,
		}
	}
	cuenta := func(nombre string, kp nkeys.KeyPair, l map[string]any) string {
		return signJWT(t, map[string]any{
			"iss": pub(operador), "name": nombre, "sub": pub(kp),
			"nats": map[string]any{"limits": l, "type": "account", "version": 2},
		}, operador)
	}
	jwtUsuario := signJWT(t, map[string]any{
		"iss": pub(prod), "name": "svc", "sub": pub(usuario),
		"nats": map[string]any{"pub": map[string]any{}, "sub": map[string]any{}, "subs": -1, "data": -1, "payload": -1, "type": "user", "version": 2},
	}, prod)
	creds := func(semilla string) string {
		return strings.Join([]string{
			"-----BEGIN NATS USER JWT-----", jwtUsuario, "------END NATS USER JWT------", "",
			"************************* IMPORTANT *************************",
			"    NKEY Seed printed below can be used to sign and prove identity.",
			"    NKEYs are sensitive and should be treated as secrets.", "",
			"-----BEGIN USER NKEY SEED-----", semilla, "------END USER NKEY SEED------", "",
			"*************************************************************", "",
		}, "\n")
	}
	return &testJWTEnv{
		conf: fmt.Sprintf("operator: %s\nsystem_account: %s\nresolver: MEMORY\nresolver_preload: {\n  %s: %s\n  %s: %s\n}\n",
			jwtOperador, pub(sistema), pub(sistema), cuenta("SYS", sistema, limites(0, 0)), pub(prod), cuenta("PROD", prod, limites(-1, -1))),
		Creds:            creds(seed(usuario)),
		CredsForeignSeed: creds(seed(ajeno)),
	}
}
