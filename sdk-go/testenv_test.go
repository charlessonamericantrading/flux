package flux

// El entorno de las pruebas de integración: un `nats-server` REAL con TLS, la ACL que genera
// `scripts/gen-acl.mjs`, una identidad nkey por servicio y `flux provision` con credenciales de operación.
//
// Es lo mismo que monta `conformance/security/entorno.mjs` para los SDKs de Node y de Rust, y por la misma
// razón: un broker abierto no prueba nada de seguridad. Los permisos NO se escriben aquí a mano (el SDK de
// Rust y el de Node lo hacían y los copiaban del generador): se piden al generador (`--json`), y el bloque
// `accounts` que carga el servidor es el que él escribe (`--nkeys`). Los aprovisiona la CLI (`flux provision`),
// no el test: un durable «aprovisionado a mano» con otra configuración haría pasar lo que en producción falla.
//
//	NATS_SERVER=/ruta/a/nats-server FLUX_REQUIRE_BROKER=1 go test ./...
//
// Necesita además `node` y `(cd cli && npm install)`. Sin ellos las pruebas se SALTAN con un aviso… salvo con
// FLUX_REQUIRE_BROKER=1: en CI eso las convierte en un fallo. Un fixture que nadie ejecuta aparenta cobertura.
//
// Los puertos salen de `FLUX_TEST_PORTS=desde-hasta` (por defecto 7300-7399).

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// ─── Qué hay y qué falta ─────────────────────────────────────────────────────

func brokerRequired() bool {
	v := os.Getenv("FLUX_REQUIRE_BROKER")
	return v != "" && v != "0"
}

// repoRoot es la raíz del repositorio: el paquete vive en `<raíz>/sdk-go`.
func repoRoot(t testing.TB) string {
	t.Helper()
	abs, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	return abs
}

func natsServerBin() string {
	if p := os.Getenv("NATS_SERVER"); p != "" {
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p
		}
		return ""
	}
	if p, err := exec.LookPath("nats-server"); err == nil {
		return p
	}
	return ""
}

// requireInfra salta la prueba (o la FALLA con FLUX_REQUIRE_BROKER=1) si falta algo de lo que necesita.
func requireInfra(t testing.TB) string {
	t.Helper()
	falta := ""
	bin := natsServerBin()
	switch {
	case bin == "":
		falta = "no hay nats-server (define NATS_SERVER con la ruta del binario, o ponlo en el PATH)"
	case func() bool { _, err := exec.LookPath("node"); return err != nil }():
		falta = "no hay `node` en el PATH: gen-acl.mjs y `flux provision` son de Node"
	case func() bool {
		_, err := os.Stat(filepath.Join(repoRoot(t), "cli", "node_modules", "@nats-io"))
		return err != nil
	}():
		falta = "faltan las dependencias de la CLI: ejecuta `(cd cli && npm install)`"
	}
	if falta != "" {
		if brokerRequired() {
			t.Fatalf("FLUX_REQUIRE_BROKER=1 y %s. Sin esto las pruebas de seguridad no se ejecutan, y un fixture "+
				"que nadie ejecuta aparenta cobertura.", falta)
		}
		t.Skipf("saltado: %s. Con FLUX_REQUIRE_BROKER=1 fallaría.", falta)
	}
	return bin
}

// ─── Puertos ─────────────────────────────────────────────────────────────────

var nextPort atomic.Uint32

// freePort devuelve un puerto libre del rango propio (FLUX_TEST_PORTS=desde-hasta, por defecto 7300-7399).
// Se comprueba con un `listen` real: un servidor anterior que no terminó de morir no debe romper este.
func freePort(t testing.TB) int {
	t.Helper()
	from, to := 7300, 7399
	if v := os.Getenv("FLUX_TEST_PORTS"); v != "" {
		if _, err := fmt.Sscanf(v, "%d-%d", &from, &to); err != nil || to < from {
			t.Fatalf("FLUX_TEST_PORTS=%q: se espera desde-hasta", v)
		}
	}
	span := to - from + 1
	for i := 0; i < span; i++ {
		port := from + int(nextPort.Add(1)-1)%span
		l, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
		if err == nil {
			l.Close()
			return port
		}
	}
	t.Fatalf("no queda ningún puerto libre en %d-%d", from, to)
	return 0
}

// ─── PKI ─────────────────────────────────────────────────────────────────────

type testPKI struct {
	CA, OtherCA, ServerCert, ServerKey string
	// WrongSANCert/Key: un certificado FIRMADO por la CA, con un SAN que no es `localhost` ni `127.0.0.1`.
	WrongSANCert, WrongSANKey string
	// ClientCert/Key: un certificado de cliente firmado por la CA (mTLS). OtherClientCert/Key, por la CA ajena.
	ClientCert, ClientKey           string
	OtherClientCert, OtherClientKey string
}

func writePEM(t testing.TB, path, kind string, der []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), mode); err != nil {
		t.Fatal(err)
	}
}

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

// newPKI crea una CA propia, un certificado de servidor con SAN `localhost` y `127.0.0.1`, otro con un SAN que no
// encaja, un certificado de cliente, y una CA AJENA que no firmó nada: sirve para probar que un cliente que confía en
// otra CA rechaza el certificado del servidor.
func newPKI(t testing.TB, dir string) testPKI {
	t.Helper()
	serial := int64(0)
	nextSerial := func() *big.Int { serial++; return big.NewInt(time.Now().UnixNano() + serial) }
	newCA := func(name, cn string) testCA {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: nextSerial(), Subject: pkix.Name{CommonName: cn},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(48 * time.Hour),
			IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		cert, _ := x509.ParseCertificate(der)
		writePEM(t, filepath.Join(dir, name+".pem"), "CERTIFICATE", der, 0o644)
		return testCA{cert, key}
	}
	leaf := func(name string, ca testCA, tmpl *x509.Certificate) (string, string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tmpl.SerialNumber = nextSerial()
		tmpl.NotBefore, tmpl.NotAfter = time.Now().Add(-time.Hour), time.Now().Add(48*time.Hour)
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		c, k := filepath.Join(dir, name+".pem"), filepath.Join(dir, name+".key")
		writePEM(t, c, "CERTIFICATE", der, 0o644)
		writePEM(t, k, "EC PRIVATE KEY", keyDER, 0o600)
		return c, k
	}
	ca := newCA("ca", "flux-test-ca")
	otra := newCA("otra-ca", "flux-otra-ca")
	server := func(cn string, dns []string, ips []net.IP) *x509.Certificate {
		return &x509.Certificate{
			Subject: pkix.Name{CommonName: cn}, DNSNames: dns, IPAddresses: ips,
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
	}
	client := &x509.Certificate{
		Subject: pkix.Name{CommonName: "flux-cliente"}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	p := testPKI{CA: filepath.Join(dir, "ca.pem"), OtherCA: filepath.Join(dir, "otra-ca.pem")}
	p.ServerCert, p.ServerKey = leaf("servidor", ca, server("localhost", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")}))
	p.WrongSANCert, p.WrongSANKey = leaf("servidor-otro-san", ca, server("otro", []string{"otro.example"}, nil))
	p.ClientCert, p.ClientKey = leaf("cliente", ca, client)
	p.OtherClientCert, p.OtherClientKey = leaf("cliente-otra-ca", otra, client)
	return p
}

// ─── Identidades ─────────────────────────────────────────────────────────────

type testKey struct {
	Seed, Pub string
}

func newKey(t testing.TB) testKey {
	t.Helper()
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	seed, _ := kp.Seed()
	pub, _ := kp.PublicKey()
	return testKey{Seed: string(seed), Pub: pub}
}

// credsFor es un `.creds` con un JWT de relleno: un servidor con nkeys estáticas solo mira la semilla. Para un
// `.creds` de verdad (JWT firmado por un operador) ver testJWTEnv.
func credsFor(seed string) string {
	return "-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QifQ.e30.relleno\n------END NATS USER JWT------\n\n" +
		"-----BEGIN USER NKEY SEED-----\n" + seed + "\n------END USER NKEY SEED------\n"
}

// ─── Los permisos que genera gen-acl.mjs ─────────────────────────────────────

// genPerms son los permisos de un servicio tal y como los afirma `gen-acl.mjs --json`.
type genPerms struct {
	Name           string          `json:"name"`
	Publish        []string        `json:"publish"`
	Subscribe      []string        `json:"subscribe"`
	AllowResponses json.RawMessage `json:"allowResponses"`
	Durables       []struct {
		Stream  string `json:"stream"`
		Durable string `json:"durable"`
		Filter  string `json:"filter"`
		Kind    string `json:"kind"`
	} `json:"durables"`
}

func runNode(t testing.TB, env []string, args ...string) (string, string, int) {
	t.Helper()
	cmd := exec.Command("node", args...)
	cmd.Dir = repoRoot(t)
	cmd.Env = append(os.Environ(), env...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("no se pudo ejecutar node %v: %v", args, err)
	}
	return out.String(), errb.String(), code
}

// generatedPerms ejecuta el generador de verdad y devuelve los permisos de cada servicio.
func generatedPerms(t testing.TB, manifest string, extra ...string) map[string]*genPerms {
	t.Helper()
	args := append([]string{"scripts/gen-acl.mjs", manifest, "--json"}, extra...)
	out, errs, code := runNode(t, nil, args...)
	if code != 0 {
		t.Fatalf("gen-acl.mjs falló (%d):\n%s", code, errs)
	}
	var list []*genPerms
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		t.Fatalf("la salida de gen-acl.mjs --json no es JSON: %v\n%s", err, out)
	}
	m := map[string]*genPerms{}
	for _, p := range list {
		m[p.Name] = p
	}
	return m
}

func quoteList(xs []string) string {
	q := make([]string, len(xs))
	for i, x := range xs {
		q[i] = strconv.Quote(x)
	}
	return "[" + strings.Join(q, ", ") + "]"
}

// userConf escribe un usuario nkey con unos permisos, como lo escribe `gen-acl.mjs`. OJO: una lista vacía NO deniega
// nada —`allow: []` es «sin restricción»— (A-25), así que nunca se pasa una.
func userConf(pub string, p *genPerms, drop map[string]bool) string {
	keep := func(xs []string) []string {
		out := []string{}
		for _, x := range xs {
			if !drop[x] {
				out = append(out, x)
			}
		}
		return out
	}
	s := fmt.Sprintf("      { nkey: %q\n        permissions: {\n          publish: { allow: %s }\n          subscribe: { allow: %s }\n",
		pub, quoteList(keep(p.Publish)), quoteList(keep(p.Subscribe)))
	if len(p.AllowResponses) > 0 && string(p.AllowResponses) != "null" {
		var ar struct {
			Max     int    `json:"max"`
			Expires string `json:"expires"`
		}
		if string(p.AllowResponses) == "true" {
			s += "          allow_responses: true\n"
		} else if json.Unmarshal(p.AllowResponses, &ar) == nil {
			s += fmt.Sprintf("          allow_responses: { max: %d, expires: %q }\n", ar.Max, ar.Expires)
		}
	}
	return s + "        } },\n"
}

// ─── El broker ───────────────────────────────────────────────────────────────

// variant es un servicio con un permiso de menos (o con otro conjunto de permisos): la identidad de `Base` —su
// nombre, su inbox, sus durables— con una nkey distinta cuya ACL es la generada, estropeada a propósito. Es cómo se
// prueba que quitar un permiso SE NOTA, en los dos sentidos.
type variant struct {
	// Name es el nombre con el que se pide en connectAs.
	Name string
	Base string
	// Drop son los permisos (de publicación o suscripción) que se quitan, EXACTOS como los genera gen-acl.
	Drop []string
	// DropAllowResponses quita el allow_responses del dueño.
	DropAllowResponses bool
	// Dev concede CONSUMER.CREATE (gen-acl.mjs --dev).
	Dev bool
	// Subscribe, si no es nil, REEMPLAZA los permisos de suscripción: para quitar el único que tiene un servicio (una lista
	// vacía no deniega nada, así que se pone otro que no sirve de nada).
	Subscribe []string
}

type envOptions struct {
	Variants []variant
	// NoProvision omite `flux provision`: nada existe, y el servicio no puede crearlo.
	NoProvision bool
	// Manifest es el manifiesto; por defecto examples/services.json.
	Manifest string
}

type testEnv struct {
	t       testing.TB
	dir     string
	Port    int
	URL     string
	PKI     testPKI
	Keys    map[string]testKey
	Perms   map[string]*genPerms
	ops     testKey
	cmd     *exec.Cmd
	Manif   string
	Prov    string
	service map[string]string
	logPath string
	wg      sync.WaitGroup
}

// startEnv monta el entorno: PKI, nkeys, ACL generada, servidor, y `flux provision`.
func startEnv(t *testing.T, opts envOptions) *testEnv {
	t.Helper()
	bin := requireInfra(t)
	dir := t.TempDir()
	manifest := opts.Manifest
	if manifest == "" {
		manifest = filepath.Join(repoRoot(t), "examples", "services.json")
	}
	env := &testEnv{t: t, dir: dir, Manif: manifest, Keys: map[string]testKey{}, ops: newKey(t), service: map[string]string{}}
	env.PKI = newPKI(t, dir)
	env.Perms = generatedPerms(t, manifest)
	devPerms := generatedPerms(t, manifest, "--dev")

	// El bloque `accounts` lo escribe el generador, con la clave PÚBLICA de cada servicio.
	pubs := map[string]string{}
	for name := range env.Perms {
		env.Keys[name] = newKey(t)
		pubs[name] = env.Keys[name].Pub
	}
	mapFile := filepath.Join(dir, "nkeys.json")
	b, _ := json.Marshal(pubs)
	if err := os.WriteFile(mapFile, b, 0o600); err != nil {
		t.Fatal(err)
	}
	acl, errs, code := runNode(t, nil, "scripts/gen-acl.mjs", manifest, "--nkeys", mapFile)
	if code != 0 {
		t.Fatalf("gen-acl.mjs --nkeys falló:\n%s", errs)
	}

	// El usuario de operación (aprovisiona; no es de ningún servicio) y las variantes, tras `users: [`.
	extra := fmt.Sprintf("      { nkey: %q },\n", env.ops.Pub)
	for _, v := range opts.Variants {
		base := env.Perms[v.Base]
		if v.Dev {
			base = devPerms[v.Base]
		}
		if base == nil {
			t.Fatalf("variante %q: no hay un servicio %q en el manifiesto", v.Name, v.Base)
		}
		key := newKey(t)
		env.Keys[v.Name] = key
		env.service[v.Name] = v.Base
		p := *base
		if v.Subscribe != nil {
			p.Subscribe = v.Subscribe
		}
		if v.DropAllowResponses {
			p.AllowResponses = nil
		}
		drop := map[string]bool{}
		for _, d := range v.Drop {
			drop[d] = true
		}
		extra += userConf(key.Pub, &p, drop)
	}
	const marca = "    users: ["
	partes := strings.SplitN(acl, marca, 2)
	if len(partes) != 2 {
		t.Fatalf("la config generada no tiene `users: [` donde se esperaba:\n%s", acl[:min(len(acl), 400)])
	}
	acl = partes[0] + marca + "\n" + extra + partes[1]
	if err := os.WriteFile(filepath.Join(dir, "acl.conf"), []byte(acl), 0o644); err != nil {
		t.Fatal(err)
	}

	env.Port = freePort(t)
	env.URL = fmt.Sprintf("nats://127.0.0.1:%d", env.Port)
	env.logPath = filepath.Join(dir, "server.log")
	conf := fmt.Sprintf("host: \"127.0.0.1\"\nport: %d\nserver_name: \"flux-go-%d\"\njetstream {\n  store_dir: %q\n}\n"+
		"tls {\n  cert_file: %q\n  key_file: %q\n  timeout: 2\n}\ninclude \"acl.conf\"\n",
		env.Port, env.Port, filepath.Join(dir, "js"), env.PKI.ServerCert, env.PKI.ServerKey)
	confPath := filepath.Join(dir, "server.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	env.startServer(bin, confPath)
	t.Cleanup(env.stop)

	if !opts.NoProvision {
		creds := filepath.Join(dir, "ops.creds")
		if err := os.WriteFile(creds, []byte(credsFor(env.ops.Seed)), 0o600); err != nil {
			t.Fatal(err)
		}
		// `flux provision` con credenciales de operación, por TLS con la CA efímera. La CLI no tiene `--ca`: la CA llega por
		// el mecanismo de Node (NODE_EXTRA_CA_CERTS). Un operador con una CA privada tendría que hacer lo mismo.
		out, errs, code := runNode(t, []string{"NO_COLOR=1", "NODE_EXTRA_CA_CERTS=" + env.PKI.CA},
			"cli/src/index.mjs", "provision", manifest, "--server", fmt.Sprintf("tls://127.0.0.1:%d", env.Port), "--creds", creds)
		env.Prov = out + errs
		if code != 0 {
			t.Fatalf("`flux provision` falló (%d):\n%s", code, env.Prov)
		}
	}
	return env
}

func (e *testEnv) startServer(bin, conf string) {
	t := e.t
	t.Helper()
	cmd := exec.Command(bin, "-c", conf, "-l", e.logPath)
	errFile, _ := os.Create(filepath.Join(e.dir, "server.err"))
	cmd.Stderr = errFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("no se pudo lanzar nats-server: %v", err)
	}
	e.cmd = cmd
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-exited:
			log, _ := os.ReadFile(e.logPath)
			er, _ := os.ReadFile(filepath.Join(e.dir, "server.err"))
			t.Fatalf("nats-server terminó antes de aceptar conexiones:\n%s\n%s", log, er)
		default:
		}
		if log, _ := os.ReadFile(e.logPath); bytes.Contains(log, []byte("Server is ready")) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("nats-server no llegó a estar listo")
}

func (e *testEnv) stop() {
	if e.cmd != nil && e.cmd.Process != nil {
		_ = e.cmd.Process.Kill()
		_, _ = e.cmd.Process.Wait()
		e.cmd = nil
	}
}

// serverLog devuelve el log del servidor.
func (e *testEnv) serverLog() string {
	b, _ := os.ReadFile(e.logPath)
	return string(b)
}

// violations devuelve las líneas del log del servidor que anotan una violación de permisos. Una violación es una
// señal de alerta: una por arranque la vuelve ruido y tapa las reales.
func (e *testEnv) violations() []string {
	// El servidor escribe el log por su cuenta: se espera a que deje de crecer, porque llega por otra tubería que la
	// respuesta de la operación.
	var last int64 = -1
	for i := 0; i < 20; i++ {
		st, err := os.Stat(e.logPath)
		if err != nil || st.Size() == last {
			break
		}
		last = st.Size()
		time.Sleep(100 * time.Millisecond)
	}
	var out []string
	sc := bufio.NewScanner(strings.NewReader(e.serverLog()))
	for sc.Scan() {
		if strings.Contains(sc.Text(), "Violation") {
			out = append(out, sc.Text())
		}
	}
	return out
}

// ─── Conectar como un servicio ───────────────────────────────────────────────

// logSink recoge lo que el SDK escribe en su Logger, para asertar lo que se avisa.
type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (l *logSink) Logger() *slog.Logger { return slog.New(&sinkHandler{l: l}) }

type sinkHandler struct{ l *logSink }

func (h *sinkHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *sinkHandler) Handle(_ context.Context, r slog.Record) error {
	h.l.add(r.Level.String() + " " + r.Message)
	return nil
}
func (h *sinkHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *sinkHandler) WithGroup(string) slog.Handler      { return h }

func (l *logSink) add(s string) {
	l.mu.Lock()
	l.lines = append(l.lines, s)
	l.mu.Unlock()
}

func (l *logSink) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

func (l *logSink) text() string { return strings.Join(l.all(), "\n") }

// Contains dice si alguna línea contiene `s`.
func (l *logSink) Contains(s string) bool { return strings.Contains(l.text(), s) }

// Count cuenta las líneas que contienen `s`.
func (l *logSink) Count(s string) int {
	n := 0
	for _, x := range l.all() {
		if strings.Contains(x, s) {
			n++
		}
	}
	return n
}

// connectAs conecta como un servicio del manifiesto (o una variante suya) con su nkey, por TLS y con la CA del
// entorno, y devuelve el bus y lo que escribió en su Logger. `mut` ajusta las opciones antes de conectar.
func (e *testEnv) connectAs(name string, mut func(*ConnectOptions)) (*Bus, *logSink, error) {
	e.t.Helper()
	key, ok := e.Keys[name]
	if !ok {
		e.t.Fatalf("no hay identidad para %q", name)
	}
	service := name
	if base, ok := e.service[name]; ok {
		service = base
	}
	sink := &logSink{}
	opts := ConnectOptions{
		Servers: e.URL, Service: service, Environment: "test", Version: "1.0.0",
		TenantID:      "acme",
		Credentials:   CredentialsFromNKeySeed(key.Seed),
		TLS:           &TLSOptions{CAFile: e.PKI.CA},
		SchemaBaseURL: "https://schemas.internal",
		Logger:        sink.Logger(),
	}
	if mut != nil {
		mut(&opts)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	bus, err := Connect(ctx, opts)
	if err == nil {
		e.t.Cleanup(func() { _ = bus.Close() })
	}
	return bus, sink, err
}

// opsConn conecta con las credenciales de OPERACIÓN —las de `flux provision`, que no son las de ningún servicio— y
// devuelve la conexión y su JetStream: lo que usa un test para estropear o inspeccionar el broker desde fuera del SDK.
func (e *testEnv) opsConn() (*nats.Conn, jetstream.JetStream) {
	e.t.Helper()
	pool := x509.NewCertPool()
	pem, err := os.ReadFile(e.PKI.CA)
	if err != nil {
		e.t.Fatal(err)
	}
	pool.AppendCertsFromPEM(pem)
	kp, err := nkeys.FromSeed([]byte(e.ops.Seed))
	if err != nil {
		e.t.Fatal(err)
	}
	nc, err := nats.Connect(e.URL, nats.Secure(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}),
		nats.Nkey(e.ops.Pub, func(nonce []byte) ([]byte, error) { return kp.Sign(nonce) }))
	if err != nil {
		e.t.Fatalf("las credenciales de operación no conectaron: %v", err)
	}
	e.t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		e.t.Fatal(err)
	}
	return nc, js
}
