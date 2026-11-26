package flux

// Las opciones de seguridad mal formadas (INVALID_CREDENTIALS, INVALID_TLS_OPTIONS) y el inbox privado: lo que se sabe
// SIN hablar con el broker. No necesita broker, y por eso corre siempre — pero la prueba de que se detecta ANTES de abrir
// ningún socket es real: un listener cuenta cada conexión que llega.

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nkeys"
)

// socketEspia es un listener que cuenta las conexiones que le llegan. Sirve para demostrar que Connect() falló ANTES de
// dialar: «se emite antes de abrir ningún socket» (04-errors.md §5.3).
type socketEspia struct {
	l    net.Listener
	hits atomic.Int32
	URL  string
}

func nuevoSocketEspia(t *testing.T) *socketEspia {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &socketEspia{l: l, URL: "nats://" + l.Addr().String()}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			s.hits.Add(1)
			_ = c.Close()
		}
	}()
	t.Cleanup(func() { _ = l.Close() })
	return s
}

func semillaDeUsuario(t *testing.T) string {
	t.Helper()
	kp, _ := nkeys.CreateUser()
	s, _ := kp.Seed()
	return string(s)
}

func escribe(t *testing.T, nombre, contenido string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), nombre)
	if err := os.WriteFile(p, []byte(contenido), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestInvalidCredentials_CadaFormaDeMalFormadoEnSuSituacionYSoloEnEsa(t *testing.T) {
	espia := nuevoSocketEspia(t)
	semilla := semillaDeUsuario(t)
	okCreds := credsFor(semilla)

	malas := []struct {
		nombre string
		c      Credentials
		dice   string
	}{
		{"CredsFile vacío (variable de entorno sin definir)", CredentialsFromFile(""), "CredsFile"},
		{"CredsContent vacío", CredentialsFromContent("  "), "CredsContent"},
		{"NKeySeed vacía", CredentialsFromNKeySeed(""), "NKeySeed"},
		{"Token vacío", CredentialsFromToken(""), "Token"},
		{"usuario sin contraseña", Credentials{User: "svc"}, "van juntos"},
		{"contraseña sin usuario", Credentials{Password: "x"}, "van juntos"},
		{"usuario vacío declarado", CredentialsFromUserPass("", "x"), "User"},
		{"dos métodos a la vez", Credentials{Token: "t", NKeySeed: semilla}, "elige UN método"},
		{".creds + token", Credentials{CredsContent: okCreds, Token: "t"}, "elige UN método"},
		{".creds ilegible: la ruta no existe", Credentials{CredsFile: "/no/existe/x.creds"}, "no se puede leer"},
		{".creds sin JWT ni semilla", Credentials{CredsFile: escribe(t, "vacio.creds", "hola")}, "no es utilizable"},
		{".creds con JWT pero sin semilla", Credentials{CredsContent: "-----BEGIN NATS USER JWT-----\nabc.def.ghi\n------END NATS USER JWT------\n"}, "semilla"},
		{"semilla que no es de usuario (cuenta, SA…)", func() Credentials {
			kp, _ := nkeys.CreateAccount()
			s, _ := kp.Seed()
			return Credentials{NKeySeed: string(s)}
		}(), "USUARIO"},
		{"semilla basura con prefijo SU", Credentials{NKeySeed: "SUxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}, "semilla nkey válida"},
	}
	for _, c := range malas {
		t.Run(c.nombre, func(t *testing.T) {
			o := baseOpts(espia.URL)
			o.Credentials = c.c
			t0 := time.Now()
			// Con un plazo: si Connect() dialara en vez de rechazar, esperaría al «broker» para siempre.
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := Connect(ctx, o)
			var ic *InvalidCredentialsError
			if !errors.As(err, &ic) {
				t.Fatalf("err = %v (%T), se esperaba *InvalidCredentialsError", err, err)
			}
			if ic.FluxCode() != "INVALID_CREDENTIALS" || ic.FluxClass() != ClassPermanent {
				t.Errorf("code/class = %s/%s", ic.FluxCode(), ic.FluxClass())
			}
			if !strings.Contains(ic.Error(), c.dice) {
				t.Errorf("el mensaje debe nombrar la opción (%q): %v", c.dice, ic)
			}
			var cr *ConnectionRejectedError
			if errors.As(err, &cr) {
				t.Errorf("un .creds/opción mal formada NO es CONNECTION_REJECTED: %v", err)
			}
			if time.Since(t0) > time.Second {
				t.Errorf("tardó %v: se sabe sin red", time.Since(t0))
			}
			if strings.Contains(err.Error(), semilla) {
				t.Errorf("el mensaje NO debe incluir una credencial")
			}
		})
	}
	// Y la prueba de que se detecta ANTES de abrir ningún socket.
	time.Sleep(100 * time.Millisecond)
	if n := espia.hits.Load(); n != 0 {
		t.Errorf("Connect() abrió %d socket(s) antes de rechazar opciones mal formadas", n)
	}

	// El otro sentido: lo bien formado NO es INVALID_CREDENTIALS (llega a dialar; aquí el espía cierra y no es un
	// broker, así que acabará esperando: se acota con el contexto).
	bien := []struct {
		nombre string
		c      Credentials
	}{
		{"nkey", CredentialsFromNKeySeed(semilla)},
		{".creds por contenido", CredentialsFromContent(okCreds)},
		{".creds por ruta", CredentialsFromFile(escribe(t, "ok.creds", okCreds))},
		{"token", CredentialsFromToken("t")},
		{"usuario y contraseña", CredentialsFromUserPass("u", "p")},
		{"sin credenciales", Credentials{}},
	}
	for _, c := range bien {
		o := baseOpts(espia.URL)
		o.Credentials = c.c
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		_, err := Connect(ctx, o)
		cancel()
		var ic *InvalidCredentialsError
		if errors.As(err, &ic) {
			t.Errorf("%s: bien formado no puede ser INVALID_CREDENTIALS: %v", c.nombre, err)
		}
	}
}

func TestInvalidTLSOptions_CadaFormaDeMalFormadoEnSuSituacionYSoloEnEsa(t *testing.T) {
	espia := nuevoSocketEspia(t)
	pki := newPKI(t, t.TempDir())
	noPEM := escribe(t, "ca.txt", "esto no es un PEM")

	malas := []struct {
		nombre string
		o      *TLSOptions
		dice   string
	}{
		{"CA que no existe", &TLSOptions{CAFile: "/no/existe/ca.pem"}, "no se puede leer"},
		{"CA que no es un PEM", &TLSOptions{CAFile: noPEM}, "PEM"},
		{"certificado de cliente sin su clave", &TLSOptions{CertFile: pki.ClientCert}, "van juntos"},
		{"clave de cliente sin su certificado", &TLSOptions{KeyFile: pki.ClientKey}, "van juntos"},
		{"par certificado/clave que no se lee", &TLSOptions{CertFile: "/no/existe.pem", KeyFile: "/no/existe.key"}, "no son un par"},
		{"certificado y clave que no casan", &TLSOptions{CertFile: pki.ClientCert, KeyFile: pki.ServerKey}, "no son un par"},
	}
	for _, c := range malas {
		t.Run(c.nombre, func(t *testing.T) {
			o := baseOpts(espia.URL)
			o.TLS = c.o
			t0 := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := Connect(ctx, o)
			var it *InvalidTLSOptionsError
			if !errors.As(err, &it) {
				t.Fatalf("err = %v (%T), se esperaba *InvalidTLSOptionsError", err, err)
			}
			if it.FluxCode() != "INVALID_TLS_OPTIONS" || it.FluxClass() != ClassPermanent {
				t.Errorf("code/class = %s/%s", it.FluxCode(), it.FluxClass())
			}
			if !strings.Contains(it.Error(), c.dice) {
				t.Errorf("el mensaje debe decir %q: %v", c.dice, it)
			}
			var cr *ConnectionRejectedError
			if errors.As(err, &cr) {
				t.Errorf("una opción TLS mal formada NO es CONNECTION_REJECTED: %v", err)
			}
			if time.Since(t0) > time.Second {
				t.Errorf("tardó %v: se sabe sin red", time.Since(t0))
			}
		})
	}
	time.Sleep(100 * time.Millisecond)
	if n := espia.hits.Load(); n != 0 {
		t.Errorf("Connect() abrió %d socket(s) antes de rechazar opciones TLS mal formadas", n)
	}

	// El otro sentido: opciones bien formadas no son INVALID_TLS_OPTIONS.
	for nombre, o := range map[string]*TLSOptions{
		"TLS con las CA del sistema": {},
		"CA propia":                  {CAFile: pki.CA},
		"mTLS":                       {CAFile: pki.CA, CertFile: pki.ClientCert, KeyFile: pki.ClientKey},
	} {
		op := baseOpts(espia.URL)
		op.TLS = o
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		_, err := Connect(ctx, op)
		cancel()
		var it *InvalidTLSOptionsError
		if errors.As(err, &it) {
			t.Errorf("%s: bien formado no puede ser INVALID_TLS_OPTIONS: %v", nombre, err)
		}
	}
}

func TestElInboxEsPrivadoYNoEsUnaOpcion(t *testing.T) {
	// El prefijo lo deriva el SDK del nombre del servicio: no hay ningún campo con el que volver a `_INBOX.>`.
	if p, err := inboxPrefixFor("facturacion-api"); err != nil || p != "_INBOX.facturacion-api" {
		t.Errorf("inboxPrefixFor = %q, %v", p, err)
	}
	// Un nombre con punto haría que el prefijo de un servicio fuera el de otro: `_INBOX.pedidos.>` capturaría
	// `_INBOX.pedidos.api.x`.
	for _, malo := range []string{"pedidos.api", "Pedidos", "", "a b", "pedidos-", "-pedidos", "pe..dos", "_pedidos", "pedidos>"} {
		if _, err := inboxPrefixFor(malo); err == nil {
			t.Errorf("inboxPrefixFor(%q) debía fallar", malo)
		}
	}
}

func TestDenegacionesSeReconocenYSeNormalizan(t *testing.T) {
	casos := []struct {
		texto   string
		op      string
		subject string
		queue   string
		ok      bool
	}{
		{`nats: Permissions Violation for Publish to "rpc.denegado.v1.x.y"`, "publish", "rpc.denegado.v1.x.y", "", true},
		{`nats: Permissions Violation for Subscription to "rpc.permitido.v1.x.y" using queue "svc"`, "subscribe", "rpc.permitido.v1.x.y", "svc", true},
		{`nats: Permissions Violation for Subscription to "_INBOX.otro.>"`, "subscribe", "_INBOX.otro.>", "", true},
		{`nats: timeout`, "", "", "", false},
		{`nats: Authorization Violation`, "", "", "", false},
	}
	for _, c := range casos {
		d, ok := parseDenial(errors.New(c.texto))
		if ok != c.ok || d.op != c.op || d.subject != c.subject || d.queue != c.queue {
			t.Errorf("parseDenial(%q) = %+v, %v", c.texto, d, ok)
		}
	}
	for entrada, quiero := range map[string]string{
		"$JS.ACK.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.1.1.1.1700000000.0": "$JS.ACK.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.>",
		"_INBOX.crm-api.AbCdEf.Tok":     "_INBOX.crm-api.>",
		"_INBOX.crm-api.>":              "_INBOX.crm-api.>",
		"rpc.clientes.cliente.v1.x":     "rpc.clientes.cliente.v1.x",
		"$JS.API.CONSUMER.MSG.NEXT.S.d": "$JS.API.CONSUMER.MSG.NEXT.S.d",
	} {
		if got := normalizeDeniedSubject(entrada); got != quiero {
			t.Errorf("normalizeDeniedSubject(%q) = %q, se esperaba %q", entrada, got, quiero)
		}
	}
}

func TestLosErroresDeSeguridadSonPermanentesYNoSePuedenSustituir(t *testing.T) {
	for _, e := range []ClassifiedError{
		&NotProvisionedError{baseError: baseError{CodeNotProvisioned, ClassPermanent, nil}},
		newPermissionDenied("s", "publish", "x", "", nil),
		&ConnectionRejectedError{baseError: baseError{CodeConnectionRejected, ClassPermanent, nil}},
		newInvalidCredentials("x", nil),
		newInvalidTLS("x"),
	} {
		if e.FluxClass() != ClassPermanent {
			t.Errorf("%T: clase %s, se esperaba PERMANENT (como un HTTP 403: reintentar da lo mismo)", e, e.FluxClass())
		}
		c := Classify(e)
		if c.Class != ClassPermanent || c.Code != e.FluxCode() {
			t.Errorf("%T: el clasificador dice %+v", e, c)
		}
	}
}

func TestElGuardSoloSeCancelaPorLaDenegacionDeSuSubject(t *testing.T) {
	// Una denegación de OTRA operación no es culpa de ésta: sin esta frontera, una petición legítima fallaría por lo que le pasó
	// a otra que corría a la vez.
	b := &Bus{denials: newDenialHub("crm-api", nil), opts: ConnectOptions{Service: "crm-api"}}
	ctx, done := b.guard(context.Background(), "rpc.clientes.cliente.v1.obtener")
	defer done()

	b.denials.handle(nil, nil, errors.New(`nats: Permissions Violation for Publish to "rpc.clientes.cliente.v1.otra"`))
	b.denials.handle(nil, nil, errors.New(`nats: Permissions Violation for Subscription to "rpc.clientes.cliente.v1.obtener"`))
	select {
	case <-ctx.Done():
		t.Fatalf("el contexto se canceló por la denegación de otro subject (u otra operación): %v", context.Cause(ctx))
	case <-time.After(100 * time.Millisecond):
	}

	b.denials.handle(nil, nil, errors.New(`nats: Permissions Violation for Publish to "rpc.clientes.cliente.v1.obtener"`))
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("la denegación de SU subject no canceló el contexto")
	}
	pd := deniedCause(ctx)
	if pd == nil || pd.Subject != "rpc.clientes.cliente.v1.obtener" || pd.Operation != "publish" {
		t.Errorf("deniedCause = %+v", pd)
	}
}
