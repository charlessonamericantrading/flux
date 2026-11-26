package flux

// Los tres códigos de `Connect()` y la primera conexión (04-errors.md §5.3, 03-delivery.md §6.1), contra un broker real.
//
// Cada fila de la tabla de §6.1 tiene su caso, y cada caso comprueba QUÉ código sale, no solo que sale: una fila que
// fallara «rápido» pero con el código de otra mandaría a quien opera a buscar donde no es. Y en los dos sentidos: lo
// bien formado que el broker no acepta es CONNECTION_REJECTED y nunca INVALID_*; lo mal formado es INVALID_* y nunca
// CONNECTION_REJECTED; y un broker que aún no ha arrancado no es ninguno de los tres.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func baseOpts(url string) ConnectOptions {
	return ConnectOptions{
		Servers: url, Service: "pedidos-api", Environment: "test", Version: "1.0.0",
		SchemaBaseURL: "https://schemas.internal",
	}
}

func connectT(t *testing.T, opts ConnectOptions, d time.Duration) (*Bus, error, time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	t0 := time.Now()
	b, err := Connect(ctx, opts)
	if err == nil {
		t.Cleanup(func() { _ = b.Close() })
	}
	return b, err, time.Since(t0)
}

// quiereRechazo comprueba el código, la clase, el motivo y que falló RÁPIDO (no por el plazo del contexto).
func quiereRechazo(t *testing.T, err error, dur time.Duration, motivo RejectionReason) *ConnectionRejectedError {
	t.Helper()
	var cr *ConnectionRejectedError
	if !errors.As(err, &cr) {
		t.Fatalf("err = %v (%T), se esperaba *ConnectionRejectedError(%s)", err, err, motivo)
	}
	if cr.FluxCode() != "CONNECTION_REJECTED" || cr.FluxClass() != ClassPermanent {
		t.Errorf("code/class = %s/%s, se esperaba CONNECTION_REJECTED/permanent", cr.FluxCode(), cr.FluxClass())
	}
	if cr.Reason != motivo {
		t.Errorf("reason = %q, se esperaba %q: %v", cr.Reason, motivo, err)
	}
	if dur > 3*time.Second {
		t.Errorf("tardó %v: un rechazo que esperar no arregla debe fallar de inmediato", dur)
	}
	var ic *InvalidCredentialsError
	var it *InvalidTLSOptionsError
	if errors.As(err, &ic) || errors.As(err, &it) {
		t.Errorf("un rechazo del broker NO es INVALID_*: %v", err)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "reintentar no lo arregla") {
		t.Errorf("el mensaje debe decir que reintentar no lo arregla (para que nadie envuelva Connect() en un bucle): %v", err)
	}
	return cr
}

func TestH_CredencialBienFormadaPeroRechazadaEsConnectionRejectedAuth(t *testing.T) {
	env := startEnv(t, envOptions{})
	// Una semilla de usuario VÁLIDA que el broker no conoce: bien formada, equivocada.
	ajena := newKey(t)
	_, err, dur := connectT(t, func() ConnectOptions {
		o := baseOpts(env.URL)
		o.Credentials = CredentialsFromNKeySeed(ajena.Seed)
		o.TLS = &TLSOptions{CAFile: env.PKI.CA}
		return o
	}(), 15*time.Second)
	cr := quiereRechazo(t, err, dur, RejectionAuth)
	if strings.Contains(err.Error(), ajena.Seed) {
		t.Errorf("el mensaje NO debe incluir la credencial: %v", err)
	}
	if !strings.Contains(cr.Error(), env.URL) {
		t.Errorf("debe nombrar el broker %s: %v", env.URL, err)
	}

	// Y en el otro sentido: la credencial buena conecta.
	b, err, _ := connectT(t, func() ConnectOptions {
		o := baseOpts(env.URL)
		o.Credentials = CredentialsFromNKeySeed(env.Keys["pedidos-api"].Seed)
		o.TLS = &TLSOptions{CAFile: env.PKI.CA}
		return o
	}(), 15*time.Second)
	if err != nil || b == nil {
		t.Fatalf("la credencial buena no conectó: %v", err)
	}
}

func TestH_CertificadoQueNoVerificaEsConnectionRejectedTls(t *testing.T) {
	env := startEnv(t, envOptions{})
	creds := CredentialsFromNKeySeed(env.Keys["pedidos-api"].Seed)
	casos := []struct {
		nombre string
		tls    *TLSOptions
	}{
		{"CA ausente: se verifica contra las CA del sistema, que no conocen la propia", &TLSOptions{}},
		{"CA equivocada: la ajena no firmó el certificado", &TLSOptions{CAFile: env.PKI.OtherCA}},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			o := baseOpts(env.URL)
			o.Credentials, o.TLS = creds, c.tls
			_, err, dur := connectT(t, o, 15*time.Second)
			cr := quiereRechazo(t, err, dur, RejectionTLS)
			if !strings.Contains(cr.Error(), "TLS.CAFile") {
				t.Errorf("debe apuntar a la CA: %v", err)
			}
		})
	}
}

func TestH_HostFueraDeLosSANEsConnectionRejectedTls(t *testing.T) {
	b := newSimpleBroker(t, "tls-san")
	b.Start()
	o := baseOpts(b.URL)
	o.TLS = &TLSOptions{CAFile: b.PKI.CA}
	_, err, dur := connectT(t, o, 15*time.Second)
	cr := quiereRechazo(t, err, dur, RejectionTLS)
	if !strings.Contains(cr.Error(), "SAN") {
		t.Errorf("debe mencionar los SAN: %v", err)
	}
}

func TestH_TLSExigidoSinTLSEnElBrokerEsConnectionRejectedTlsYNoBajaATextoPlano(t *testing.T) {
	b := newSimpleBroker(t, "plain")
	b.Start()
	o := baseOpts(b.URL)
	o.TLS = &TLSOptions{}
	_, err, dur := connectT(t, o, 15*time.Second)
	cr := quiereRechazo(t, err, dur, RejectionTLS)
	if !strings.Contains(cr.Error(), "no ofrece TLS") || !strings.Contains(cr.Error(), "texto plano") {
		t.Errorf("debe decir que el servidor no ofrece TLS y que el SDK no baja a texto plano: %v", err)
	}
	// El otro sentido: sin `TLS` el mismo broker sí conecta (es el desarrollo local sin TLS).
	o2 := baseOpts(b.URL)
	if _, err, _ := connectT(t, o2, 15*time.Second); err != nil {
		t.Fatalf("sin `TLS` el broker sin TLS debe conectar: %v", err)
	}
}

func TestH_BrokerQueExigeCertificadoDeClienteYNoSePresentaEsConnectionRejectedTls(t *testing.T) {
	b := newSimpleBroker(t, "mtls")
	b.Start()
	o := baseOpts(b.URL)
	o.TLS = &TLSOptions{CAFile: b.PKI.CA}
	_, err, dur := connectT(t, o, 15*time.Second)
	cr := quiereRechazo(t, err, dur, RejectionTLS)
	if !strings.Contains(cr.Error(), "TLS.CertFile") {
		t.Errorf("debe apuntar a TLS.CertFile/KeyFile: %v", err)
	}

	// Con un certificado de cliente que la CA reconoce, conecta (mTLS funciona).
	o.TLS = &TLSOptions{CAFile: b.PKI.CA, CertFile: b.PKI.ClientCert, KeyFile: b.PKI.ClientKey}
	if _, err, _ := connectT(t, o, 15*time.Second); err != nil {
		t.Fatalf("con certificado de cliente válido debe conectar: %v", err)
	}
	// Y con uno que firmó OTRA CA, el broker lo rechaza: CONNECTION_REJECTED, no INVALID_TLS_OPTIONS.
	o.TLS = &TLSOptions{CAFile: b.PKI.CA, CertFile: b.PKI.OtherClientCert, KeyFile: b.PKI.OtherClientKey}
	_, err, dur = connectT(t, o, 15*time.Second)
	quiereRechazo(t, err, dur, RejectionTLS)
}

func TestH_UsuarioYContrasenaYTokenDelBroker(t *testing.T) {
	t.Run("usuario y contraseña", func(t *testing.T) {
		b := newSimpleBroker(t, "tls-userpass")
		b.Start()
		o := baseOpts(b.URL)
		o.TLS = &TLSOptions{CAFile: b.PKI.CA}
		o.Credentials = CredentialsFromUserPass("svc", "secreto")
		if _, err, _ := connectT(t, o, 15*time.Second); err != nil {
			t.Fatalf("la contraseña buena no conectó: %v", err)
		}
		o.Credentials = CredentialsFromUserPass("svc", "otra")
		_, err, dur := connectT(t, o, 15*time.Second)
		cr := quiereRechazo(t, err, dur, RejectionAuth)
		if strings.Contains(cr.Error(), "otra") && strings.Contains(cr.Error(), "contraseña otra") {
			t.Errorf("la contraseña no debe aparecer: %v", err)
		}
	})
	t.Run("token", func(t *testing.T) {
		b := newSimpleBroker(t, "tls-token")
		b.Start()
		o := baseOpts(b.URL)
		o.TLS = &TLSOptions{CAFile: b.PKI.CA}
		o.Credentials = CredentialsFromToken("tk-secreto")
		if _, err, _ := connectT(t, o, 15*time.Second); err != nil {
			t.Fatalf("el token bueno no conectó: %v", err)
		}
		o.Credentials = CredentialsFromToken("tk-equivocado")
		_, err, dur := connectT(t, o, 15*time.Second)
		quiereRechazo(t, err, dur, RejectionAuth)
	})
}

func TestH_CredsDeVerdadConUnOperadorYsuJWT(t *testing.T) {
	b := newSimpleBroker(t, "jwt")
	b.Start()
	dir := t.TempDir()
	buena := filepath.Join(dir, "buena.creds")
	ajena := filepath.Join(dir, "semilla-ajena.creds")
	for ruta, c := range map[string]string{buena: b.JWT.Creds, ajena: b.JWT.CredsForeignSeed} {
		if err := os.WriteFile(ruta, []byte(c), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	o := baseOpts(b.URL)
	o.TLS = &TLSOptions{CAFile: b.PKI.CA}

	// `.creds` por ruta, y por contenido (un gestor de secretos): conectan.
	o.Credentials = CredentialsFromFile(buena)
	if _, err, _ := connectT(t, o, 15*time.Second); err != nil {
		t.Fatalf("un .creds válido (por ruta) no conectó: %v", err)
	}
	o.Credentials = CredentialsFromContent(b.JWT.Creds)
	if _, err, _ := connectT(t, o, 15*time.Second); err != nil {
		t.Fatalf("un .creds válido (por contenido) no conectó: %v", err)
	}
	// Bien formado (JWT y semilla de usuario) pero la semilla no es la del JWT: el broker lo rechaza.
	o.Credentials = CredentialsFromFile(ajena)
	_, err, dur := connectT(t, o, 15*time.Second)
	quiereRechazo(t, err, dur, RejectionAuth)
}

func TestH_UnBrokerQueAunNoHaArrancadoSeEspera_YNoEsNingunoDeLosTresCodigos(t *testing.T) {
	b := newSimpleBroker(t, "plain")
	o := baseOpts(b.URL)
	resultado := make(chan error, 1)
	var bus *Bus
	go func() {
		var err error
		bus, err, _ = connectT(t, o, 30*time.Second)
		resultado <- err
	}()
	select {
	case err := <-resultado:
		t.Fatalf("Connect() volvió con el broker caído (%v): debe ESPERAR", err)
	case <-time.After(1500 * time.Millisecond):
	}
	b.Start()
	select {
	case err := <-resultado:
		if err != nil {
			t.Fatalf("al arrancar el broker, Connect() debía conectar: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Connect() no conectó tras arrancar el broker")
	}
	if bus == nil || !bus.Connected() {
		t.Fatal("el bus no quedó conectado")
	}
}

func TestH_SiElBrokerNuncaArrancaElContextoCortaLaEsperaSinCodigoDeSeguridad(t *testing.T) {
	o := baseOpts("nats://127.0.0.1:" + itoa(silentPort(t)))
	_, err, dur := connectT(t, o, 1500*time.Millisecond)
	if err == nil {
		t.Fatal("no debía conectar")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, se esperaba el plazo del contexto", err)
	}
	var cr *ConnectionRejectedError
	var ic *InvalidCredentialsError
	var it *InvalidTLSOptionsError
	if errors.As(err, &cr) || errors.As(err, &ic) || errors.As(err, &it) {
		t.Errorf("un broker caído NO es ninguno de los tres códigos: %v", err)
	}
	if dur > 4*time.Second {
		t.Errorf("tardó %v", dur)
	}
}

func TestH_NombreDeServicioInvalidoNoLlevaNingunCodigoDeConnect(t *testing.T) {
	o := baseOpts("nats://127.0.0.1:" + itoa(silentPort(t)))
	o.Service = "Pedidos.Api"
	_, err, dur := connectT(t, o, 5*time.Second)
	var isn *InvalidServiceNameError
	if !errors.As(err, &isn) {
		t.Fatalf("err = %v, se esperaba *InvalidServiceNameError", err)
	}
	if _, ok := AsClassified(err); ok {
		t.Errorf("un nombre de servicio inválido NO debe llevar ninguno de los códigos estables: %v", err)
	}
	if dur > time.Second {
		t.Errorf("debe fallar sin esperar (%v)", dur)
	}
}

func itoa(n int) string {
	var b [20]byte
	i := len(b)
	for n > 0 || i == len(b) {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
