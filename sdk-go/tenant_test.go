package flux

import (
	"errors"
	"strings"
	"testing"
)

// La regla que se prueba aquí es la única que, al fallar, no produce ningún error: un
// consumidor sin filtro ve los eventos de todos los tenants y todo parece funcionar. De
// ahí que resolveTenantFilter sea una función suelta y no una rama enterrada en
// Subscribe — 09-multitenancy.md §3.

const subjectTenant = "pedidos.pedido.v1.creado"

func TestResolveTenantFilter(t *testing.T) {
	casos := []struct {
		nombre    string
		conn      string
		sub       string
		isolation TenantIsolation
		want      string
	}{
		{"el de la suscripción gana sobre el de la conexión", "acme", "globex", TenantIsolationOff, "globex"},
		{"sin filtro en la suscripción manda el de la conexión", "acme", "", TenantIsolationOff, "acme"},
		{"sin tenant en ningún sitio no hay filtro", "", "", TenantIsolationOff, ""},
		// "system" es la AUSENCIA de tenant, no un tenant: usarlo como filtro dejaría
		// fuera todos los eventos de negocio — 09-multitenancy.md §5.
		{"system no cuenta como filtro", "system", "", TenantIsolationOff, ""},
		{"system en la suscripción cae al de la conexión", "acme", "system", TenantIsolationOff, "acme"},
		{"con tenant, el modo estricto no estorba", "acme", "", TenantIsolationStrict, "acme"},
		{"con tenant solo en la suscripción, el modo estricto no estorba", "", "globex", TenantIsolationStrict, "globex"},
	}

	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			got, err := resolveTenantFilter(subjectTenant, c.conn, c.sub, c.isolation)
			if err != nil {
				t.Fatalf("error inesperado: %v", err)
			}
			if got != c.want {
				t.Errorf("filtro = %q, se esperaba %q", got, c.want)
			}
		})
	}
}

func TestModoEstrictoSinTenantEsError(t *testing.T) {
	// Un filtro que hay que acordarse de poner es un filtro que alguien olvidará, y el
	// fallo no produce ningún error: produce un incidente de privacidad que se descubre
	// semanas después — 09-multitenancy.md §3.
	for _, conn := range []string{"", "system"} {
		_, err := resolveTenantFilter(subjectTenant, conn, "", TenantIsolationStrict)

		var te *TenantIsolationError
		if !errors.As(err, &te) {
			t.Fatalf("TenantID=%q: err = %v, se esperaba *TenantIsolationError", conn, err)
		}
		if te.Subject != subjectTenant {
			t.Errorf("Subject = %q, se esperaba %q", te.Subject, subjectTenant)
		}
		if !strings.Contains(err.Error(), "TODOS los tenants") {
			t.Errorf("el mensaje no explica la consecuencia: %v", err)
		}
	}
}

func TestElCeroDeTenantIsolationEsOff(t *testing.T) {
	// El cero de un string es "" y tiene que significar el default del protocolo: si
	// significara "strict", añadir el campo rompería a todo consumidor existente.
	var isolation TenantIsolation
	if _, err := resolveTenantFilter(subjectTenant, "", "", isolation); err != nil {
		t.Errorf("el cero de TenantIsolation no se comporta como off: %v", err)
	}
}

func TestEventoDeOtroTenantSeDescarta(t *testing.T) {
	// La decisión que toma dispatch antes de invocar al handler, aislada: un evento de
	// otro tenant no es un fallo, es correo de otro. Se ackea y se descarta.
	filtro, err := resolveTenantFilter(subjectTenant, "acme", "", TenantIsolationStrict)
	if err != nil {
		t.Fatal(err)
	}

	in := entradaValida()
	in.TenantID = "globex"
	ajeno, err := BuildEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	if ajeno.TenantID == filtro {
		t.Fatal("el caso de prueba no representa a otro tenant")
	}

	in.TenantID = "acme"
	propio, err := BuildEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	if propio.TenantID != filtro {
		t.Errorf("el evento propio debería pasar el filtro %q", filtro)
	}
}

func TestSystemEsLegitimoComoTenantDeUnEvento(t *testing.T) {
	// Es un valor válido para eventos de plataforma. Lo que NO es: un comodín ni un
	// default cuando el tenant real se desconoce — si no se sabe de quién es un evento,
	// el bug está aguas arriba (09-multitenancy.md §5).
	in := entradaValida()
	in.TenantID = "system"
	e, err := BuildEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	if e.TenantID != "system" {
		t.Errorf("tenantid = %q", e.TenantID)
	}
}

func TestTenantVacioNoConstruyeEvento(t *testing.T) {
	// En Go el cero de un string es "" y es indistinguible de "ausente", así que la
	// extensión obligatoria se comprueba en tiempo de ejecución — 01-envelope.md §3.1.
	in := entradaValida()
	in.TenantID = ""
	if _, err := BuildEvent(in); err == nil {
		t.Error("BuildEvent aceptó un tenantid vacío")
	}
}
