// Aislamiento entre tenants — Modelo A.
// Contrato normativo: specification/09-multitenancy.md §3
//
// Vive en su propio fichero porque es la regla de la que depende que un consumidor no
// vea datos ajenos, y conviene poder leerla entera de una vez.
//
// Recordatorio de lo que el Modelo A NO cubre (§1): un servicio legítimo comprometido
// puede publicar con el tenantid de otro, y un consumidor comprometido puede leer el
// subject entero. El filtro del SDK evita ERRORES, no adversarios; para eso está el
// Modelo B, una account de NATS por tenant.

package flux

import "fmt"

// TenantIsolation es la política de aislamiento de la conexión.
type TenantIsolation string

const (
	// TenantIsolationOff es el default: se filtra si hay tenant configurado, pero
	// olvidarlo no rompe nada. El cero del tipo ("") se interpreta como éste.
	TenantIsolationOff TenantIsolation = "off"

	// TenantIsolationStrict exige que toda suscripción filtre por tenant. Suscribirse
	// sin tenant configurado devuelve un error de configuración.
	TenantIsolationStrict TenantIsolation = "strict"
)

// TenantIsolationError se devuelve al suscribirse sin filtro de tenant con
// TenantIsolationStrict.
//
// Es un error de arranque a propósito. El fallo que evita —un consumidor que ve los
// eventos de TODOS los tenants— no produce ninguna señal en tiempo de ejecución: no hay
// error, no hay log, no hay métrica. Solo hay un incidente de privacidad que alguien
// descubre semanas después (09-multitenancy.md §3).
type TenantIsolationError struct {
	Subject string
	Reason  string
}

func (e *TenantIsolationError) Error() string {
	return fmt.Sprintf(
		"flux: TenantIsolation=\"strict\" pero %s al suscribirse a %q. Sin filtro de tenant, "+
			"este consumidor vería los eventos de TODOS los tenants y eso no produce ningún "+
			"error visible (09-multitenancy.md §3).", e.Reason, e.Subject)
}

// resolveTenantFilter devuelve el filtro efectivo de una suscripción, o "" si no hay.
//
// El de la suscripción gana sobre el de la conexión: un servicio multi-tenant puede
// tener una conexión sin tenant y una suscripción por cada uno.
//
// "system" NO cuenta como filtro: es la ausencia de tenant, no un tenant
// (09-multitenancy.md §5). Aceptarlo dejaría fuera todos los eventos de negocio y —peor—
// daría por satisfecho el modo estricto sin filtrar nada.
func resolveTenantFilter(subject, connTenant, subTenant string, isolation TenantIsolation) (string, error) {
	for _, candidato := range []string{subTenant, connTenant} {
		if candidato != "" && candidato != "system" {
			return candidato, nil
		}
	}
	if isolation == TenantIsolationStrict {
		return "", &TenantIsolationError{
			Subject: subject,
			Reason: `no hay TenantID ni en ConnectOptions ni en WithTenantFilter ` +
				`(o vale "system", que es la ausencia de tenant)`,
		}
	}
	return "", nil
}
