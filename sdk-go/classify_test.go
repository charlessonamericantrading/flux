package flux

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"
)

func TestClassifyRespetaLosErroresTipados(t *testing.T) {
	// Un error tipado de flux siempre gana: la aplicación conoce sus dependencias,
	// el SDK no — 04-errors.md §2.
	casos := []struct {
		err   error
		class ErrorClass
		code  string
	}{
		{NewRetryableError("proveedor 503", WithCode("PROVEEDOR_CAIDO")), ClassRetryable, "PROVEEDOR_CAIDO"},
		{NewPermanentError("pedido ya cancelado", WithCode("PEDIDO_YA_CANCELADO")), ClassPermanent, "PEDIDO_YA_CANCELADO"},
		{NewPoisonError("envelope roto", WithCode("MALFORMED_JSON")), ClassPoison, "MALFORMED_JSON"},
		// Sin código explícito se cae al nombre del tipo: nunca una métrica sin etiqueta.
		{NewRetryableError("sin código"), ClassRetryable, "RetryableError"},
	}
	for _, c := range casos {
		got := Classify(c.err)
		if got.Class != c.class || got.Code != c.code {
			t.Errorf("Classify(%v) = %+v, se esperaba clase %q código %q",
				c.err, got, c.class, c.code)
		}
	}
}

func TestClassifyPropagaRetryAfter(t *testing.T) {
	err := NewRetryableError("429", WithRetryAfter(5*time.Second))
	if got := Classify(err); got.RetryAfter != 5*time.Second {
		t.Errorf("RetryAfter = %s, se esperaba 5s", got.RetryAfter)
	}
	// Un PERMANENT no tiene reintento del que hablar.
	if got := Classify(NewPermanentError("x", WithRetryAfter(5*time.Second))); got.RetryAfter != 0 {
		t.Errorf("un PERMANENT no debe llevar RetryAfter, se obtuvo %s", got.RetryAfter)
	}
}

func TestClassifyAtraviesaErroresEnvueltos(t *testing.T) {
	// Mejora sobre el `instanceof` de Node: errors.As recorre la cadena, así que un
	// error tipado envuelto por una capa intermedia sigue clasificándose bien.
	base := NewRetryableError("timeout del pool", WithCode("POOL_AGOTADO"))
	envuelto := fmt.Errorf("al facturar el pedido ped-123: %w", base)
	got := Classify(envuelto)
	if got.Class != ClassRetryable || got.Code != "POOL_AGOTADO" {
		t.Errorf("Classify de un error envuelto = %+v", got)
	}
}

func TestClassifyStatusHTTP(t *testing.T) {
	// 429/502/503/504 → RETRYABLE. 400/403/404/422 → PERMANENT: reintentarlos es
	// gastar 51 minutos para obtener la misma respuesta — 04-errors.md §1.
	retryables := []int{429, 502, 503, 504}
	permanentes := []int{400, 401, 403, 404, 409, 422, 500, 501}

	for _, s := range retryables {
		got := Classify(NewHTTPError(s, "dependencia", 0))
		if got.Class != ClassRetryable {
			t.Errorf("HTTP %d debería ser RETRYABLE, se obtuvo %q", s, got.Class)
		}
		if want := fmt.Sprintf("HTTP_%d", s); got.Code != want {
			t.Errorf("code = %q, se esperaba %q", got.Code, want)
		}
	}
	for _, s := range permanentes {
		if got := Classify(NewHTTPError(s, "dependencia", 0)); got.Class != ClassPermanent {
			t.Errorf("HTTP %d debería ser PERMANENT, se obtuvo %q", s, got.Class)
		}
	}
}

func TestClassifyLeeRetryAfterDeLaDependencia(t *testing.T) {
	got := Classify(NewHTTPError(429, "rate limit", 12*time.Second))
	if got.Class != ClassRetryable || got.RetryAfter != 12*time.Second {
		t.Errorf("no se respetó el Retry-After anunciado: %+v", got)
	}
	// Un 503 sin Retry-After cae al backoff canónico (RetryAfter cero).
	if got := Classify(NewHTTPError(503, "", 0)); got.RetryAfter != 0 {
		t.Errorf("sin Retry-After debe quedar en cero, se obtuvo %s", got.RetryAfter)
	}
}

func TestClassifyErroresDeRed(t *testing.T) {
	// Red y DNS son transitorios por definición. El errno viaja tipado dentro de
	// *net.OpError, así que errors.Is lo encuentra sin mirar cadenas de texto.
	casos := map[string]error{
		"ECONNRESET":   &net.OpError{Op: "read", Err: syscall.ECONNRESET},
		"ECONNREFUSED": &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED},
		"EPIPE":        &net.OpError{Op: "write", Err: syscall.EPIPE},
		"EHOSTUNREACH": &net.OpError{Op: "dial", Err: syscall.EHOSTUNREACH},
		"ENETUNREACH":  &net.OpError{Op: "dial", Err: syscall.ENETUNREACH},
	}
	for code, err := range casos {
		got := Classify(err)
		if got.Class != ClassRetryable {
			t.Errorf("%s debería ser RETRYABLE, se obtuvo %q", code, got.Class)
		}
		if got.Code != code {
			t.Errorf("code = %q, se esperaba %q", got.Code, code)
		}
	}
}

func TestClassifyTimeoutsSegunPolitica(t *testing.T) {
	// El default es RETRYABLE: un timeout suele indicar saturación transitoria.
	timeouts := []error{
		context.DeadlineExceeded,
		&net.DNSError{Err: "server misbehaving", IsTemporary: true}, // el EAI_AGAIN de la spec
		&net.OpError{Op: "read", Err: &net.DNSError{Err: "timeout", IsTimeout: true}},
	}
	for _, err := range timeouts {
		if got := Classify(err); got.Class != ClassRetryable || got.Code != "TIMEOUT" {
			t.Errorf("Classify(%v) = %+v, se esperaba RETRYABLE/TIMEOUT", err, got)
		}
	}

	// Si vuestros timeouts son consultas que nunca van a terminar, PERMANENT evita
	// reintentar lo imposible.
	estricto := NewClassifier(ClassifierOptions{TimeoutPolicy: ClassPermanent})
	if got := estricto(context.DeadlineExceeded); got.Class != ClassPermanent {
		t.Errorf("con TimeoutPolicy=permanent se esperaba PERMANENT, se obtuvo %q", got.Class)
	}
}

func TestClassifyDefaultDeLoDesconocidoEsRetryableAcotado(t *testing.T) {
	// LA decisión de política del protocolo. Domina a las dos alternativas: el
	// transitorio se recupera en el 2º intento, el sistemático llega a la DLQ en ~30 s
	// sin atascar la cola — 04-errors.md §2.1.
	desconocido := errors.New("algo que nadie previó")

	got := Classify(desconocido)
	if got.Class != ClassRetryable || got.Code != "UNKNOWN" {
		t.Errorf("el default debe ser RETRYABLE/UNKNOWN, se obtuvo %+v", got)
	}
	if DefaultUnknownRetryBudget != 2 {
		t.Errorf("el presupuesto de la spec es 2 entregas, no %d", DefaultUnknownRetryBudget)
	}
	if got.MaxAttempts != DefaultUnknownRetryBudget {
		t.Errorf("MaxAttempts = %d, se esperaba %d", got.MaxAttempts, DefaultUnknownRetryBudget)
	}
}

func TestElPresupuestoAcotadoNoRecortaLosRetryableReconocidos(t *testing.T) {
	// Un ECONNRESET o un 503 conservan sus 6 intentos: el presupuesto es por error, no
	// por consumidor. Bajarlo en max_deliver los habría recortado a todos.
	reconocidos := []error{
		NewHTTPError(503, "dependencia", 0),
		&net.OpError{Op: "read", Err: syscall.ECONNRESET},
		NewRetryableError("proveedor caído", WithCode("PROVEEDOR_CAIDO")),
	}
	for _, err := range reconocidos {
		if got := Classify(err); got.MaxAttempts != 0 {
			t.Errorf("Classify(%v).MaxAttempts = %d, se esperaba 0 (sin tope propio)",
				err, got.MaxAttempts)
		}
	}
}

func TestClassifyPresupuestoDeLoDesconocidoConfigurable(t *testing.T) {
	generoso := NewClassifier(ClassifierOptions{UnknownRetryBudget: 3})
	if got := generoso(errors.New("x")); got.MaxAttempts != 3 {
		t.Errorf("MaxAttempts = %d, se esperaba 3", got.MaxAttempts)
	}
}

func TestClassifyPoliticasDeLoDesconocido(t *testing.T) {
	// Configurable, porque es política y no infraestructura: el equilibrio correcto
	// depende de cómo fallen las dependencias de cada ecosistema — 04-errors.md §2.1.
	desconocido := errors.New("algo que nadie previó")

	// permanent: a la DLQ sin gastar ningún reintento.
	estricto := NewClassifier(ClassifierOptions{UnknownErrorPolicy: UnknownPermanent})
	if got := estricto(desconocido); got.Class != ClassPermanent || got.MaxAttempts != 0 {
		t.Errorf("con UnknownPermanent se esperaba PERMANENT sin tope, se obtuvo %+v", got)
	}

	// retryable: backoff completo, sin tope propio — manda el max_deliver del consumidor.
	tolerante := NewClassifier(ClassifierOptions{UnknownErrorPolicy: UnknownRetryable})
	if got := tolerante(desconocido); got.Class != ClassRetryable || got.MaxAttempts != 0 {
		t.Errorf("con UnknownRetryable se esperaba RETRYABLE sin tope, se obtuvo %+v", got)
	}

	// retryable-bounded explícito == el default.
	acotado := NewClassifier(ClassifierOptions{UnknownErrorPolicy: UnknownRetryableBounded})
	if got := acotado(desconocido); got.Class != ClassRetryable || got.MaxAttempts != 2 {
		t.Errorf("con UnknownRetryableBounded se esperaba RETRYABLE/2, se obtuvo %+v", got)
	}
}

func TestClassifyReglasDeLaAplicacion(t *testing.T) {
	// Las reglas van después de los errores tipados y antes de todo lo demás.
	var errDeadlock = errors.New("deadlock detected")

	c := NewClassifier(ClassifierOptions{
		Rules: []Rule{
			func(err error) (Classification, bool) {
				if errors.Is(err, errDeadlock) {
					return Classification{Class: ClassRetryable, Code: "DB_DEADLOCK",
						RetryAfter: 2 * time.Second}, true
				}
				return Classification{}, false
			},
		},
	})

	got := c(fmt.Errorf("al escribir la factura: %w", errDeadlock))
	if got.Class != ClassRetryable || got.Code != "DB_DEADLOCK" || got.RetryAfter != 2*time.Second {
		t.Errorf("la regla de la aplicación no se aplicó: %+v", got)
	}

	// Una regla NO puede sobrescribir un error tipado de flux.
	got = c(NewPermanentError("rechazado", WithCode("RECHAZADO"), WithCause(errDeadlock)))
	if got.Class != ClassPermanent {
		t.Errorf("un error tipado debe ganar a las reglas, se obtuvo %q", got.Class)
	}

	// Una regla que cede deja pasar al resto de la cadena.
	if got := c(NewHTTPError(503, "", 0)); got.Class != ClassRetryable || got.Code != "HTTP_503" {
		t.Errorf("la cadena no continuó tras una regla que cedió: %+v", got)
	}
}

func TestClassifierNoDependeDeUnSliceCompartido(t *testing.T) {
	// La política no debe poder cambiar bajo los pies del consumidor en marcha.
	rules := []Rule{func(error) (Classification, bool) { return Classification{}, false }}
	c := NewClassifier(ClassifierOptions{Rules: rules})
	rules[0] = func(error) (Classification, bool) {
		return Classification{Class: ClassRetryable, Code: "INYECTADA"}, true
	}
	if got := c(errors.New("x")); got.Code == "INYECTADA" {
		t.Fatal("mutar el slice de reglas alteró un clasificador ya construido")
	}
}

func TestAsClassified(t *testing.T) {
	if _, ok := AsClassified(errors.New("plano")); ok {
		t.Error("un error plano no declara clase")
	}
	envuelto := fmt.Errorf("capa 1: %w", fmt.Errorf("capa 2: %w",
		NewPoisonError("roto", WithCode("MALFORMED_JSON"))))
	ce, ok := AsClassified(envuelto)
	if !ok {
		t.Fatal("AsClassified debe atravesar errores envueltos")
	}
	if ce.FluxClass() != ClassPoison || ce.FluxCode() != "MALFORMED_JSON" {
		t.Errorf("clase/código incorrectos: %q/%q", ce.FluxClass(), ce.FluxCode())
	}
	if !IsClassified(envuelto) {
		t.Error("IsClassified debe coincidir con AsClassified")
	}
}

func TestErroresExponenSuCausa(t *testing.T) {
	base := errors.New("connection refused")
	for _, err := range []error{
		NewRetryableError("fallo de red", WithCause(base)),
		NewPermanentError("rechazo", WithCause(base)),
		NewPoisonError("ininterpretable", WithCause(base)),
	} {
		if !errors.Is(err, base) {
			t.Errorf("%T no expone su causa a errors.Is", err)
		}
	}
}
