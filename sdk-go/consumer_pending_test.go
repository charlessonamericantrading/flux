package flux

// flux_consumer_pending — specification/08-observability.md §2.3.
//
// La métrica tiene DOS fuentes y el SDK DEBE usar las dos, porque cada una falla justo
// donde la otra sirve:
//
//   - los metadatos del mensaje entregado son gratis y frescos, pero si el bucle del
//     consumidor muere dejan de llegar mensajes y el gauge se queda plano en su último
//     valor — un panel mostraría una línea horizontal, indistinguible de "no pasa nada";
//   - el sondeo al servidor cuesta una petición cada ~15 s y sigue creciendo. Es la señal.
//
// El sondeo se prueba sin broker porque pollPending recibe sus dependencias como
// funciones: que un fallo NO mate el bucle solo se demuestra haciéndolo fallar.

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

const tiempoDeGracia = 2 * time.Second

// enviar no bloquea nunca: si el test ya tiene lo que buscaba, el sondeo debe poder
// seguir hasta que se le diga que pare.
func enviar[T any](c chan T, v T) {
	select {
	case c <- v:
	default:
	}
}

func TestElSondeoEmiteElNumPendingDelServidor(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)
	reportado := make(chan uint64, 1)

	go pollPending(time.Millisecond, stop,
		func() (uint64, error) { return 42, nil },
		func(p uint64) { enviar(reportado, p) },
		func(err error) { t.Errorf("no debía fallar: %v", err) })

	select {
	case p := <-reportado:
		if p != 42 {
			t.Errorf("pending = %d, se esperaba 42", p)
		}
	case <-time.After(tiempoDeGracia):
		t.Fatal("el sondeo no emitió nada")
	}
}

func TestUnFalloDelSondeoNoMataElBucle(t *testing.T) {
	// Un fallo del sondeo NO DEBE afectar al consumo: es telemetría. Si el primer error
	// terminara la goroutine, la métrica se apagaría para siempre tras un hipo del
	// broker — y nadie se enteraría, porque el consumidor seguiría consumiendo.
	stop := make(chan struct{})
	defer close(stop)
	reportado := make(chan uint64, 1)
	fallos := make(chan error, 1)

	var llamadas atomic.Int32
	go pollPending(time.Millisecond, stop,
		func() (uint64, error) {
			if llamadas.Add(1) == 1 {
				return 0, errors.New("broker no disponible")
			}
			return 9, nil
		},
		func(p uint64) { enviar(reportado, p) },
		func(err error) { enviar(fallos, err) })

	select {
	case p := <-reportado:
		if p != 9 {
			t.Errorf("pending = %d, se esperaba 9", p)
		}
	case <-time.After(tiempoDeGracia):
		t.Fatal("el sondeo murió con el primer error")
	}
	select {
	case <-fallos:
	default:
		t.Error("el fallo no se reportó a onErr")
	}
}

func TestElSondeoParaAlDesuscribir(t *testing.T) {
	stop := make(chan struct{})
	terminado := make(chan struct{})

	// Intervalo largo a propósito: parar no puede depender de que llegue el siguiente
	// tick, o Close() se quedaría esperando hasta 15 s por consumidor.
	go func() {
		pollPending(time.Hour, stop,
			func() (uint64, error) { return 0, nil },
			func(uint64) {}, func(error) {})
		close(terminado)
	}()

	close(stop)
	select {
	case <-terminado:
	case <-time.After(tiempoDeGracia):
		t.Fatal("el sondeo no paró al cerrar stop")
	}
}

// ─── El intervalo ────────────────────────────────────────────────────────────

func busConIntervalo(interval time.Duration, sink MetricsSink) *Bus {
	return &Bus{opts: ConnectOptions{PendingPollInterval: interval}, metrics: sink}
}

func TestElIntervaloDeSondeo(t *testing.T) {
	casos := []struct {
		nombre   string
		interval time.Duration
		sink     MetricsSink
		esperado time.Duration
	}{
		// ⚠️ El cero es "no configurado", NO "desactivado" — divergencia deliberada con
		// Node y Python, donde 0 apaga. Si el cero apagara, la métrica quedaría muda en
		// todo servicio que no conozca el campo: justo los que nadie vigila.
		{"el cero es el default", 0, NewInMemoryMetrics(), DefaultPendingPoll},
		{"un valor explícito manda", 3 * time.Second, NewInMemoryMetrics(), 3 * time.Second},
		{"negativo lo desactiva", PendingPollDisabled, NewInMemoryMetrics(), 0},
		// Sería una petición cada 15 s por consumidor para tirar el resultado.
		{"sin sumidero no se sondea", 0, NoMetrics{}, 0},
		{"sin sumidero tampoco con intervalo explícito", time.Second, NoMetrics{}, 0},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			if got := busConIntervalo(c.interval, c.sink).pendingPollInterval(); got != c.esperado {
				t.Errorf("intervalo = %v, se esperaba %v", got, c.esperado)
			}
		})
	}
}

func TestElIntervaloPorDefectoEsElDeProtocolJSON(t *testing.T) {
	metricas := observabilidad(t)["metrics"].(map[string]any)
	pendiente := metricas["flux_consumer_pending"].(map[string]any)

	ms, ok := pendiente["defaultPollMs"].(float64)
	if !ok {
		t.Fatal("protocol.json no declara defaultPollMs para flux_consumer_pending")
	}
	if esperado := time.Duration(ms) * time.Millisecond; DefaultPendingPoll != esperado {
		t.Errorf("DefaultPendingPoll = %v, protocol.json dice %v", DefaultPendingPoll, esperado)
	}
	// Si alguien "optimizara" el sondeo dejando solo los metadatos, este test recuerda
	// que el contrato dice de dónde sale el dato.
	if fuente := pendiente["source"]; fuente != "polled-from-server" {
		t.Errorf("source = %v, se esperaba polled-from-server", fuente)
	}
}
