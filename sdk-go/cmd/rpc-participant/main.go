// El participante del SDK de GO en los escenarios de petición/respuesta con SDKs reales
// (`sdk_script` de `conformance/security/interprete.mjs`: los casos de `conformance/cases/interop-rpc.json`).
//
// No es un test: es un proceso que un orquestador lanza y gobierna por STDIN, con UNA línea JSON por orden, y que contesta por
// STDOUT con UNA línea `{"id":n,…}` por orden. Habla con el broker SOLO por la API pública del SDK (`Connect`, `Bus.Request`,
// `Bus.Respond`, `Bus.Subscribe`, `State().Watch`, `Bus.Publish`): lo que el SDK haga por debajo —sus suscripciones, sus
// permisos, sus tiempos— es justo lo que el escenario mide. Los otros participantes, `conformance/security/participante-node.mjs`
// y `sdk-rust/examples/rpc_participant.rs`, hablan el MISMO protocolo con sus SDKs: el orquestador no sabe cuál es cuál.
//
// Entrada (todo lo que un servicio real recibiría de su despliegue)
//
//	FLUX_P_URL      `tls://127.0.0.1:<puerto>`
//	FLUX_P_CA       CA que firma el certificado del broker
//	FLUX_P_SERVICE  el nombre del servicio (su identidad y su inbox privado `_INBOX.<servicio>`)
//	FLUX_P_SEED     su semilla nkey
//	FLUX_P_TENANT   el tenant de `Connect` (por defecto `acme`)
//	FLUX_P_LABEL    cómo se llama esta INSTANCIA en `instancia` de las respuestas (por defecto, el servicio)
//	FLUX_P_SIGNING  JSON opcional: `{ keyId, privateKey, publicKeys: {id: PEM}, verify: off|warn|require }`
//
// El SDK NO se parchea aquí ni se «ayuda»: si un permiso falta o un código no sale, el participante lo cuenta tal cual. No hay
// «tramposo» (`FLUX_P_TRAMPOSO`, A-104): con la API pública no se puede contestar dos veces a una petición, y colocarse por debajo de
// `nats.go` sería parchear el SDK; los escenarios que lo usan fijan su SDK (node).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	flux "github.com/charlessonamericantrading/flux/sdk-go"
)

func env(nombre, porDefecto string) string {
	if v := os.Getenv(nombre); v != "" {
		return v
	}
	if porDefecto == "" {
		fmt.Fprintf(os.Stderr, "falta la variable de entorno %s\n", nombre)
		os.Exit(2)
	}
	return porDefecto
}

// ─── Lo que el SDK cuenta de sí mismo: su log ─────────────────────────────────

type registro struct {
	mu   sync.Mutex
	logs []string
}

func (r *registro) add(s string) {
	r.mu.Lock()
	r.logs = append(r.logs, s)
	r.mu.Unlock()
}

func (r *registro) todos() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.logs...)
}

type manejadorLog struct{ r *registro }

func (manejadorLog) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelWarn }
func (h manejadorLog) Handle(_ context.Context, rec slog.Record) error {
	nivel := "WARN"
	if rec.Level >= slog.LevelError {
		nivel = "ERROR"
	}
	h.r.add(nivel + " " + rec.Message)
	return nil
}
func (h manejadorLog) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h manejadorLog) WithGroup(string) slog.Handler      { return h }

// ─── Lo que se cuenta de un error ─────────────────────────────────────────────

func describirError(err error, ms *float64) map[string]any {
	clase := "desconocida"
	var code any
	if ce, ok := flux.AsClassified(err); ok {
		code = ce.FluxCode()
		clase = string(ce.FluxClass())
	}
	nombre := strings.TrimPrefix(fmt.Sprintf("%T", err), "*flux.")
	o := map[string]any{"name": nombre, "code": code, "class": clase, "message": err.Error()}
	var ra interface{ RetryAfter() time.Duration }
	if errors.As(err, &ra) && ra.RetryAfter() > 0 {
		o["retryAfterMs"] = ra.RetryAfter().Milliseconds()
	}
	if ms != nil {
		o["ms"] = *ms
	}
	return o
}

func ahora() float64 { return float64(time.Now().UnixNano()) / 1e6 }

// ─── Los comportamientos del responder ────────────────────────────────────────

type comportamiento struct {
	Kind         string `json:"kind"`
	AfterMs      int    `json:"afterMs"`
	Class        string `json:"class"`
	Code         string `json:"code"`
	Message      string `json:"message"`
	RetryAfterMs int64  `json:"retryAfterMs"`
	Text         string `json:"text"`
	Status       int    `json:"status"`
	Bytes        int    `json:"bytes"`
}

// errorHTTP es lo que el clasificador RECONOCE: un 503 (o el status que sea) de una dependencia, con el texto de por medio.
type errorHTTP struct {
	status int
	texto  string
}

func (e errorHTTP) Error() string   { return e.texto }
func (e errorHTTP) HTTPStatus() int { return e.status }

type participante struct {
	servicio, etiqueta string
	bus                *flux.Bus
	logs               *registro

	mu              sync.Mutex
	comportamientos map[string]comportamiento
	atendidas       map[string]int
	idsVistos       map[string][]string
	responders      map[string]*flux.Responder
	consumidos      atomic.Int64
	observados      atomic.Int64
}

func (p *participante) manejador(operacion string) flux.RPCHandler {
	return func(ctx context.Context, req flux.RPCRequest) (any, error) {
		p.mu.Lock()
		p.atendidas[operacion]++
		p.idsVistos[operacion] = append(p.idsVistos[operacion], req.ID)
		b, ok := p.comportamientos[operacion]
		p.mu.Unlock()
		if !ok {
			b = comportamiento{Kind: "echo"}
		}
		if b.AfterMs > 0 {
			time.Sleep(time.Duration(b.AfterMs) * time.Millisecond)
		}
		switch b.Kind {
		case "echo", "":
			r := map[string]any{
				"instancia": p.etiqueta, "tenant": req.TenantID, "correlationId": req.CorrelationID,
				"correlacionEsElId": req.CorrelationID == req.ID, "peticionId": req.ID, "eco": req.Data,
			}
			if req.AggregateID != "" {
				r["aggregateId"] = req.AggregateID
			}
			return r, nil
		case "error":
			msg := b.Message
			if b.Class == "retryable" {
				if msg == "" {
					msg = "el servicio de tarifas no responde"
				}
				opts := []flux.ErrorOption{}
				if b.Code != "" {
					opts = append(opts, flux.WithCode(b.Code))
				}
				if b.RetryAfterMs > 0 {
					opts = append(opts, flux.WithRetryAfter(time.Duration(b.RetryAfterMs)*time.Millisecond))
				}
				return nil, flux.NewRetryableError(msg, opts...)
			}
			if msg == "" {
				msg = "pedido ya cancelado"
			}
			opts := []flux.ErrorOption{}
			if b.Code != "" {
				opts = append(opts, flux.WithCode(b.Code))
			}
			return nil, flux.NewPermanentError(msg, opts...)
		case "unknown":
			// Una excepción que NO lanzó a propósito la aplicación: su texto puede llevar valores de `data`, rutas…
			return nil, errors.New(orDefault(b.Text, "SECRETO-DE-LA-EXCEPCION"))
		case "http":
			st := b.Status
			if st == 0 {
				st = 503
			}
			return nil, errorHTTP{status: st, texto: orDefault(b.Text, "SECRETO-DEL-PROVEEDOR")}
		case "econnreset":
			return nil, fmt.Errorf("%s: %w", orDefault(b.Text, "SECRETO-DEL-PROVEEDOR"), syscall.ECONNRESET)
		case "big":
			n := b.Bytes
			if n == 0 {
				n = 1_100_000
			}
			return map[string]any{"instancia": p.etiqueta, "relleno": strings.Repeat("x", n)}, nil
		case "no-object":
			return "esto no es un objeto", nil
		}
		return nil, fmt.Errorf("comportamiento desconocido «%s»", b.Kind)
	}
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

// ─── Las órdenes ──────────────────────────────────────────────────────────────

type opcionesLlamada struct {
	TimeoutMs      *int64  `json:"timeoutMs"`
	TenantID       *string `json:"tenantId"`
	AggregateID    *string `json:"aggregateId"`
	Classification *string `json:"classification"`
	Retries        *int    `json:"retries"`
}

func (o opcionesLlamada) aOpciones() []flux.RequestOption {
	var r []flux.RequestOption
	if o.TimeoutMs != nil {
		r = append(r, flux.WithRPCTimeout(time.Duration(*o.TimeoutMs)*time.Millisecond))
	}
	if o.TenantID != nil {
		r = append(r, flux.WithRPCTenant(*o.TenantID))
	}
	if o.AggregateID != nil {
		r = append(r, flux.WithRPCAggregateID(*o.AggregateID))
	}
	if o.Classification != nil {
		r = append(r, flux.WithRPCClassification(flux.DataClassification(*o.Classification)))
	}
	if o.Retries != nil {
		r = append(r, flux.WithRPCRetries(*o.Retries))
	}
	return r
}

type orden struct {
	ID        int64           `json:"id"`
	Cmd       string          `json:"cmd"`
	Operation string          `json:"operation"`
	Data      json.RawMessage `json:"data"`
	Options   json.RawMessage `json:"options"`
	Behavior  *comportamiento `json:"behavior"`
	Repeat    int             `json:"repeat"`
	N         int             `json:"n"`
	Conc      int             `json:"concurrency"`
	Subject   string          `json:"subject"`
	Contract  string          `json:"contract"`
	Key       string          `json:"key"`
	TenantID  *string         `json:"tenantId"`
}

type resultadoLlamada struct {
	ok     bool
	data   json.RawMessage
	err    map[string]any
	ms     float64
	codigo string
}

func (p *participante) llamar(operacion string, data json.RawMessage, opciones json.RawMessage) resultadoLlamada {
	var o opcionesLlamada
	if len(opciones) > 0 {
		_ = json.Unmarshal(opciones, &o)
	}
	if len(data) == 0 {
		data = json.RawMessage(`{}`)
	}
	t0 := ahora()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r, err := p.bus.Request(ctx, operacion, data, o.aOpciones()...)
	ms := ahora() - t0
	if err != nil {
		e := describirError(err, &ms)
		c, _ := e["code"].(string)
		if c == "" {
			c, _ = e["name"].(string)
		}
		return resultadoLlamada{ok: false, err: e, ms: ms, codigo: c}
	}
	return resultadoLlamada{ok: true, data: r, ms: ms, codigo: "OK"}
}

func mediana(xs []float64) float64 {
	c := append([]float64{}, xs...)
	sort.Float64s(c)
	return c[len(c)/2]
}

func (p *participante) ejecutar(o orden) map[string]any {
	switch o.Cmd {
	case "respond":
		p.mu.Lock()
		b := comportamiento{Kind: "echo"}
		if o.Behavior != nil {
			b = *o.Behavior
		}
		p.comportamientos[o.Operation] = b
		p.mu.Unlock()
		r, err := p.bus.Respond(context.Background(), o.Operation, p.manejador(o.Operation))
		if err != nil {
			return map[string]any{"ok": false, "error": describirError(err, nil)}
		}
		p.mu.Lock()
		p.responders[o.Operation] = r
		p.mu.Unlock()
		return map[string]any{"ok": true, "subject": r.Subject}

	case "behavior":
		p.mu.Lock()
		if o.Behavior != nil {
			p.comportamientos[o.Operation] = *o.Behavior
		}
		p.mu.Unlock()
		return map[string]any{"ok": true}

	case "stop":
		p.mu.Lock()
		r := p.responders[o.Operation]
		delete(p.responders, o.Operation)
		p.mu.Unlock()
		if r == nil {
			return map[string]any{"ok": false, "error": map[string]any{"name": "Error", "message": "no se atiende " + o.Operation}}
		}
		t0 := ahora()
		_ = r.Stop(context.Background())
		return map[string]any{"ok": true, "ms": ahora() - t0}

	case "call":
		r := p.llamar(o.Operation, o.Data, o.Options)
		if r.ok {
			return map[string]any{"ok": true, "data": r.data, "ms": r.ms}
		}
		return map[string]any{"ok": false, "error": r.err, "ms": r.ms}

	case "call_repeat":
		n := o.Repeat
		if n == 0 {
			n = 5
		}
		var tiempos []float64
		codigos := map[string]int{}
		var ultimo resultadoLlamada
		for i := 0; i < n; i++ {
			r := p.llamar(o.Operation, o.Data, o.Options)
			tiempos = append(tiempos, r.ms)
			codigos[r.codigo]++
			ultimo = r
		}
		minimo, maximo := tiempos[0], tiempos[0]
		for _, t := range tiempos {
			minimo, maximo = min(minimo, t), max(maximo, t)
		}
		last := map[string]any{"ok": ultimo.ok, "ms": ultimo.ms}
		if ultimo.ok {
			last["data"] = ultimo.data
		} else {
			last["error"] = ultimo.err
		}
		return map[string]any{"ok": true, "calls": len(tiempos), "p50Ms": mediana(tiempos), "maxMs": maximo, "minMs": minimo, "codes": codigos, "last": last}

	case "call_many":
		n := o.N
		if n == 0 {
			n = 100
		}
		conc := o.Conc
		if conc == 0 {
			conc = 20
		}
		var mu sync.Mutex
		porInstancia := map[string]int{}
		errores := map[string]int{}
		contestadas := 0
		var siguiente atomic.Int64
		var wg sync.WaitGroup
		for w := 0; w < conc; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := int(siguiente.Add(1)) - 1
					if i >= n {
						return
					}
					var cuerpo map[string]any
					if len(o.Data) > 0 {
						_ = json.Unmarshal(o.Data, &cuerpo)
					}
					if cuerpo == nil {
						cuerpo = map[string]any{}
					}
					cuerpo["n"] = i
					raw, _ := json.Marshal(cuerpo)
					r := p.llamar(o.Operation, raw, o.Options)
					mu.Lock()
					if r.ok {
						contestadas++
						var d struct {
							Instancia string `json:"instancia"`
						}
						_ = json.Unmarshal(r.data, &d)
						if d.Instancia == "" {
							d.Instancia = "?"
						}
						porInstancia[d.Instancia]++
					} else {
						errores[r.codigo]++
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		minShare := 0
		first := true
		for _, v := range porInstancia {
			if first || v < minShare {
				minShare, first = v, false
			}
		}
		return map[string]any{"ok": true, "answered": contestadas, "failed": n - contestadas, "byInstance": porInstancia, "minShare": minShare, "errors": errores}

	case "consume":
		s, err := p.bus.Subscribe(context.Background(), o.Subject, func(context.Context, flux.Event, flux.Delivery) error {
			p.consumidos.Add(1)
			return nil
		})
		if err != nil {
			return map[string]any{"ok": false, "error": describirError(err, nil)}
		}
		return map[string]any{"ok": true, "durable": s.Durable}

	case "watch":
		var sincronizado atomic.Bool
		opts := []flux.StateOption{flux.WithOnSynced(func() { sincronizado.Store(true) })}
		if o.TenantID != nil {
			opts = append(opts, flux.WithStateTenant(*o.TenantID))
		}
		_, err := p.bus.State().Watch(context.Background(), o.Contract, func(context.Context, flux.StateEntry, flux.StateDelivery) error {
			p.observados.Add(1)
			return nil
		}, opts...)
		if err != nil {
			return map[string]any{"ok": false, "error": describirError(err, nil)}
		}
		return map[string]any{"ok": true, "synced": sincronizado.Load()}

	case "publish":
		n := max(o.N, 1)
		var cuerpo map[string]any
		_ = json.Unmarshal(o.Data, &cuerpo)
		var opts struct {
			AggregateID string `json:"aggregateId"`
		}
		_ = json.Unmarshal(o.Options, &opts)
		var primero string
		for i := 0; i < n; i++ {
			d := map[string]any{}
			for k, v := range cuerpo {
				d[k] = v
			}
			agg := opts.AggregateID
			if o.N > 1 {
				d["pedidoId"] = fmt.Sprintf("ped-%d", i)
				if agg == "" {
					agg = fmt.Sprintf("ped-%d", i)
				}
			}
			var po []flux.PublishOption
			if agg != "" {
				po = append(po, flux.WithAggregateID(agg))
			}
			e, err := p.bus.Publish(context.Background(), o.Subject, d, po...)
			if err != nil {
				return map[string]any{"ok": false, "error": describirError(err, nil)}
			}
			if i == 0 {
				primero = e.ID
			}
		}
		return map[string]any{"ok": true, "eventId": primero, "published": n}

	case "state_put":
		var opts struct {
			TenantID       *string `json:"tenantId"`
			Classification *string `json:"classification"`
		}
		_ = json.Unmarshal(o.Options, &opts)
		var so []flux.StateOption
		if opts.TenantID != nil {
			so = append(so, flux.WithStateTenant(*opts.TenantID))
		}
		if opts.Classification != nil {
			so = append(so, flux.WithStateClassification(flux.DataClassification(*opts.Classification)))
		}
		r, err := p.bus.State().Put(context.Background(), o.Contract, o.Key, o.Data, so...)
		if err != nil {
			return map[string]any{"ok": false, "error": describirError(err, nil)}
		}
		return map[string]any{"ok": true, "outcome": r.Outcome, "revision": r.Revision}

	case "stats":
		p.mu.Lock()
		manejadas := map[string]int{}
		total := 0
		ids := map[string][]string{}
		unicos := map[string]struct{}{}
		for k, v := range p.atendidas {
			manejadas[k] = v
			total += v
		}
		for k, v := range p.idsVistos {
			ids[k] = append([]string{}, v...)
			for _, id := range v {
				unicos[id] = struct{}{}
			}
		}
		p.mu.Unlock()
		logs := p.logs.todos()
		return map[string]any{"ok": true, "handled": manejadas, "total": total, "requestIds": ids, "uniqueRequestIds": len(unicos),
			"consumed": p.consumidos.Load(), "watched": p.observados.Load(), "logs": logs, "logCount": len(logs)}

	case "close":
		t0 := ahora()
		_ = p.bus.Close()
		return map[string]any{"ok": true, "ms": ahora() - t0}
	}
	return map[string]any{"ok": false, "error": map[string]any{"name": "Error", "message": fmt.Sprintf("orden desconocida «%s»", o.Cmd)}}
}

// ─── El arranque y el bucle de órdenes ────────────────────────────────────────

var salidaMu sync.Mutex

func salir(o map[string]any) {
	b, _ := json.Marshal(o)
	salidaMu.Lock()
	defer salidaMu.Unlock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
}

func main() {
	servicio := env("FLUX_P_SERVICE", "")
	etiqueta := env("FLUX_P_LABEL", servicio)
	logs := &registro{}

	opts := flux.ConnectOptions{
		Servers: env("FLUX_P_URL", ""), Service: servicio, Environment: "test", Version: "1.0.0",
		TenantID:            env("FLUX_P_TENANT", "acme"),
		SchemaBaseURL:       "https://schemas.internal",
		Credentials:         flux.CredentialsFromNKeySeed(env("FLUX_P_SEED", "")),
		TLS:                 &flux.TLSOptions{CAFile: env("FLUX_P_CA", "")},
		PendingPollInterval: flux.PendingPollDisabled,
		Logger:              slog.New(manejadorLog{logs}),
	}
	if s := os.Getenv("FLUX_P_SIGNING"); s != "" {
		var f struct {
			KeyID      string            `json:"keyId"`
			PrivateKey string            `json:"privateKey"`
			PublicKeys map[string]string `json:"publicKeys"`
			Verify     string            `json:"verify"`
		}
		if err := json.Unmarshal([]byte(s), &f); err != nil {
			fmt.Fprintln(os.Stderr, "FLUX_P_SIGNING no es JSON:", err)
			os.Exit(2)
		}
		opts.Signing = flux.SigningOptions{PrivateKeyPEM: f.PrivateKey, KeyID: f.KeyID, PublicKeys: f.PublicKeys, Verify: flux.VerificationMode(f.Verify)}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	bus, err := flux.Connect(ctx, opts)
	cancel()
	listo := map[string]any{"id": 0, "ready": true, "sdk": "go", "service": servicio, "label": etiqueta}
	if err != nil {
		listo["ok"] = false
		listo["error"] = describirError(err, nil)
		salir(listo)
		return
	}
	salir(listo)

	p := &participante{servicio: servicio, etiqueta: etiqueta, bus: bus, logs: logs,
		comportamientos: map[string]comportamiento{}, atendidas: map[string]int{}, idsVistos: map[string][]string{},
		responders: map[string]*flux.Responder{}}

	// Las órdenes se atienden EN PARALELO (un `call_many` no bloquea un `stats`), salvo `close`, que espera a las demás.
	var pendientes sync.WaitGroup
	lector := bufio.NewScanner(os.Stdin)
	lector.Buffer(make([]byte, 1<<20), 16<<20)
	for lector.Scan() {
		linea := lector.Text()
		var o orden
		if err := json.Unmarshal([]byte(linea), &o); err != nil {
			salir(map[string]any{"id": -1, "ok": false, "error": map[string]any{"name": "Error", "message": "línea que no es JSON: " + linea[:min(len(linea), 80)]}})
			continue
		}
		if os.Getenv("FLUX_P_DEBUG") != "" {
			fmt.Fprintf(os.Stderr, "[%s] → %s\n", etiqueta, linea[:min(len(linea), 200)])
		}
		if o.Cmd == "close" {
			pendientes.Wait()
			r := p.ejecutar(o)
			r["id"] = o.ID
			salir(r)
			os.Exit(0)
		}
		pendientes.Add(1)
		go func() {
			defer pendientes.Done()
			r := p.ejecutar(o)
			r["id"] = o.ID
			salir(r)
		}()
	}
	pendientes.Wait()
	_ = bus.Close()
}
