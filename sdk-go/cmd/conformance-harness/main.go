// Arnés de conformidad cruzada — SDK de Go.
// Contrato: conformance/harness/README.md
//
// Lee UNA operación por stdin, escribe UN resultado por stdout, sale con 0 siempre.
// Deliberadamente delgado: toda lógica aquí es lógica que no está en el SDK y que el
// runner, por tanto, no verifica.
package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	flux "github.com/charlessonamericantrading/flux/sdk-go"
)

// eventoEntrada es el `event` del vector tal cual viene en el JSON.
//
// Data es json.RawMessage y no `any` a propósito: decodificarlo a map[string]any y
// volver a serializarlo ordenaría las claves alfabéticamente, y el orden del payload
// forma parte de los bytes que el runner compara.
type eventoEntrada struct {
	Subject            string                  `json:"subject"`
	ID                 string                  `json:"id"`
	Source             string                  `json:"source"`
	Time               string                  `json:"time"`
	DataSchema         string                  `json:"dataschema"`
	CorrelationID      string                  `json:"correlationid"`
	CausationID        string                  `json:"causationid"`
	TenantID           string                  `json:"tenantid"`
	ProducerVersion    string                  `json:"producerversion"`
	DataClassification flux.DataClassification `json:"dataclassification"`
	AggregateID        string                  `json:"aggregateId"`
	PartitionKey       string                  `json:"partitionkey"`
	TraceParent        string                  `json:"traceparent"`
	TraceState         string                  `json:"tracestate"`
	Data               json.RawMessage         `json:"data"`
}

type firmaEntrada struct {
	PrivateKeyPEM string `json:"privateKeyPem"`
	KeyID         string `json:"keyId"`
}

type dlqEntrada struct {
	Reason   string `json:"reason"`
	Attempts int    `json:"attempts"`
	Consumer string `json:"consumer"`
	Error    string `json:"error"`
	DLQTime  string `json:"dlqtime"`
}

// estadoEntrada es la entrada PLANA de `state-build`, `state-tombstone` y `state-subject` (la misma que el `input` de los fixtures de
// cross-sdk-state-envelope.json, más el `op`), y el `original` de `state-dlq`.
//
// `Data` y `AggregateVersion` son json.RawMessage por lo mismo que el `data` de los eventos: decodificarlos a un tipo de Go
// reordenaría las claves, y el orden forma parte de los bytes que el runner compara.
type estadoEntrada struct {
	Contract         string          `json:"contract"`
	TenantID         string          `json:"tenantId"`
	Key              string          `json:"key"`
	ID               string          `json:"id"`
	Source           string          `json:"source"`
	Time             string          `json:"time"`
	CorrelationID    string          `json:"correlationid"`
	ProducerVersion  string          `json:"producerversion"`
	Classification   string          `json:"classification"`
	CausationID      string          `json:"causationid"`
	TraceParent      string          `json:"traceparent"`
	TraceState       string          `json:"tracestate"`
	Data             json.RawMessage `json:"data"`
	AggregateVersion json.RawMessage `json:"aggregateVersion"`
}

type entrada struct {
	estadoEntrada
	Original *estadoEntrada `json:"original"`
	Revision uint64         `json:"revision"`

	Op         string            `json:"op"`
	Event      eventoEntrada     `json:"event"`
	DLQ        dlqEntrada        `json:"dlq"`
	Signing    *firmaEntrada     `json:"signing"`
	SignFirst  bool              `json:"signFirst"`
	Bytes      string            `json:"bytes"`
	PublicKeys map[string]string `json:"publicKeys"`
	Mode       string            `json:"mode"`
	Input      externalIDEntrada `json:"input"`
}

type salida struct {
	OK        bool   `json:"ok"`
	Bytes     string `json:"bytes,omitempty"`
	Subject   string `json:"subject,omitempty"`
	ID        string `json:"id,omitempty"`
	Effective int64  `json:"effectiveMs,omitempty"`
	Code      string `json:"code,omitempty"`
	Detail    string `json:"detail,omitempty"`
}

// construir monta el evento. El arnés NO rellena nada: todos los atributos vienen del
// vector, o los bytes no serían comparables.
//
// La única adaptación de forma es `time`: el vector lo da como cadena RFC 3339 y
// BuildEvent pide un time.Time. Se interpreta aquí y el SDK vuelve a formatearlo con
// flux.FormatTime, que es quien fija los 3 decimales del protocolo.
func construir(e eventoEntrada) (flux.Event, error) {
	canon, ok := flux.CanonicalTime(e.Time)
	if !ok {
		return flux.Event{}, flux.NewPermanentError("`time` debe ser un instante RFC 3339 (01-envelope.md §2.2)",
			flux.WithCode("WRONG_ATTRIBUTE_TYPE"))
	}
	t, err := time.Parse(time.RFC3339, canon)
	if err != nil {
		return flux.Event{}, fmt.Errorf("`time` %q no es RFC 3339: %w", canon, err)
	}
	return flux.BuildEvent(flux.BuildEventInput{
		Subject:            e.Subject,
		Data:               e.Data,
		ID:                 e.ID,
		Source:             e.Source,
		ProducerVersion:    e.ProducerVersion,
		TenantID:           e.TenantID,
		DataClassification: e.DataClassification,
		DataSchema:         e.DataSchema,
		CorrelationID:      e.CorrelationID,
		Time:               t,
		AggregateID:        e.AggregateID,
		CausationID:        e.CausationID,
		PartitionKey:       e.PartitionKey,
		TraceParent:        e.TraceParent,
		TraceState:         e.TraceState,
	})
}

func firmar(s firmaEntrada, e flux.Event) (flux.Event, error) {
	firmante, err := flux.NewSigner(flux.SigningOptions{PrivateKeyPEM: s.PrivateKeyPEM, KeyID: s.KeyID})
	if err != nil {
		return flux.Event{}, err
	}
	if firmante == nil {
		// NewSigner devuelve (nil, nil) sin clave privada: firmar es opcional en el SDK,
		// pero un vector de firma sin clave es una entrada inválida.
		return flux.Event{}, errors.New("la operación de firma requiere signing.privateKeyPem")
	}
	return firmante.Sign(e)
}

func serializar(e flux.Event) salida {
	b, err := flux.Serialize(e)
	if err != nil {
		return fallo(err)
	}
	return salida{OK: true, Bytes: base64.StdEncoding.EncodeToString(b)}
}

// fallo reporta el error con su código de protocolo. El código es lo que el runner
// compara en los vectores POISON: agrupar la DLQ por causa depende de que los siete
// SDKs devuelvan el mismo ante la misma entrada.
func fallo(err error) salida {
	code := "ERROR"
	var clasificado flux.ClassifiedError
	if errors.As(err, &clasificado) {
		code = clasificado.FluxCode()
	}
	return salida{OK: false, Code: code, Detail: err.Error()}
}

// estadoAEntrada pasa la entrada plana del arnés a la del SDK. No rellena NADA: todo viene del vector.
func estadoAEntrada(e estadoEntrada) flux.StateEventInput {
	return flux.StateEventInput{
		Contract: e.Contract, TenantID: e.TenantID, Key: e.Key, ID: e.ID, Source: e.Source, Time: e.Time,
		CorrelationID: e.CorrelationID, ProducerVersion: e.ProducerVersion,
		Classification: flux.DataClassification(e.Classification), Data: e.Data,
		CausationID: e.CausationID, TraceParent: e.TraceParent, TraceState: e.TraceState,
	}
}

// externalIDEntrada es la entrada de `external-id`: `{ source, externalKey, time }`, o bajo `input`. `externalKey` puede ser
// `{ repeat, times }`, que se expande para no pegar 513 caracteres en el caso.
type externalIDEntrada struct {
	Source      string          `json:"source"`
	ExternalKey json.RawMessage `json:"externalKey"`
	Time        string          `json:"time"`
}

func externalID(e externalIDEntrada) salida {
	var clave string
	var expandida struct {
		Repeat string `json:"repeat"`
		Times  int    `json:"times"`
	}
	if json.Unmarshal(e.ExternalKey, &clave) != nil {
		if json.Unmarshal(e.ExternalKey, &expandida) != nil || expandida.Times < 0 || expandida.Times > 1<<20 {
			return salida{OK: false}
		}
		clave = strings.Repeat(expandida.Repeat, expandida.Times)
	}
	canon, ok := flux.CanonicalTime(e.Time)
	if !ok {
		return salida{OK: false}
	}
	t, err := time.Parse(time.RFC3339, canon)
	if err != nil {
		return salida{OK: false}
	}
	id, err := flux.ExternalEventID(e.Source, clave, t)
	if err != nil {
		return salida{OK: false}
	}
	return salida{OK: true, ID: id}
}

func ejecutar(in entrada) salida {
	switch in.Op {
	case "build":
		e, err := construir(in.Event)
		if err != nil {
			return fallo(err)
		}
		return serializar(e)

	case "dlq":
		e, err := construir(in.Event)
		if err != nil {
			return fallo(err)
		}
		if in.SignFirst && in.Signing != nil {
			if e, err = firmar(*in.Signing, e); err != nil {
				return fallo(err)
			}
		}
		conDLQ := flux.ToDLQEvent(e, flux.DLQInfo{
			Reason:   flux.DLQReason(in.DLQ.Reason),
			Attempts: in.DLQ.Attempts,
			Consumer: in.DLQ.Consumer,
			Error:    in.DLQ.Error,
		})
		// `dlqtime` lo fija el vector: si lo pusiera el SDK, los bytes no serían
		// comparables entre ejecuciones, y mucho menos entre lenguajes.
		conDLQ.DLQTime = in.DLQ.DLQTime
		return serializar(conDLQ)

	case "sign":
		e, err := construir(in.Event)
		if err != nil {
			return fallo(err)
		}
		if in.Signing == nil {
			return fallo(errors.New("la operación de firma requiere `signing`"))
		}
		if e, err = firmar(*in.Signing, e); err != nil {
			return fallo(err)
		}
		return serializar(e)

	case "verify":
		raw, err := base64.StdEncoding.DecodeString(in.Bytes)
		if err != nil {
			return fallo(err)
		}
		e, err := flux.ParseEvent(raw)
		if err != nil {
			return fallo(err)
		}
		modo := in.Mode
		if modo == "" {
			modo = string(flux.VerifyRequire)
		}
		verificador, err := flux.NewVerifier(flux.SigningOptions{
			PublicKeys: in.PublicKeys,
			Verify:     flux.VerificationMode(modo),
		}, nil)
		if err != nil {
			return fallo(err)
		}
		if verificador != nil { // nil en modo `off`: no hay nada que comprobar.
			if err := verificador.Check(e); err != nil {
				return fallo(err)
			}
		}
		return salida{OK: true}

	case "parse":
		raw, err := base64.StdEncoding.DecodeString(in.Bytes)
		if err != nil {
			return fallo(err)
		}
		if _, err := flux.ParseEvent(raw); err != nil {
			return fallo(err)
		}
		return salida{OK: true}

	case "state-build":
		e, err := flux.BuildStateEvent(estadoAEntrada(in.estadoEntrada))
		if err != nil {
			return fallo(err)
		}
		return serializar(e)

	case "state-tombstone":
		version, err := flux.AggregateVersionFromJSON(in.AggregateVersion)
		if err != nil {
			// El orden de §3.6: contrato, clave y tenant van ANTES que la versión.
			if _, errSubj := flux.StateSubject(in.Contract, in.TenantID, in.Key); errSubj != nil {
				return fallo(errSubj)
			}
			return fallo(err)
		}
		e, err := flux.BuildStateTombstone(flux.StateTombstoneInput{
			Contract: in.Contract, TenantID: in.TenantID, Key: in.Key, ID: in.ID, Source: in.Source, Time: in.Time,
			CorrelationID: in.CorrelationID, ProducerVersion: in.ProducerVersion, AggregateVersion: version,
			CausationID: in.CausationID, TraceParent: in.TraceParent, TraceState: in.TraceState,
		})
		if err != nil {
			return fallo(err)
		}
		return serializar(e)

	case "state-subject":
		subject, err := flux.StateSubject(in.Contract, in.TenantID, in.Key)
		if err != nil {
			return fallo(err)
		}
		return salida{OK: true, Subject: subject}

	case "state-dlq":
		if in.Original == nil {
			return fallo(errors.New("`state-dlq` requiere `original`"))
		}
		e, err := flux.BuildStateDLQEvent(flux.StateDLQBuildInput{
			Original: estadoAEntrada(*in.Original), Revision: in.Revision,
			DLQ: flux.StateDLQInfo{
				Reason: flux.DLQReason(in.DLQ.Reason), Attempts: in.DLQ.Attempts, Consumer: in.DLQ.Consumer,
				Error: in.DLQ.Error, DLQTime: in.DLQ.DLQTime,
			},
		})
		if err != nil {
			return fallo(err)
		}
		return serializar(e)

	// El `id` determinista de un hecho importado (01-envelope.md §2.6.1; conformance/cases/cross-sdk-external-id.json).
	case "external-id":
		return externalID(in.Input)
	}

	return salida{OK: false, Code: "UNSUPPORTED_OP", Detail: in.Op}
}

func main() {
	bruto, err := io.ReadAll(os.Stdin)
	if err != nil {
		// Sin entrada no hay operación que reportar: esto sí es el arnés roto.
		fmt.Fprintln(os.Stderr, "no se pudo leer stdin:", err)
		os.Exit(1)
	}
	var in entrada
	if err := json.Unmarshal(bruto, &in); err != nil {
		var tipo *json.UnmarshalTypeError
		if errors.As(err, &tipo) {
			// Un valor de otro tipo JSON del que pide la operación (`time: 5`, una clave que no es una cadena): la entrada del
			// arnés está mal formada, no la operación. Tipos cerrados, como el arnés de Rust (`HARNESS_INPUT`).
			out, _ := json.Marshal(salida{OK: false, Code: "HARNESS_INPUT", Detail: err.Error()})
			os.Stdout.Write(out)
			return
		}
		fmt.Fprintln(os.Stderr, "la entrada del arnés no es JSON válido:", err)
		os.Exit(1)
	}

	// Un fallo de la operación se REPORTA, no se propaga: exit != 0 significaría que el
	// arnés está roto, no que el caso falló.
	out, err := json.Marshal(ejecutar(in))
	if err != nil {
		fmt.Fprintln(os.Stderr, "no se pudo serializar la salida del arnés:", err)
		os.Exit(1)
	}
	os.Stdout.Write(out)
}
