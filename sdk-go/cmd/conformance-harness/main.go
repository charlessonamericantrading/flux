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

type entrada struct {
	Op         string            `json:"op"`
	Event      eventoEntrada     `json:"event"`
	DLQ        dlqEntrada        `json:"dlq"`
	Signing    *firmaEntrada     `json:"signing"`
	SignFirst  bool              `json:"signFirst"`
	Bytes      string            `json:"bytes"`
	PublicKeys map[string]string `json:"publicKeys"`
	Mode       string            `json:"mode"`
}

type salida struct {
	OK     bool   `json:"ok"`
	Bytes  string `json:"bytes,omitempty"`
	Code   string `json:"code,omitempty"`
	Detail string `json:"detail,omitempty"`
}

// construir monta el evento. El arnés NO rellena nada: todos los atributos vienen del
// vector, o los bytes no serían comparables.
//
// La única adaptación de forma es `time`: el vector lo da como cadena RFC 3339 y
// BuildEvent pide un time.Time. Se interpreta aquí y el SDK vuelve a formatearlo con
// flux.FormatTime, que es quien fija los 3 decimales del protocolo.
func construir(e eventoEntrada) (flux.Event, error) {
	t, err := time.Parse(time.RFC3339, e.Time)
	if err != nil {
		return flux.Event{}, fmt.Errorf("`time` %q no es RFC 3339: %w", e.Time, err)
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
