package flux

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// Réplica de sdk-node/test/signing.test.ts. Los mismos casos con las mismas claves de
// prueba generadas al vuelo: una firma que verifica en Node y no en Go rompería la
// premisa entera de la extensión — que la autenticidad viaja con el evento, no con el
// canal.

const keyID = "pedidos-api-1"

func nuevoPar(t *testing.T) KeyPair {
	t.Helper()
	par, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	return par
}

func firmante(t *testing.T, par KeyPair, id string) *Signer {
	t.Helper()
	s, err := NewSigner(SigningOptions{PrivateKeyPEM: par.PrivateKeyPEM, KeyID: id})
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	if s == nil {
		t.Fatal("NewSigner devolvió nil con una clave privada válida")
	}
	return s
}

func verificador(t *testing.T, claves map[string]string, mode VerificationMode) *Verifier {
	t.Helper()
	v, err := NewVerifier(SigningOptions{PublicKeys: claves, Verify: mode}, nil)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	if v == nil {
		t.Fatalf("NewVerifier devolvió nil en modo %q", mode)
	}
	return v
}

// eventoFirmable es el mismo evento base que usan los tests de envelope, con Time fijo
// para que la firma sea comparable entre ejecuciones.
func eventoFirmable(t *testing.T) Event {
	t.Helper()
	e, err := BuildEvent(entradaValida())
	if err != nil {
		t.Fatalf("BuildEvent: %v", err)
	}
	return e
}

// rootKeys devuelve los atributos raíz EN EL ORDEN en que aparecen en el JSON.
//
// Se recorre el stream de tokens porque un map[string]any perdería el orden, y el orden
// es justo lo que estos tests verifican — 01-envelope.md §6.
func rootKeys(t *testing.T, payload []byte) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(payload))
	if _, err := dec.Token(); err != nil { // '{'
		t.Fatalf("el payload no abre un objeto: %v", err)
	}
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			t.Fatalf("token: %v", err)
		}
		keys = append(keys, tok.(string))
		var descartado json.RawMessage
		if err := dec.Decode(&descartado); err != nil {
			t.Fatalf("valor de %q: %v", keys[len(keys)-1], err)
		}
	}
	return keys
}

func indiceDe(keys []string, name string) int {
	for i, k := range keys {
		if k == name {
			return i
		}
	}
	return -1
}

// poisonCode extrae el código estable del error, o "" si no es un PoisonError.
func poisonCode(err error) string {
	var pe *PoisonError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func TestFirmaAnadeSignKeyIDYSignature(t *testing.T) {
	par := nuevoPar(t)
	firmado, err := firmante(t, par, keyID).Sign(eventoFirmable(t))
	if err != nil {
		t.Fatal(err)
	}

	if firmado.SignKeyID != keyID {
		t.Errorf("signkeyid = %q, se esperaba %q", firmado.SignKeyID, keyID)
	}
	// base64url SIN padding: ni `+`, ni `/`, ni `=` — 07-signing.md §4.
	if strings.ContainsAny(firmado.Signature, "+/=") || firmado.Signature == "" {
		t.Errorf("signature %q no es base64url sin padding", firmado.Signature)
	}

	// `data` sigue siendo el último atributo y la firma va antes — 01-envelope.md §6.
	payload, err := Serialize(firmado)
	if err != nil {
		t.Fatal(err)
	}
	keys := rootKeys(t, payload)
	if keys[len(keys)-1] != "data" {
		t.Errorf("el último atributo es %q, debe ser `data`", keys[len(keys)-1])
	}
	if indiceDe(keys, "signkeyid") >= indiceDe(keys, "signature") {
		t.Errorf("orden incorrecto: %v", keys)
	}
	if indiceDe(keys, "signature") >= indiceDe(keys, "data") {
		t.Errorf("`signature` debe ir antes de `data`: %v", keys)
	}
}

func TestFirmaValidaVerifica(t *testing.T) {
	par := nuevoPar(t)
	firmado, err := firmante(t, par, keyID).Sign(eventoFirmable(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := verificador(t, map[string]string{keyID: par.PublicKeyPEM}, VerifyRequire).Check(firmado); err != nil {
		t.Errorf("una firma válida no verificó: %v", err)
	}
}

func TestFirmaEsDeterminista(t *testing.T) {
	// Solo es cierto porque 01-envelope.md §1.1, §2.2 y §6 fijan una única
	// representación en bytes. Sin ellas, firmar sería imposible entre lenguajes.
	s := firmante(t, nuevoPar(t), keyID)
	a, err := s.Sign(eventoFirmable(t))
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Sign(eventoFirmable(t))
	if err != nil {
		t.Fatal(err)
	}
	if a.Signature != b.Signature {
		t.Errorf("el mismo evento produjo dos firmas: %q vs %q", a.Signature, b.Signature)
	}
}

func TestFirmaSobreviveAlRoundTripDeSerializacion(t *testing.T) {
	par := nuevoPar(t)
	firmado, err := firmante(t, par, keyID).Sign(eventoFirmable(t))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := Serialize(firmado)
	if err != nil {
		t.Fatal(err)
	}
	// Si `signkeyid` o `signature` no estuviesen en AllowedRootAttributes, un evento
	// firmado sería POISON para su propio consumidor.
	vuelto, err := ParseEvent(payload)
	if err != nil {
		t.Fatalf("un evento firmado no se pudo parsear: %v", err)
	}
	if err := verificador(t, map[string]string{keyID: par.PublicKeyPEM}, VerifyRequire).Check(vuelto); err != nil {
		t.Errorf("la firma no sobrevivió al round-trip: %v", err)
	}
}

func TestFirmarNoMutaElEventoOriginal(t *testing.T) {
	original := eventoFirmable(t)
	if _, err := firmante(t, nuevoPar(t), keyID).Sign(original); err != nil {
		t.Fatal(err)
	}
	if original.Signature != "" || original.SignKeyID != "" {
		t.Error("Sign mutó el evento que recibió; Event se pasa por valor justo para evitarlo")
	}
}

func TestManipulacionInvalidaLaFirma(t *testing.T) {
	par := nuevoPar(t)
	v := verificador(t, map[string]string{keyID: par.PublicKeyPEM}, VerifyRequire)
	firmado, err := firmante(t, par, keyID).Sign(eventoFirmable(t))
	if err != nil {
		t.Fatal(err)
	}

	casos := []struct {
		nombre string
		mutar  func(Event) Event
		code   string
	}{
		{
			// Lo que la ACL del broker no cubre: un evento sacado del stream, editado y
			// reinyectado — 07-signing.md §1.
			"alterar data",
			func(e Event) Event { e.Data = json.RawMessage(`{"pedidoId":"ped-999"}`); return e },
			"INVALID_SIGNATURE",
		},
		{
			"alterar tenantid",
			func(e Event) Event { e.TenantID = "otro"; return e },
			"INVALID_SIGNATURE",
		},
		{
			// signkeyid va DENTRO de lo firmado justo para esto — 07-signing.md §5.
			"cambiar signkeyid",
			func(e Event) Event { e.SignKeyID = "otro-1"; return e },
			"UNKNOWN_SIGNING_KEY",
		},
		{
			// El base64 corrupto y la firma que no verifica son el mismo hecho para el
			// operador, así que comparten código.
			"firma que no es base64url",
			func(e Event) Event { e.Signature = "no-es-una-firma!!"; return e },
			"INVALID_SIGNATURE",
		},
	}

	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			err := v.Check(c.mutar(firmado))
			if got := poisonCode(err); got != c.code {
				t.Errorf("code = %q, se esperaba %q (err = %v)", got, c.code, err)
			}
		})
	}
}

func TestFirmaDeOtraClaveNoVerifica(t *testing.T) {
	legitima, impostora := nuevoPar(t), nuevoPar(t)
	// Mismo keyID, otra clave: es el ataque que la firma existe para detectar.
	firmado, err := firmante(t, impostora, keyID).Sign(eventoFirmable(t))
	if err != nil {
		t.Fatal(err)
	}
	v := verificador(t, map[string]string{keyID: legitima.PublicKeyPEM}, VerifyRequire)
	if code := poisonCode(v.Check(firmado)); code != "INVALID_SIGNATURE" {
		t.Errorf("code = %q, se esperaba INVALID_SIGNATURE", code)
	}
}

func TestFirmaSigueVerificandoTrasLaDLQ(t *testing.T) {
	// Las extensiones dlq* se añaden DESPUÉS de firmar y no están cubiertas. Si la
	// verificación no las ignorara, todo evento en la DLQ parecería manipulado.
	par := nuevoPar(t)
	firmado, err := firmante(t, par, keyID).Sign(eventoFirmable(t))
	if err != nil {
		t.Fatal(err)
	}
	enDLQ := ToDLQEvent(firmado, DLQInfo{
		Reason:   DLQReasonPermanent,
		Attempts: 1,
		Consumer: "facturacion-api__pedidos_pedido_v1_creado",
		Error:    "PEDIDO_YA_CANCELADO",
	})

	v := verificador(t, map[string]string{keyID: par.PublicKeyPEM}, VerifyRequire)
	if err := v.Check(enDLQ); err != nil {
		t.Errorf("un evento en la DLQ dejó de verificar: %v", err)
	}

	// Y tras el replay verbatim: el replay redistribuye un hecho ya emitido, no crea uno
	// nuevo — 07-signing.md §5.1.
	payload, err := Serialize(enDLQ)
	if err != nil {
		t.Fatal(err)
	}
	reproducido, err := ParseEvent(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Check(reproducido); err != nil {
		t.Errorf("un evento reproducido desde la DLQ dejó de verificar: %v", err)
	}
}

func TestLasDLQVanDespuesDeLaFirmaEnElCable(t *testing.T) {
	// El orden de claves es normativo (01-envelope.md §6) y aquí importa dos veces: las
	// dlq* se añaden después de firmar, así que quitarlas tiene que devolver exactamente
	// los bytes que se firmaron. Si SignKeyID se declarara bajo DLQTime, el mismo evento
	// daría bytes distintos en Go y en Node.
	firmado, err := firmante(t, nuevoPar(t), keyID).Sign(eventoFirmable(t))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := Serialize(ToDLQEvent(firmado, DLQInfo{
		Reason: DLQReasonPoison, Attempts: 1, Consumer: "c", Error: "x",
	}))
	if err != nil {
		t.Fatal(err)
	}
	keys := rootKeys(t, payload)
	if !(indiceDe(keys, "signkeyid") < indiceDe(keys, "signature") &&
		indiceDe(keys, "signature") < indiceDe(keys, "dlqreason") &&
		indiceDe(keys, "dlqreason") < indiceDe(keys, "data")) {
		t.Errorf("orden de atributos incorrecto: %v", keys)
	}
}

func TestModoRequireRechazaUnEventoSinFirma(t *testing.T) {
	v := verificador(t, map[string]string{keyID: nuevoPar(t).PublicKeyPEM}, VerifyRequire)
	if code := poisonCode(v.Check(eventoFirmable(t))); code != "MISSING_SIGNATURE" {
		t.Errorf("code = %q, se esperaba MISSING_SIGNATURE", code)
	}
}

func TestModoWarnRegistraYAcepta(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	v, err := NewVerifier(SigningOptions{
		PublicKeys: map[string]string{keyID: nuevoPar(t).PublicKeyPEM},
		Verify:     VerifyWarn,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Check(eventoFirmable(t)); err != nil {
		t.Errorf("warn no debe fallar, devolvió %v", err)
	}
	if !strings.Contains(buf.String(), "sin firma") {
		t.Errorf("warn no registró nada; log = %q", buf.String())
	}
}

func TestModoOffNoConstruyeVerificador(t *testing.T) {
	// No se paga lo que no se usa, y off es el default del protocolo.
	for _, opts := range []SigningOptions{{Verify: VerifyOff}, {}} {
		v, err := NewVerifier(opts, nil)
		if err != nil || v != nil {
			t.Errorf("NewVerifier(%+v) = (%v, %v), se esperaba (nil, nil)", opts, v, err)
		}
	}
}

func TestGestionDeClaves(t *testing.T) {
	par := nuevoPar(t)

	t.Run("firmar sin KeyID falla con un mensaje accionable", func(t *testing.T) {
		_, err := NewSigner(SigningOptions{PrivateKeyPEM: par.PrivateKeyPEM})
		var ke *SigningKeyError
		if !errors.As(err, &ke) || !strings.Contains(err.Error(), "KeyID") {
			t.Errorf("err = %v, se esperaba un SigningKeyError sobre KeyID", err)
		}
	})

	t.Run("sin clave privada no hay firmante", func(t *testing.T) {
		s, err := NewSigner(SigningOptions{})
		if s != nil || err != nil {
			t.Errorf("NewSigner(vacío) = (%v, %v), se esperaba (nil, nil)", s, err)
		}
	})

	t.Run("rechaza una clave que no sea Ed25519", func(t *testing.T) {
		rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		der, err := x509.MarshalPKCS8PrivateKey(rsaKey)
		if err != nil {
			t.Fatal(err)
		}
		pemText := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
		_, err = NewSigner(SigningOptions{PrivateKeyPEM: pemText, KeyID: "x-1"})
		if err == nil || !strings.Contains(err.Error(), "ed25519") {
			t.Errorf("err = %v, se esperaba un rechazo mencionando ed25519", err)
		}
	})

	t.Run("verificar sin claves públicas explica la retención", func(t *testing.T) {
		_, err := NewVerifier(SigningOptions{Verify: VerifyRequire}, nil)
		if err == nil || !strings.Contains(err.Error(), "RETIRADAS") {
			t.Errorf("err = %v, se esperaba una mención a las claves RETIRADAS", err)
		}
	})

	t.Run("una clave retirada sigue verificando", func(t *testing.T) {
		// Retirar una clave impide EMITIR con ella, no VERIFICAR lo ya emitido. Tratarla
		// como inválida convertiría una rotación rutinaria en la invalidación
		// retroactiva de todo el historial — 07-signing.md §6.
		vieja, nueva := nuevoPar(t), nuevoPar(t)
		firmado, err := firmante(t, vieja, "pedidos-api-1").Sign(eventoFirmable(t))
		if err != nil {
			t.Fatal(err)
		}
		v := verificador(t, map[string]string{
			"pedidos-api-1": vieja.PublicKeyPEM, // retirada, conservada
			"pedidos-api-2": nueva.PublicKeyPEM, // activa
		}, VerifyRequire)
		if err := v.Check(firmado); err != nil {
			t.Errorf("una clave retirada dejó de verificar: %v", err)
		}
	})
}
