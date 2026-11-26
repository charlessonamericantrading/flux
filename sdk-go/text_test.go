package flux

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// La forma canónica de `data` (01-envelope.md §4.1): la de JSON.stringify(JSON.parse(texto)) con UTF-8 literal.
func TestCanonicalizeJSON(t *testing.T) {
	casos := []struct{ nombre, entrada, quiero string }{
		{"sin espacios", "{ \"a\" : 1 ,\n\t\"b\" : [ 1 , 2 ] }", `{"a":1,"b":[1,2]}`},
		{"escape unicode resuelto a UTF-8 literal", `{"n":"café"}`, `{"n":"café"}`},
		{"barra escapada", `{"u":"a\/b"}`, `{"u":"a/b"}`},
		{"DEL, NBSP y U+2028 literales", `{"x":"\u007f\u00a0\u2028"}`, "{" + "\"x\":\"\x7f\u00a0\u2028\"}"},
		{"los controles siguen escapados, en minúsculas", `{"x":"\u0001\u001f\n\t\b\f\r"}`, `{"x":"\u0001\u001f\n\t\b\f\r"}`},
		{"comillas y barra invertida", `{"x":"\"\\"}`, `{"x":"\"\\"}`},
		{"par sustituto → carácter", `{"e":"😀"}`, `{"e":"😀"}`},
		{"clave repetida: posición de la primera, valor de la última", `{"a":1,"b":2,"a":3}`, `{"a":3,"b":2}`},
		{"orden de claves conservado", `{"z":1,"a":2,"m":3}`, `{"z":1,"a":2,"m":3}`},
		{"números tal cual", `{"a":-0,"b":1.5,"c":1E+2,"d":9007199254740993}`, `{"a":-0,"b":1.5,"c":1E+2,"d":9007199254740993}`},
		{"literales", `{"t":true,"f":false,"n":null,"o":{},"a":[]}`, `{"t":true,"f":false,"n":null,"o":{},"a":[]}`},
	}
	for _, c := range casos {
		got, err := canonicalizeJSON([]byte(c.entrada))
		if err != nil || string(got) != c.quiero {
			t.Errorf("%s: canonicalizeJSON(%q) = %q, %v; se esperaba %q", c.nombre, c.entrada, got, err, c.quiero)
		}
	}
}

func TestCanonicalizeJSONRechazaLoQueNoEsJSONOLoQueNoSeAdmite(t *testing.T) {
	profundo := func(n int) string { return strings.Repeat(`{"a":`, n) + "1" + strings.Repeat("}", n) }
	for nombre, entrada := range map[string]string{
		"BOM":                               "\xef\xbb\xbf{}",
		"UTF-8 inválido":                    "{\"a\":\"\xff\"}",
		"sustituto suelto alto":             `{"a":"\ud83d"}`,
		"sustituto suelto bajo":             `{"a":"\ude00"}`,
		"control sin escapar en una cadena": "{\"a\":\"\x01\"}",
		"escape desconocido":                `{"a":"\x"}`,
		"número con cero a la izquierda":    `{"a":01}`,
		"número sin dígitos tras el punto":  `{"a":1.}`,
		"número que empieza por punto":      `{"a":.5}`,
		"signo más":                         `{"a":+1}`,
		"coma final":                        `{"a":1,}`,
		"sobra texto":                       `{"a":1} x`,
		"vacío":                             ``,
		"objeto sin cerrar":                 `{"a":1`,
		"más de 128 niveles":                profundo(129),
	} {
		if got, err := canonicalizeJSON([]byte(entrada)); err == nil {
			t.Errorf("%s: se aceptó %q → %q", nombre, entrada, got)
		}
	}
	if _, err := canonicalizeJSON([]byte(profundo(128))); err != nil {
		t.Errorf("128 niveles sí se admiten: %v", err)
	}
}

func TestAppendJSONStringSigueAJSONStringify(t *testing.T) {
	// U+2028 y U+2029 son LITERALES: encoding/json los escapa aunque se le pida no escapar el HTML, y JSON.stringify no.
	for entrada, quiero := range map[string]string{
		"café":              `"café"`,
		"a\u2028b\u2029c":   "\"a\u2028b\u2029c\"",
		"<a href='x'>&</a>": `"<a href='x'>&</a>"`,
		"\x00\x1f":          `"\u0000\u001f"`,
		"\x7f ":             "\"\x7f \"",
		"\xff":              "\"\xef\xbf\xbd\"",
		"\"\\/":             `"\"\\/"`,
	} {
		if got := jsonString(entrada); got != quiero {
			t.Errorf("jsonString(%q) = %q, se esperaba %q", entrada, got, quiero)
		}
	}
}

func TestElEventoSerializadoNoEscapaU2028(t *testing.T) {
	in := entradaValida()
	in.Data = map[string]any{"texto": "a\u2028b"}
	e, err := BuildEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Serialize(e)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "a\u2028b") {
		t.Fatalf("no encuentro el texto en %s", b)
	}
	if strings.Contains(string(b), "\\u2028") {
		t.Errorf("U+2028 va escapado, y JSON.stringify lo escribe literal: los bytes (y la firma, y el hash) divergen de Node: %s", b)
	}
}

func TestCanonicalTime(t *testing.T) {
	validos := map[string]string{
		"2026-08-20T10:41:07.318Z":      "2026-08-20T10:41:07.318Z",
		"2026-08-20T10:41:07Z":          "2026-08-20T10:41:07.000Z",
		"2026-08-20T10:41:07.3Z":        "2026-08-20T10:41:07.300Z",
		"2026-08-20T10:41:07.318999Z":   "2026-08-20T10:41:07.318Z", // se TRUNCA, no se redondea
		"2026-08-20T12:41:07.318+02:00": "2026-08-20T10:41:07.318Z",
		"2026-08-20T10:41:07-05:30":     "2026-08-20T16:11:07.000Z",
		"2026-08-20t10:41:07z":          "2026-08-20T10:41:07.000Z",
		"2026-08-20 10:41:07Z":          "2026-08-20T10:41:07.000Z",
		"0000-01-01T00:00:00Z":          "0000-01-01T00:00:00.000Z",
		"9999-12-31T23:59:59.999Z":      "9999-12-31T23:59:59.999Z",
		"2024-02-29T23:59:59.999Z":      "2024-02-29T23:59:59.999Z",
		"0099-03-04T05:06:07Z":          "0099-03-04T05:06:07.000Z",
	}
	for in, quiero := range validos {
		if got, ok := canonicalTime(in); !ok || got != quiero {
			t.Errorf("canonicalTime(%q) = %q, %v; se esperaba %q", in, got, ok, quiero)
		}
	}
	for _, mal := range []string{
		"", "ayer", "2026-13-01T00:00:00Z", "2026-02-30T00:00:00Z", "2023-02-29T00:00:00Z", "2026-08-20T24:00:00Z",
		"2026-08-20T10:41:60Z", "2026-08-20T10:41:07", "2026-08-20T10:41:07.Z", "2026-08-20T10:41:07+24:00",
		"2026-08-20T10:41:07+02:60", "2026-08-20T10:41:07Z\n", "٢٠٢٦-08-20T10:41:07Z", "2026-08-20T10:41:07ZZ",
		"0000-01-01T00:00:00+01:00", "9999-12-31T23:59:59-01:00", "2026-8-20T10:41:07Z",
	} {
		if got, ok := canonicalTime(mal); ok {
			t.Errorf("canonicalTime(%q) = %q: se aceptó un instante que no lo es (o que cae fuera de 0000–9999 en UTC)", mal, got)
		}
	}
}

func TestTruncados(t *testing.T) {
	// 1024 BYTES UTF-8 sin partir un carácter: 1500 «é» (2 bytes) → 512.
	if got := truncateUTF8Bytes(strings.Repeat("é", 1500), 1024); got != strings.Repeat("é", 512) {
		t.Errorf("truncateUTF8Bytes de 1500 «é»: %d bytes", len(got))
	}
	// Un emoji de 4 bytes que no cabe se descarta ENTERO.
	s := strings.Repeat("a", 1022) + "😀"
	if got := truncateUTF8Bytes(s, 1024); got != strings.Repeat("a", 1022) || !utf8.ValidString(got) {
		t.Errorf("el emoji partido: %q", got[len(got)-3:])
	}
	if got := truncateUTF8Bytes("corto", 1024); got != "corto" {
		t.Errorf("no debe tocar lo que cabe: %q", got)
	}
	// `maxLength` cuenta puntos de código: mil emojis caben, 1001 no.
	if n := codePointLength(strings.Repeat("😀", 1000)); n != 1000 {
		t.Errorf("codePointLength = %d", n)
	}
	if got := truncateCodePoints(strings.Repeat("😀", 1001), 1000); codePointLength(got) != 1000 || !strings.HasSuffix(got, "…") {
		t.Errorf("truncateCodePoints: %d puntos de código", codePointLength(got))
	}
	if got := truncateCodePoints(strings.Repeat("😀", 1000), 1000); codePointLength(got) != 1000 || strings.HasSuffix(got, "…") {
		t.Errorf("lo que cabe no se toca")
	}
}
