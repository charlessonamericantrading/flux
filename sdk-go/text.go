// Cómo se miden, se cortan y se escriben los textos del protocolo — la misma regla en todos los SDKs.
// Contrato normativo: specification/01-envelope.md §1.1 y §4.1, 04-errors.md §3
//
// JavaScript cuenta UNIDADES UTF-16 (`s.length`): un emoji son dos. JSON Schema (`maxLength`) cuenta
// **caracteres Unicode** (puntos de código), y en Go `len(s)` cuenta BYTES. Para que dos SDKs acepten y
// rechacen lo mismo, todo límite de longitud del protocolo se cuenta aquí en puntos de código
// (`message` de un error de rpc, `code`…), y lo único que se acota en **bytes** es `dlqerror`
// (1024 bytes UTF-8, sin partir un carácter): es un texto que acaba en un almacén con un tope de
// tamaño, no un campo con esquema.
//
// Y la otra mitad de la misma regla: los BYTES del mensaje. `encoding/json` escapa U+2028 y U+2029
// (`U+2028`) aunque se le pida que no escape el HTML, y `JSON.stringify` no: el mismo evento tendría
// dos secuencias de bytes distintas en Go y en Node, y con ellas dos firmas y dos hashes. Por eso el
// sobre se escribe con `appendJSONString`, que sigue la regla de `JSON.stringify` (01-envelope.md §1.1):
// UTF-8 literal, y solo se escapan las comillas, la barra invertida y los controles.

package flux

import (
	"strconv"
	"time"
	"unicode/utf8"
)

// codePointLength es la longitud en puntos de código: lo que cuenta `maxLength` de JSON Schema.
func codePointLength(s string) int { return utf8.RuneCountInString(s) }

// truncateCodePoints corta a `max` puntos de código como mucho: si no cabe, `max − 1` más `…`.
func truncateCodePoints(s string, max int) string {
	if len(s) <= max || utf8.RuneCountInString(s) <= max {
		return s
	}
	n := 0
	for i := range s {
		if n == max-1 {
			return s[:i] + "…"
		}
		n++
	}
	return s
}

// truncateUTF8Bytes corta a `maxBytes` bytes UTF-8 como mucho sin partir un carácter: el corte cae en
// la frontera del último carácter entero que cabe. Es la regla de `dlqerror` (04-errors.md §3).
func truncateUTF8Bytes(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

const hexDigits = "0123456789abcdef"

// appendJSONString escribe `s` como una cadena JSON con las reglas de `JSON.stringify`: `"` y `\` con
// barra, `\b \f \n \r \t` con su forma corta, el resto de controles (U+0000–U+001F) como `\u00xx` en
// minúsculas, y TODO lo demás literal —también U+007F, U+00A0, U+2028 y U+2029—. Un byte que no es UTF-8
// se escribe como U+FFFD, que es lo que haría el decodificador de cualquier otro SDK.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			switch c {
			case '"':
				dst = append(dst, '\\', '"')
			case '\\':
				dst = append(dst, '\\', '\\')
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				if c < 0x20 {
					dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
				} else {
					dst = append(dst, c)
				}
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, "\xef\xbf\xbd"...)
		} else {
			dst = append(dst, s[i:i+size]...)
		}
		i += size
	}
	return append(dst, '"')
}

// jsonString es appendJSONString sobre un búfer nuevo.
func jsonString(s string) string { return string(appendJSONString(nil, s)) }

// ─── time ────────────────────────────────────────────────────────────────────

// canonicalTime devuelve la forma canónica de `text` (`2026-08-20T10:41:07.318Z`: UTC, exactamente tres
// decimales y `Z`) o ok=false si no es un instante RFC 3339 válido — 01-envelope.md §2.2.
//
// Los constructores puros de cada SDK reciben `time` de fuera (el arnés de conformidad, un test, quien
// construya un transporte propio). Copiarlo tal cual dejaría pasar `2026-08-20T10:41:07Z` o `…+02:00`,
// que NO son los mismos bytes que la forma canónica y rompen el replay, la firma y la dedupe por hash; y
// un valor que ni siquiera es un instante («ayer») saldría en el sobre. Se NORMALIZA o se RECHAZA.
//
// Se admite lo que RFC 3339 llama `date-time` —`T`, `t` o un espacio entre la fecha y la hora; `Z`, `z`
// o un desfase `±hh:mm`; decimales opcionales y de cualquier longitud— y lo mismo que los SDKs de Node y
// de Rust. Los decimales de más de tres se TRUNCAN (no se redondean). No se admiten un segundo 60, un
// instante cuyo año en UTC cae fuera de 0000–9999, ni un desfase de 24 horas o más.
func canonicalTime(text string) (string, bool) {
	// Se analiza a mano y no con regexp: es la ruta caliente de todo constructor, y así cada rechazo
	// es exactamente el de time.ts (`\d` ASCII, nada de Unicode, y sin que `$` acepte un `\n` final).
	b := []byte(text)
	digits := func(from, n int) (int, bool) {
		if from+n > len(b) {
			return 0, false
		}
		v := 0
		for _, c := range b[from : from+n] {
			if c < '0' || c > '9' {
				return 0, false
			}
			v = v*10 + int(c-'0')
		}
		return v, true
	}
	var y, mo, d, h, mi, s int
	var ok bool
	if y, ok = digits(0, 4); !ok || len(b) < 19 || b[4] != '-' {
		return "", false
	}
	if mo, ok = digits(5, 2); !ok || b[7] != '-' {
		return "", false
	}
	if d, ok = digits(8, 2); !ok {
		return "", false
	}
	if c := b[10]; c != 'T' && c != 't' && c != ' ' {
		return "", false
	}
	if h, ok = digits(11, 2); !ok || b[13] != ':' {
		return "", false
	}
	if mi, ok = digits(14, 2); !ok || b[16] != ':' {
		return "", false
	}
	if s, ok = digits(17, 2); !ok {
		return "", false
	}
	i := 19
	ms := 0
	if i < len(b) && b[i] == '.' {
		i++
		start := i
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
		if i == start {
			return "", false
		}
		frac := string(b[start:i]) + "000"
		ms, _ = strconv.Atoi(frac[:3])
	}
	offsetMin := 0
	switch {
	case i < len(b) && (b[i] == 'Z' || b[i] == 'z') && i+1 == len(b):
		// UTC
	case i+6 == len(b) && (b[i] == '+' || b[i] == '-') && b[i+3] == ':':
		oh, ok1 := digits(i+1, 2)
		om, ok2 := digits(i+4, 2)
		if !ok1 || !ok2 || oh > 23 || om > 59 {
			return "", false
		}
		offsetMin = oh*60 + om
		if b[i] == '-' {
			offsetMin = -offsetMin
		}
	default:
		return "", false
	}
	if mo < 1 || mo > 12 || d < 1 || d > daysIn(y, mo) || h > 23 || mi > 59 || s > 59 {
		return "", false
	}
	utc := timeFromParts(y, mo, d, h, mi, s, ms, offsetMin)
	if utc.Year() < 0 || utc.Year() > 9999 {
		return "", false
	}
	return FormatTime(utc), true
}

func daysIn(year, month int) int {
	if month == 2 && (year%4 == 0 && year%100 != 0 || year%400 == 0) {
		return 29
	}
	return [...]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}[month-1]
}

// timeFromParts monta el instante UTC de unos campos ya validados y un desfase en minutos.
func timeFromParts(y, mo, d, h, mi, s, ms, offsetMin int) time.Time {
	return time.Date(y, time.Month(mo), d, h, mi, s, ms*1_000_000, time.UTC).
		Add(-time.Duration(offsetMin) * time.Minute)
}

// CanonicalTime devuelve la forma canónica de un instante RFC 3339 (`2026-08-20T10:41:07.318Z`: UTC, exactamente tres decimales y
// `Z`), o ok=false si no es un instante válido. Los constructores puros normalizan con ella el `time` que reciben de fuera
// (01-envelope.md §2.2).
func CanonicalTime(text string) (string, bool) { return canonicalTime(text) }
