// La forma canónica de `data` — 01-envelope.md §4.1.
//
// Dos SDKs que escriben el mismo `data` con espacios, escapes o un orden distintos producen secuencias de
// bytes distintas, y flux depende de la secuencia de bytes para el replay, la firma y la dedupe por hash
// (CLAUDE.md, regla 8). La forma canónica es la de `JSON.stringify(JSON.parse(texto))` con UTF-8
// literal: sin espacios entre elementos, las claves en el orden en que se dieron, los escapes
// `\uXXXX` de un carácter que no los necesita resueltos (`café` → `café`), `\/` → `/`, y solo los
// escapes que el formato exige (`"`, `\`, controles).
//
// Qué NO se admite —diferencias del lenguaje, no del SDK (01-envelope.md §4.1, «Lo que no se admite»):
// un entero fuera de ±(2^53−1) (va como cadena), un flotante o un exponente (el dinero va en enteros),
// una clave que sea un índice de array (JavaScript la ordenaría primero), un sustituto UTF-16 suelto
// (`\ud83d`: no es UTF-8) y más de 128 niveles de anidamiento. Los números se copian tal cual: no se
// reescriben, así que `4995.00` sigue siendo `4995.00` aquí y `4995` en Node, y por eso no se admite.
//
// Una clave repetida (`{"a":1,"b":2,"a":3}`) conserva la POSICIÓN de la primera y el valor de la última, que
// es lo que hace JSON.parse.

package flux

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"unicode/utf8"
)

// maxDataDepth: más niveles de anidamiento que esto no se admiten (01-envelope.md §4.1).
const maxDataDepth = 128

// canonicalizeJSON devuelve la forma canónica de un texto JSON, o un error si no es JSON válido (RFC 8259,
// UTF-8 estricto y sin marca de orden de bytes) o usa algo que el protocolo no admite.
func canonicalizeJSON(raw []byte) ([]byte, error) {
	if bytes.HasPrefix(raw, []byte("\xef\xbb\xbf")) {
		return nil, errors.New("el texto empieza por una marca de orden de bytes (BOM), que no es JSON (RFC 8259 §8.1)")
	}
	if !utf8.Valid(raw) {
		return nil, errors.New("el texto no es UTF-8 válido")
	}
	p := &canonParser{in: raw}
	out, err := p.value(0)
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.pos != len(p.in) {
		return nil, p.errorf("sobra texto tras el valor")
	}
	return out, nil
}

type canonParser struct {
	in  []byte
	pos int
}

func (p *canonParser) errorf(format string, a ...any) error {
	return fmt.Errorf("JSON no válido en el byte %d: %s", p.pos, fmt.Sprintf(format, a...))
}

func (p *canonParser) skipSpace() {
	for p.pos < len(p.in) {
		switch p.in[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *canonParser) value(depth int) ([]byte, error) {
	p.skipSpace()
	if p.pos >= len(p.in) {
		return nil, p.errorf("se esperaba un valor")
	}
	switch c := p.in[p.pos]; {
	case c == '{':
		return p.object(depth)
	case c == '[':
		return p.array(depth)
	case c == '"':
		s, err := p.str()
		if err != nil {
			return nil, err
		}
		return appendJSONString(nil, s), nil
	case c == '-' || (c >= '0' && c <= '9'):
		return p.number()
	default:
		for _, lit := range []string{"true", "false", "null"} {
			if bytes.HasPrefix(p.in[p.pos:], []byte(lit)) {
				p.pos += len(lit)
				return []byte(lit), nil
			}
		}
		return nil, p.errorf("carácter inesperado %q", c)
	}
}

func (p *canonParser) object(depth int) ([]byte, error) {
	if depth >= maxDataDepth {
		return nil, p.errorf("más de %d niveles de anidamiento: `data` plano (01-envelope.md §4.1)", maxDataDepth)
	}
	p.pos++ // {
	var keys []string
	vals := map[string][]byte{}
	p.skipSpace()
	if p.pos < len(p.in) && p.in[p.pos] == '}' {
		p.pos++
		return []byte("{}"), nil
	}
	for {
		p.skipSpace()
		if p.pos >= len(p.in) || p.in[p.pos] != '"' {
			return nil, p.errorf("se esperaba el nombre de una clave")
		}
		k, err := p.str()
		if err != nil {
			return nil, err
		}
		p.skipSpace()
		if p.pos >= len(p.in) || p.in[p.pos] != ':' {
			return nil, p.errorf("se esperaba `:` tras la clave")
		}
		p.pos++
		v, err := p.value(depth + 1)
		if err != nil {
			return nil, err
		}
		if _, dup := vals[k]; !dup {
			keys = append(keys, k)
		}
		vals[k] = v
		p.skipSpace()
		if p.pos >= len(p.in) {
			return nil, p.errorf("objeto sin cerrar")
		}
		if p.in[p.pos] == ',' {
			p.pos++
			continue
		}
		if p.in[p.pos] == '}' {
			p.pos++
			break
		}
		return nil, p.errorf("se esperaba `,` o `}`")
	}
	out := []byte{'{'}
	for i, k := range keys {
		if i > 0 {
			out = append(out, ',')
		}
		out = appendJSONString(out, k)
		out = append(out, ':')
		out = append(out, vals[k]...)
	}
	return append(out, '}'), nil
}

func (p *canonParser) array(depth int) ([]byte, error) {
	if depth >= maxDataDepth {
		return nil, p.errorf("más de %d niveles de anidamiento: `data` plano (01-envelope.md §4.1)", maxDataDepth)
	}
	p.pos++ // [
	out := []byte{'['}
	p.skipSpace()
	if p.pos < len(p.in) && p.in[p.pos] == ']' {
		p.pos++
		return append(out, ']'), nil
	}
	for first := true; ; first = false {
		if !first {
			out = append(out, ',')
		}
		v, err := p.value(depth + 1)
		if err != nil {
			return nil, err
		}
		out = append(out, v...)
		p.skipSpace()
		if p.pos >= len(p.in) {
			return nil, p.errorf("array sin cerrar")
		}
		if p.in[p.pos] == ',' {
			p.pos++
			continue
		}
		if p.in[p.pos] == ']' {
			p.pos++
			return append(out, ']'), nil
		}
		return nil, p.errorf("se esperaba `,` o `]`")
	}
}

// str lee una cadena JSON con sus escapes y devuelve su valor.
func (p *canonParser) str() (string, error) {
	p.pos++ // "
	var sb []byte
	for p.pos < len(p.in) {
		c := p.in[p.pos]
		switch {
		case c == '"':
			p.pos++
			return string(sb), nil
		case c < 0x20:
			return "", p.errorf("un carácter de control sin escapar dentro de una cadena")
		case c == '\\':
			p.pos++
			if p.pos >= len(p.in) {
				return "", p.errorf("escape incompleto")
			}
			e := p.in[p.pos]
			p.pos++
			switch e {
			case '"', '\\', '/':
				sb = append(sb, e)
			case 'b':
				sb = append(sb, '\b')
			case 'f':
				sb = append(sb, '\f')
			case 'n':
				sb = append(sb, '\n')
			case 'r':
				sb = append(sb, '\r')
			case 't':
				sb = append(sb, '\t')
			case 'u':
				r, err := p.hex4()
				if err != nil {
					return "", err
				}
				if r >= 0xD800 && r <= 0xDBFF {
					// Un sustituto alto exige su pareja: `😀` es U+1F600.
					if p.pos+1 < len(p.in) && p.in[p.pos] == '\\' && p.in[p.pos+1] == 'u' {
						save := p.pos
						p.pos += 2
						lo, err := p.hex4()
						if err == nil && lo >= 0xDC00 && lo <= 0xDFFF {
							r = 0x10000 + (r-0xD800)<<10 + (lo - 0xDC00)
							sb = utf8.AppendRune(sb, r)
							continue
						}
						p.pos = save
					}
					return "", p.errorf("un sustituto UTF-16 suelto (%04x): no es UTF-8 y el protocolo no lo admite", r)
				}
				if r >= 0xDC00 && r <= 0xDFFF {
					return "", p.errorf("un sustituto UTF-16 suelto (%04x): no es UTF-8 y el protocolo no lo admite", r)
				}
				sb = utf8.AppendRune(sb, r)
			default:
				return "", p.errorf("escape desconocido `\\%c`", e)
			}
		default:
			sb = append(sb, c)
			p.pos++
		}
	}
	return "", p.errorf("cadena sin cerrar")
}

func (p *canonParser) hex4() (rune, error) {
	if p.pos+4 > len(p.in) {
		return 0, p.errorf("escape \\u incompleto")
	}
	v, err := strconv.ParseUint(string(p.in[p.pos:p.pos+4]), 16, 32)
	if err != nil {
		return 0, p.errorf("escape \\u con dígitos que no son hexadecimales")
	}
	p.pos += 4
	return rune(v), nil
}

// number copia un número tal cual, comprobando la gramática de RFC 8259.
func (p *canonParser) number() ([]byte, error) {
	start := p.pos
	if p.in[p.pos] == '-' {
		p.pos++
	}
	if p.pos >= len(p.in) {
		return nil, p.errorf("número incompleto")
	}
	switch {
	case p.in[p.pos] == '0':
		p.pos++
	case p.in[p.pos] >= '1' && p.in[p.pos] <= '9':
		for p.pos < len(p.in) && p.in[p.pos] >= '0' && p.in[p.pos] <= '9' {
			p.pos++
		}
	default:
		return nil, p.errorf("número mal formado")
	}
	if p.pos < len(p.in) && p.in[p.pos] == '.' {
		p.pos++
		n := 0
		for p.pos < len(p.in) && p.in[p.pos] >= '0' && p.in[p.pos] <= '9' {
			p.pos++
			n++
		}
		if n == 0 {
			return nil, p.errorf("número mal formado: falta un dígito tras el punto")
		}
	}
	if p.pos < len(p.in) && (p.in[p.pos] == 'e' || p.in[p.pos] == 'E') {
		p.pos++
		if p.pos < len(p.in) && (p.in[p.pos] == '+' || p.in[p.pos] == '-') {
			p.pos++
		}
		n := 0
		for p.pos < len(p.in) && p.in[p.pos] >= '0' && p.in[p.pos] <= '9' {
			p.pos++
			n++
		}
		if n == 0 {
			return nil, p.errorf("número mal formado: falta el exponente")
		}
	}
	return append([]byte(nil), p.in[start:p.pos]...), nil
}
