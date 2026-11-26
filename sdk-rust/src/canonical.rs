//! La forma canónica de `data` — 01-envelope.md §4.1.
//!
//! La que produce `JSON.stringify` de Node con UTF-8 literal: **sin espacios** entre elementos; las claves
//! **en el orden en que se dan**, y una clave repetida conserva la posición de la primera y el valor de la
//! última (como `JSON.parse`); las cadenas con los caracteres no ASCII **literales** y solo `"`, `\` y los
//! controles U+0000–U+001F escapados (`\b \f \n \r \t`, o `\u00xx` en minúscula).
//!
//! Los **números no se tocan**: cada uno viaja con los mismos caracteres con los que llegó
//! (01-envelope.md §2.5: `4995.00` sigue siendo `4995.00`). Por eso no hay forma canónica común para los
//! que un lenguaje escribe de otra manera —los enteros fuera de ±(2^53−1), que Node redondea; los
//! flotantes (`4995.00`, `1E+2`, `-0`), que JavaScript reescribe como `4995`, `100` y `0`—: el dinero va en
//! enteros, y esos números no se admiten en `data` si los bytes importan (firma, replay, dedupe por hash).
//!
//! Se reescribe con un analizador propio y no pasando por `serde_json::Value` precisamente por los números:
//! `Value` convertiría `4995.00` en `4995.0` y `123456789012345678901` en un `f64`.

use std::collections::HashMap;

use serde_json::value::RawValue;

/// Profundidad máxima de anidamiento: la misma que el analizador de `serde_json` (128), para que lo que se
/// rechaza al construir sea lo que se rechazaría al consumir. JavaScript admite más; no hay vector que lo pida.
const MAX_DEPTH: usize = 128;

enum Node {
    /// Un número, con sus caracteres originales.
    Number(String),
    /// Una cadena ya decodificada.
    Text(String),
    /// `true`, `false` o `null`.
    Literal(&'static str),
    Array(Vec<Node>),
    Object(Vec<(String, Node)>),
}

struct Parser<'a> {
    src: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn bytes(&self) -> &'a [u8] {
        self.src.as_bytes()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes().get(self.pos).copied()
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn fail<T>(&self, expected: &str) -> Result<T, String> {
        Err(format!(
            "`data` no es JSON válido: en el byte {} se esperaba {expected}",
            self.pos
        ))
    }

    fn literal(&mut self, word: &'static str) -> Result<Node, String> {
        if self.src[self.pos..].starts_with(word) {
            self.pos += word.len();
            Ok(Node::Literal(word))
        } else {
            self.fail(&format!("`{word}`"))
        }
    }

    /// Un número JSON: `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`. Se devuelve el texto original.
    fn number(&mut self) -> Result<Node, String> {
        let start = self.pos;
        let digits = |p: &mut Self| {
            let from = p.pos;
            while matches!(p.peek(), Some(b'0'..=b'9')) {
                p.pos += 1;
            }
            p.pos > from
        };
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        match self.peek() {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => {
                digits(self);
            }
            _ => return self.fail("un dígito"),
        }
        if self.peek() == Some(b'.') {
            self.pos += 1;
            if !digits(self) {
                return self.fail("un dígito tras el punto decimal");
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !digits(self) {
                return self.fail("un dígito en el exponente");
            }
        }
        Ok(Node::Number(self.src[start..self.pos].to_string()))
    }

    /// Una cadena: se localiza el cierre respetando las barras y se decodifica con `serde_json`, que
    /// valida los escapes y rechaza un sustituto suelto (`\ud83d`).
    fn string(&mut self) -> Result<String, String> {
        let start = self.pos;
        self.pos += 1; // la comilla de apertura
        loop {
            match self.peek() {
                None => return self.fail("el cierre de la cadena"),
                Some(b'\\') => self.pos += 2,
                Some(b'"') => {
                    self.pos += 1;
                    break;
                }
                Some(_) => self.pos += 1,
            }
        }
        let end = self.pos.min(self.src.len());
        serde_json::from_str::<String>(&self.src[start..end]).map_err(|_| {
            format!(
                "`data` no es JSON válido: la cadena que empieza en el byte {start} no se puede \
                 decodificar (un escape mal formado, un carácter de control sin escapar o un \
                 sustituto UTF-16 suelto, que no es UTF-8)"
            )
        })
    }

    fn value(&mut self, depth: usize) -> Result<Node, String> {
        if depth > MAX_DEPTH {
            return Err(format!(
                "`data` anida más de {MAX_DEPTH} niveles: el analizador de este SDK no lo admite"
            ));
        }
        self.skip_whitespace();
        match self.peek() {
            Some(b'{') => {
                self.pos += 1;
                let mut entries: Vec<(String, Node)> = Vec::new();
                // Solo se indexan los objetos grandes: la búsqueda lineal de un objeto corriente es más barata.
                let mut index: HashMap<String, usize> = HashMap::new();
                self.skip_whitespace();
                if self.peek() == Some(b'}') {
                    self.pos += 1;
                    return Ok(Node::Object(entries));
                }
                loop {
                    self.skip_whitespace();
                    if self.peek() != Some(b'"') {
                        return self.fail("el nombre de una clave (una cadena)");
                    }
                    let key = self.string()?;
                    self.skip_whitespace();
                    if self.peek() != Some(b':') {
                        return self.fail("`:`");
                    }
                    self.pos += 1;
                    let value = self.value(depth + 1)?;
                    // Una clave repetida: la posición de la primera, el valor de la última (`JSON.parse`).
                    let position = if entries.len() < 16 {
                        entries.iter().position(|(k, _)| *k == key)
                    } else {
                        if index.is_empty() {
                            index = entries
                                .iter()
                                .enumerate()
                                .map(|(i, (k, _))| (k.clone(), i))
                                .collect();
                        }
                        index.get(&key).copied()
                    };
                    if let Some(i) = position {
                        entries[i].1 = value;
                    } else {
                        if !index.is_empty() {
                            index.insert(key.clone(), entries.len());
                        }
                        entries.push((key, value));
                    }
                    self.skip_whitespace();
                    match self.peek() {
                        Some(b',') => self.pos += 1,
                        Some(b'}') => {
                            self.pos += 1;
                            return Ok(Node::Object(entries));
                        }
                        _ => return self.fail("`,` o `}`"),
                    }
                }
            }
            Some(b'[') => {
                self.pos += 1;
                let mut items = Vec::new();
                self.skip_whitespace();
                if self.peek() == Some(b']') {
                    self.pos += 1;
                    return Ok(Node::Array(items));
                }
                loop {
                    items.push(self.value(depth + 1)?);
                    self.skip_whitespace();
                    match self.peek() {
                        Some(b',') => self.pos += 1,
                        Some(b']') => {
                            self.pos += 1;
                            return Ok(Node::Array(items));
                        }
                        _ => return self.fail("`,` o `]`"),
                    }
                }
            }
            Some(b'"') => self.string().map(Node::Text),
            Some(b't') => self.literal("true"),
            Some(b'f') => self.literal("false"),
            Some(b'n') => self.literal("null"),
            Some(b'-' | b'0'..=b'9') => self.number(),
            _ => self.fail("un valor JSON"),
        }
    }
}

fn emit(node: &Node, out: &mut String) {
    match node {
        Node::Number(text) => out.push_str(text),
        // `to_string` de una cadena: UTF-8 literal, escapando `"`, `\` y los controles.
        Node::Text(s) => out.push_str(&serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into())),
        Node::Literal(word) => out.push_str(word),
        Node::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                emit(item, out);
            }
            out.push(']');
        }
        Node::Object(entries) => {
            out.push('{');
            for (i, (key, value)) in entries.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).unwrap_or_else(|_| "\"\"".into()));
                out.push(':');
                emit(value, out);
            }
            out.push('}');
        }
    }
}

/// La forma canónica de un texto JSON, que DEBE ser un objeto en la raíz. Ver el módulo.
///
/// # Errores
///
/// El motivo, sin valores, si no es JSON, no es un objeto o anida demasiado.
pub(crate) fn canonical_text(text: &str) -> Result<String, String> {
    let mut parser = Parser { src: text, pos: 0 };
    parser.skip_whitespace();
    if parser.peek() != Some(b'{') {
        return Err(
            "`data` debe ser un objeto JSON en la raíz (ni array ni escalar), para poder \
                    añadir campos sin romper el contrato (01-envelope.md §4)"
                .to_string(),
        );
    }
    let root = parser.value(0)?;
    parser.skip_whitespace();
    if parser.pos != text.len() {
        return parser.fail("el fin del texto");
    }
    let mut out = String::with_capacity(text.len());
    emit(&root, &mut out);
    Ok(out)
}

/// [`canonical_text`] de un valor crudo, de vuelta a un `RawValue`.
pub(crate) fn canonical_raw(raw: &RawValue) -> Result<Box<RawValue>, String> {
    RawValue::from_string(canonical_text(raw.get())?)
        .map_err(|e| format!("`data` no es serializable a JSON: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write;

    fn canon(text: &str) -> String {
        canonical_text(text).unwrap()
    }

    #[test]
    fn sin_espacios_y_con_las_claves_en_su_orden() {
        assert_eq!(
            canon(" {\n\t\"z\" : 1 , \"a\" : [ 1 , 2 , { \"b\" : null } ] , \"m\" : true }\r\n"),
            r#"{"z":1,"a":[1,2,{"b":null}],"m":true}"#
        );
        assert_eq!(canon("{}"), "{}");
        assert_eq!(canon(r#"{"a":[],"b":{}}"#), r#"{"a":[],"b":{}}"#);
    }

    #[test]
    fn una_clave_repetida_conserva_la_posicion_de_la_primera_y_el_valor_de_la_ultima() {
        assert_eq!(canon(r#"{"a":1,"a":2}"#), r#"{"a":2}"#);
        assert_eq!(canon(r#"{"a":1,"b":3,"a":2}"#), r#"{"a":2,"b":3}"#);
        // En un objeto grande (con índice) pasa lo mismo.
        let mut texto = String::from("{");
        for i in 0..40 {
            let _ = write!(texto, "\"k{i}\":{i},");
        }
        texto.push_str("\"k3\":99,\"k39\":98}");
        let c = canon(&texto);
        assert!(c.contains("\"k3\":99,\"k4\":4"), "{c}");
        assert!(c.ends_with("\"k39\":98}"), "{c}");
        assert_eq!(c.matches("\"k3\":").count(), 1);
    }

    #[test]
    fn utf8_literal_y_solo_se_escapa_lo_que_hay_que_escapar() {
        // `\u00f1` y `\/` y un par sustituto se reescriben literales; `\u007f` es DEL literal.
        assert_eq!(
            canon(r#"{"a":"ca\u00f1a a\/b \ud83d\ude00"}"#),
            "{\"a\":\"caña a/b 😀\"}"
        );
        assert_eq!(canon(r#"{"a":"\u007f"}"#), "{\"a\":\"\u{7f}\"}");
        // Se escapan las comillas, la barra invertida y los controles (con la forma corta si la hay).
        assert_eq!(
            canon(r#"{"a":"\"\\\n\t\u0001\u001f"}"#),
            r#"{"a":"\"\\\n\t\u0001\u001f"}"#
        );
        // `<`, `>`, `&` y U+2028 van literales.
        assert_eq!(canon("{\"a\":\"<&>\u{2028}\"}"), "{\"a\":\"<&>\u{2028}\"}");
    }

    #[test]
    fn los_numeros_no_se_reescriben() {
        assert_eq!(
            canon(r#"{"a":4995.00,"b":1E+2,"c":-0,"d":123456789012345678901,"e":0.1,"f":1e-7}"#),
            r#"{"a":4995.00,"b":1E+2,"c":-0,"d":123456789012345678901,"e":0.1,"f":1e-7}"#
        );
    }

    #[test]
    fn lo_que_no_es_un_objeto_json_valido_se_rechaza_sin_citar_valores() {
        for malo in [
            "[1]",
            "5",
            "\"x\"",
            "null",
            "",
            "{",
            "{\"a\"}",
            "{\"a\":}",
            "{\"a\":1,}",
            "{\"a\":01}",
            "{\"a\":1.}",
            "{\"a\":+1}",
            "{\"a\":\"\\ud83d\"}",
            "{\"a\":\"x\ny\"}",
            "{\"a\":1} x",
            "{'a':1}",
        ] {
            assert!(canonical_text(malo).is_err(), "{malo:?}");
        }
        let motivo = canonical_text("{\"secreto\":tarjeta}").unwrap_err();
        assert!(!motivo.contains("tarjeta"), "{motivo}");
    }

    #[test]
    fn la_profundidad_esta_acotada() {
        let hondo = |n: usize| format!("{}1{}", "{\"a\":".repeat(n), "}".repeat(n));
        assert!(canonical_text(&hondo(100)).is_ok());
        assert!(canonical_text(&hondo(200)).is_err());
    }
}
