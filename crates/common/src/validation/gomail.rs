//! Faithful port of Go's `net/mail.ParseAddress` accept/reject behaviour
//! (`src/net/mail/message.go`, Go 1.25: `addrParser`, `parseSingleAddress`
//! :373, `parseAddress` :394, `consumeGroupList` :477, `consumeAddrSpec` :514,
//! `consumePhrase` :576, `consumeQuotedString` :629, `consumeAtom` :683,
//! `consumeDomainLiteral` :721, `consumeDisplayNameComment` :762, `skipCFWS`
//! :812, `consumeComment` :830, `decodeRFC2047Word` :856), used by upstream
//! `pkg/apis/certificates/validation/validation.go:824`
//! (`mail.ParseAddress(emailAddress)`).
//!
//! Only success/failure is observable to the caller, so the parsed `Name` /
//! `Address` values are not materialised. RFC 2047 decoding
//! (`mime.WordDecoder.Decode`, `src/mime/encodedword.go:198`) is reduced to its
//! three outcomes, which are the only ones `decodeRFC2047Word` distinguishes:
//! decoded, "charset reader error" (a hard error, `isEncoded=true`) and
//! "invalid word" (ignored, `isEncoded=false`).
//!
//! Working on bytes (like Go's `p.s[1:]` slicing) keeps `consumeComment`'s
//! byte-wise behaviour identical.

use base64::alphabet::STANDARD;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use base64::Engine;

/// `mail.ParseAddress(address)` returned `err == nil`.
pub fn parse_address_ok(address: &str) -> bool {
    AddrParser {
        s: address.as_bytes(),
    }
    .parse_single_address()
    .is_ok()
}

type R<T> = Result<T, ()>;

struct AddrParser<'a> {
    s: &'a [u8],
}

/// `utf8.DecodeRuneInString`: `(rune, size)`; `(0, 0)` at end of input and
/// `(U+FFFD, 1)` for an invalid encoding (`size == 1 && r == RuneError`).
/// A literal U+FFFD in the input decodes with size 3, so it is distinguished.
fn decode_rune(s: &[u8]) -> (char, usize) {
    let Some(&b0) = s.first() else {
        return ('\0', 0);
    };
    let n = match b0 {
        0x00..=0x7f => return (b0 as char, 1),
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf4 => 4,
        _ => return ('\u{fffd}', 1),
    };
    if s.len() < n {
        return ('\u{fffd}', 1);
    }
    match std::str::from_utf8(&s[..n]) {
        Ok(st) => (st.chars().next().unwrap_or('\u{fffd}'), n),
        Err(_) => ('\u{fffd}', 1),
    }
}

fn is_multibyte(r: char) -> bool {
    r as u32 >= 0x80
}
fn is_vchar(r: char) -> bool {
    ('!'..='~').contains(&r) || is_multibyte(r)
}
fn is_atext(r: char, dot: bool) -> bool {
    match r {
        '.' => dot,
        '(' | ')' | '<' | '>' | '[' | ']' | ':' | ';' | '@' | '\\' | ',' | '"' => false,
        _ => is_vchar(r),
    }
}
fn is_qtext(r: char) -> bool {
    if r == '\\' || r == '"' {
        return false;
    }
    is_vchar(r)
}
fn is_wsp(r: char) -> bool {
    r == ' ' || r == '\t'
}
fn is_dtext(r: char) -> bool {
    if r == '[' || r == ']' || r == '\\' {
        return false;
    }
    is_vchar(r)
}

impl<'a> AddrParser<'a> {
    fn empty(&self) -> bool {
        self.s.is_empty()
    }
    fn peek(&self) -> u8 {
        self.s[0]
    }
    fn consume(&mut self, c: u8) -> bool {
        if self.empty() || self.peek() != c {
            return false;
        }
        self.s = &self.s[1..];
        true
    }
    /// `strings.TrimLeft(p.s, " \t")`.
    fn skip_space(&mut self) {
        while let Some(&b) = self.s.first() {
            if b == b' ' || b == b'\t' {
                self.s = &self.s[1..];
            } else {
                break;
            }
        }
    }

    /// `parseSingleAddress` (:373).
    fn parse_single_address(&mut self) -> R<()> {
        let n = self.parse_address(true)?;
        if !self.skip_cfws() {
            return Err(());
        }
        if !self.empty() {
            return Err(());
        }
        // len(addrs) == 0 -> "empty group"; > 1 -> "group with multiple addresses".
        if n != 1 {
            return Err(());
        }
        Ok(())
    }

    /// `parseAddress` (:394); returns `len(addrs)`.
    fn parse_address(&mut self, handle_group: bool) -> R<usize> {
        self.skip_space();
        if self.empty() {
            return Err(());
        }

        // addr-spec has a more restricted grammar than name-addr, so try it
        // first and fall back to name-addr.
        if self.consume_addr_spec().is_ok() {
            self.skip_space();
            if !self.empty() && self.peek() == b'(' {
                self.consume_display_name_comment()?;
            }
            return Ok(1);
        }

        // display-name
        let mut display_name_atext = true;
        if self.peek() != b'<' {
            let name = self.consume_phrase()?;
            display_name_atext = name.iter().all(|&r| is_atext(r, true));
        }

        self.skip_space();
        if handle_group && self.consume(b':') {
            return self.consume_group_list();
        }
        // angle-addr = "<" addr-spec ">"
        if !self.consume(b'<') {
            // Both branches are errors ("missing '@' or angle-addr" /
            // "no angle-addr").
            let _ = display_name_atext;
            return Err(());
        }
        self.consume_addr_spec()?;
        if !self.consume(b'>') {
            return Err(());
        }
        Ok(1)
    }

    /// `consumeGroupList` (:477); returns `len(group)`.
    fn consume_group_list(&mut self) -> R<usize> {
        let mut group = 0usize;
        // handle empty group.
        self.skip_space();
        if self.consume(b';') {
            if !self.skip_cfws() {
                return Err(());
            }
            return Ok(group);
        }
        loop {
            self.skip_space();
            // embedded groups not allowed.
            group += self.parse_address(false)?;
            if !self.skip_cfws() {
                return Err(());
            }
            if self.consume(b';') {
                if !self.skip_cfws() {
                    return Err(());
                }
                break;
            }
            if !self.consume(b',') {
                return Err(());
            }
        }
        Ok(group)
    }

    /// `consumeAddrSpec` (:514); restores `p` on error.
    fn consume_addr_spec(&mut self) -> R<()> {
        let orig = self.s;
        let r = self.consume_addr_spec_inner();
        if r.is_err() {
            self.s = orig;
        }
        r
    }

    fn consume_addr_spec_inner(&mut self) -> R<()> {
        self.skip_space();
        if self.empty() {
            return Err(());
        }
        if self.peek() == b'"' {
            let local = self.consume_quoted_string()?;
            if local.is_empty() {
                return Err(());
            }
        } else {
            self.consume_atom(true, false)?;
        }

        if !self.consume(b'@') {
            return Err(());
        }

        self.skip_space();
        if self.empty() {
            return Err(());
        }
        if self.peek() == b'[' {
            self.consume_domain_literal()?;
        } else {
            self.consume_atom(true, false)?;
        }
        Ok(())
    }

    /// `consumePhrase` (:576); returns the (lossy) phrase characters, which
    /// only feed the `isAtext` error-message choice in `parseAddress`.
    fn consume_phrase(&mut self) -> R<Vec<char>> {
        let mut words: usize = 0;
        let mut phrase: Vec<char> = Vec::new();
        let mut err = false;
        loop {
            // obs-phrase allows CFWS after one word
            if words > 0 && !self.skip_cfws() {
                return Err(());
            }
            self.skip_space();
            if self.empty() {
                break;
            }
            let res: R<Vec<char>> = if self.peek() == b'"' {
                self.consume_quoted_string()
            } else {
                // We actually parse dot-atom here to be more permissive.
                match self.consume_atom(true, true) {
                    Ok(atom) => {
                        let text: String = atom.iter().collect();
                        decode_rfc2047_word(&text).map(|_| atom)
                    }
                    Err(e) => Err(e),
                }
            };
            match res {
                Ok(w) => {
                    if words > 0 {
                        phrase.push(' ');
                    }
                    phrase.extend(w);
                    words += 1;
                }
                Err(()) => {
                    err = true;
                    break;
                }
            }
        }
        // Ignore any error if we got at least one word.
        if err && words == 0 {
            return Err(());
        }
        Ok(phrase)
    }

    /// `consumeQuotedString` (:629).
    fn consume_quoted_string(&mut self) -> R<Vec<char>> {
        let mut i = 1usize;
        let mut qsb = Vec::new();
        let mut escaped = false;
        loop {
            let (r, size) = decode_rune(&self.s[i.min(self.s.len())..]);
            if size == 0 {
                return Err(()); // unclosed quoted-string
            }
            if size == 1 && r == '\u{fffd}' {
                return Err(());
            }
            if escaped {
                // quoted-pair = ("\" (VCHAR / WSP))
                if !is_vchar(r) && !is_wsp(r) {
                    return Err(());
                }
                qsb.push(r);
                escaped = false;
            } else if is_qtext(r) || is_wsp(r) {
                qsb.push(r);
            } else if r == '"' {
                break;
            } else if r == '\\' {
                escaped = true;
            } else {
                return Err(());
            }
            i += size;
        }
        self.s = &self.s[i + 1..];
        Ok(qsb)
    }

    /// `consumeAtom` (:683).
    fn consume_atom(&mut self, dot: bool, permissive: bool) -> R<Vec<char>> {
        let mut i = 0usize;
        loop {
            let (r, size) = decode_rune(&self.s[i..]);
            if size == 1 && r == '\u{fffd}' {
                return Err(());
            }
            if size == 0 || !is_atext(r, dot) {
                break;
            }
            i += size;
        }
        if i == 0 {
            return Err(());
        }
        let atom = &self.s[..i];
        self.s = &self.s[i..];
        if !permissive
            && (atom.starts_with(b".")
                || atom.windows(2).any(|w| w == b"..")
                || atom.ends_with(b"."))
        {
            return Err(());
        }
        Ok(String::from_utf8_lossy(atom).chars().collect())
    }

    /// `consumeDomainLiteral` (:721).
    fn consume_domain_literal(&mut self) -> R<()> {
        if !self.consume(b'[') {
            return Err(());
        }
        let mut dtext = Vec::new();
        loop {
            if self.empty() {
                return Err(());
            }
            if self.peek() == b']' {
                break;
            }
            let (r, size) = decode_rune(self.s);
            if size == 1 && r == '\u{fffd}' {
                return Err(());
            }
            if !is_dtext(r) {
                return Err(());
            }
            dtext.extend_from_slice(&self.s[..size]);
            self.s = &self.s[size..];
        }
        if !self.consume(b']') {
            return Err(());
        }
        // `net.ParseIP(dtext) == nil` -> error.
        let ok = std::str::from_utf8(&dtext)
            .ok()
            .is_some_and(|t| t.parse::<std::net::IpAddr>().is_ok());
        if ok {
            Ok(())
        } else {
            Err(())
        }
    }

    /// `consumeDisplayNameComment` (:762).
    fn consume_display_name_comment(&mut self) -> R<()> {
        if !self.consume(b'(') {
            return Err(());
        }
        let Some(comment) = self.consume_comment() else {
            return Err(());
        };
        // strings.FieldsFunc(comment, ' ' | '\t'), then decode each word.
        for word in comment
            .split(|&b| b == b' ' || b == b'\t')
            .filter(|w| !w.is_empty())
        {
            decode_rfc2047_word(&String::from_utf8_lossy(word))?;
        }
        Ok(())
    }

    /// `skipCFWS` (:812).
    fn skip_cfws(&mut self) -> bool {
        self.skip_space();
        loop {
            if !self.consume(b'(') {
                break;
            }
            if self.consume_comment().is_none() {
                return false;
            }
            self.skip_space();
        }
        true
    }

    /// `consumeComment` (:830); `None` when the parentheses are unbalanced.
    fn consume_comment(&mut self) -> Option<Vec<u8>> {
        // '(' already consumed.
        let mut depth = 1usize;
        let mut comment = Vec::new();
        loop {
            if self.empty() || depth == 0 {
                break;
            }
            if self.peek() == b'\\' && self.s.len() > 1 {
                self.s = &self.s[1..];
            } else if self.peek() == b'(' {
                depth += 1;
            } else if self.peek() == b')' {
                depth -= 1;
            }
            if depth > 0 {
                comment.push(self.s[0]);
            }
            self.s = &self.s[1..];
        }
        (depth == 0).then_some(comment)
    }
}

/// `addrParser.decodeRFC2047Word` (:856) with `p.dec == nil`
/// (`rfc2047Decoder`, whose `CharsetReader` always fails): `Ok(isEncoded)` or
/// `Err(())` for the charset-reader error. An undecodable word is ignored
/// (`Ok(false)`).
fn decode_rfc2047_word(s: &str) -> R<bool> {
    match mime_decode(s) {
        MimeDecode::Ok => Ok(true),
        MimeDecode::CharsetError => Err(()),
        MimeDecode::Invalid => Ok(false),
    }
}

enum MimeDecode {
    Ok,
    CharsetError,
    Invalid,
}

/// `mime.WordDecoder.Decode` (src/mime/encodedword.go:198) reduced to its
/// outcome class.
fn mime_decode(word: &str) -> MimeDecode {
    let b = word.as_bytes();
    if b.len() < 8
        || !word.starts_with("=?")
        || !word.ends_with("?=")
        || b.iter().filter(|&&c| c == b'?').count() != 4
    {
        return MimeDecode::Invalid;
    }
    let inner = &word[2..word.len() - 2];
    // strings.Cut(word, "?") twice.
    let Some((charset, rest)) = inner.split_once('?') else {
        return MimeDecode::Invalid;
    };
    if charset.is_empty() {
        return MimeDecode::Invalid;
    }
    let Some((encoding, text)) = rest.split_once('?') else {
        return MimeDecode::Invalid;
    };
    if encoding.len() != 1 {
        return MimeDecode::Invalid;
    }
    let decoded = match encoding.as_bytes()[0] {
        b'B' | b'b' => base64_std_decode(text.as_bytes()),
        b'Q' | b'q' => q_decode(text.as_bytes()),
        _ => false,
    };
    if !decoded {
        return MimeDecode::Invalid;
    }
    // WordDecoder.convert: utf-8 / iso-8859-1 / us-ascii are built in; any
    // other charset goes to CharsetReader, which errors.
    if ["utf-8", "iso-8859-1", "us-ascii"]
        .iter()
        .any(|c| c.eq_ignore_ascii_case(charset))
    {
        MimeDecode::Ok
    } else {
        MimeDecode::CharsetError
    }
}

/// `base64.StdEncoding.DecodeString`: padded, trailing bits not checked,
/// `\r` and `\n` ignored.
fn base64_std_decode(text: &[u8]) -> bool {
    let filtered: Vec<u8> = text
        .iter()
        .copied()
        .filter(|&c| c != b'\r' && c != b'\n')
        .collect();
    let engine = GeneralPurpose::new(
        &STANDARD,
        GeneralPurposeConfig::new()
            .with_decode_allow_trailing_bits(true)
            .with_decode_padding_mode(DecodePaddingMode::RequireCanonical),
    );
    engine.decode(filtered).is_ok()
}

/// `mime.qDecode` (src/mime/encodedword.go:362).
fn q_decode(s: &[u8]) -> bool {
    let hex = |c: u8| c.is_ascii_hexdigit();
    let mut i = 0;
    while i < s.len() {
        let c = s[i];
        if c == b'=' {
            if i + 2 >= s.len() {
                return false;
            }
            if !hex(s[i + 1]) || !hex(s[i + 2]) {
                return false;
            }
            i += 2;
        } else if !(c == b'_'
            || (b' '..=b'~').contains(&c)
            || c == b'\n'
            || c == b'\r'
            || c == b'\t')
        {
            return false;
        }
        i += 1;
    }
    true
}
