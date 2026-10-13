//! Go `regexp.Compile` syntax errors: a port of `regexp/syntax/parse.go`.
//!
//! `ValidateStructural` reports an invalid `pattern` with
//! `fmt.Sprintf("must be a valid regular expression, but isn't: %v", err)`
//! (`staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/validation.go:301`)
//! where `err` is the `*syntax.Error` from `regexp.Compile`. Its text is
//! `"error parsing regexp: " + code + ": `" + expr + "`"`
//! (`regexp/syntax/parse.go`, `func (e *Error) Error()`), so this module ports
//! the parser's error paths, codes and `expr` slices from Go 1.25.9
//! (`../kubernetes/.go-version`).
//!
//! Ported: `parse`, `parseRepeat`, `parseInt`, `parsePerlFlags`,
//! `parseClass`, `parseNamedClass`, `parseUnicodeClass` (with
//! `canonicalName` / `unicodeTable` name resolution), `parsePerlClassEscape`,
//! `parseEscape`, `parseRightParen`, `repeat`, `repeatIsValid`, `concat`,
//! `alternate`, `checkSize`/`calcSize` and `checkHeight`/`calcHeight`.
//!
//! Character classes are parsed for their errors only; their rune sets are
//! not built, so `maxRunes`, the `swapVerticalBar` class merge and `factor`
//! (which only reshape the tree and the allocation count behind the
//! `checkSize`/`checkHeight` gates) are not modelled.

use std::collections::HashMap;

const FOLD_CASE: u16 = 1;
const CLASS_NL: u16 = 1 << 2;
const DOT_NL: u16 = 1 << 3;
const ONE_LINE: u16 = 1 << 4;
const NON_GREEDY: u16 = 1 << 5;
const PERL_X: u16 = 1 << 6;
const UNICODE_GROUPS: u16 = 1 << 7;
/// `syntax.Perl`, the flags `regexp.Compile` parses with.
const PERL: u16 = CLASS_NL | ONE_LINE | PERL_X | UNICODE_GROUPS;

const MAX_HEIGHT: usize = 1000;
const INST_SIZE: i64 = 5 * 8;
const MAX_SIZE: i64 = (128 << 20) / INST_SIZE;
const MAX_RUNE: i32 = 0x10FFFF;

// `syntax.ErrorCode` strings.
const ERR_INVALID_CHAR_RANGE: &str = "invalid character class range";
const ERR_INVALID_ESCAPE: &str = "invalid escape sequence";
const ERR_INVALID_NAMED_CAPTURE: &str = "invalid named capture";
const ERR_INVALID_PERL_OP: &str = "invalid or unsupported Perl syntax";
const ERR_INVALID_REPEAT_OP: &str = "invalid nested repetition operator";
const ERR_INVALID_REPEAT_SIZE: &str = "invalid repeat count";
const ERR_MISSING_BRACKET: &str = "missing closing ]";
const ERR_MISSING_PAREN: &str = "missing closing )";
const ERR_MISSING_REPEAT_ARGUMENT: &str = "missing argument to repetition operator";
const ERR_TRAILING_BACKSLASH: &str = "trailing backslash at end of expression";
const ERR_UNEXPECTED_PAREN: &str = "unexpected )";
const ERR_NESTING_DEPTH: &str = "expression nests too deeply";
const ERR_LARGE: &str = "expression too large";

/// `*syntax.Error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub code: &'static str,
    pub expr: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "error parsing regexp: {}: `{}`", self.code, self.expr)
    }
}

impl std::error::Error for Error {}

type Res<T> = Result<T, Error>;

/// Names `unicodeTable` resolves after `canonicalName`: the keys of
/// `unicode.Categories` and `unicode.Scripts`, the canonicalised keys of
/// `unicode.CategoryAliases`, and the specials `Any`, `Assigned`, `Ascii`
/// and `Lc`. Matching is exact against the canonicalised input, so keys that
/// contain `_` (which `canonicalName` strips) are unreachable, as upstream.
static UNICODE_NAMES: &[&str] = &[
    "Any",
    "Assigned",
    "Ascii",
    "Lc",
    "Adlam",
    "Ahom",
    "Anatolian_Hieroglyphs",
    "Arabic",
    "Armenian",
    "Avestan",
    "Balinese",
    "Bamum",
    "Bassa_Vah",
    "Batak",
    "Bengali",
    "Bhaiksuki",
    "Bopomofo",
    "Brahmi",
    "Braille",
    "Buginese",
    "Buhid",
    "C",
    "Canadian_Aboriginal",
    "Carian",
    "Casedletter",
    "Caucasian_Albanian",
    "Cc",
    "Cf",
    "Chakma",
    "Cham",
    "Cherokee",
    "Chorasmian",
    "Closepunctuation",
    "Cn",
    "Cntrl",
    "Co",
    "Combiningmark",
    "Common",
    "Connectorpunctuation",
    "Control",
    "Coptic",
    "Cs",
    "Cuneiform",
    "Currencysymbol",
    "Cypriot",
    "Cypro_Minoan",
    "Cyrillic",
    "Dashpunctuation",
    "Decimalnumber",
    "Deseret",
    "Devanagari",
    "Digit",
    "Dives_Akuru",
    "Dogra",
    "Duployan",
    "Egyptian_Hieroglyphs",
    "Elbasan",
    "Elymaic",
    "Enclosingmark",
    "Ethiopic",
    "Finalpunctuation",
    "Format",
    "Georgian",
    "Glagolitic",
    "Gothic",
    "Grantha",
    "Greek",
    "Gujarati",
    "Gunjala_Gondi",
    "Gurmukhi",
    "Han",
    "Hangul",
    "Hanifi_Rohingya",
    "Hanunoo",
    "Hatran",
    "Hebrew",
    "Hiragana",
    "Imperial_Aramaic",
    "Inherited",
    "Initialpunctuation",
    "Inscriptional_Pahlavi",
    "Inscriptional_Parthian",
    "Javanese",
    "Kaithi",
    "Kannada",
    "Katakana",
    "Kawi",
    "Kayah_Li",
    "Kharoshthi",
    "Khitan_Small_Script",
    "Khmer",
    "Khojki",
    "Khudawadi",
    "L",
    "LC",
    "Lao",
    "Latin",
    "Lepcha",
    "Letter",
    "Letternumber",
    "Limbu",
    "Linear_A",
    "Linear_B",
    "Lineseparator",
    "Lisu",
    "Ll",
    "Lm",
    "Lo",
    "Lowercaseletter",
    "Lt",
    "Lu",
    "Lycian",
    "Lydian",
    "M",
    "Mahajani",
    "Makasar",
    "Malayalam",
    "Mandaic",
    "Manichaean",
    "Marchen",
    "Mark",
    "Masaram_Gondi",
    "Mathsymbol",
    "Mc",
    "Me",
    "Medefaidrin",
    "Meetei_Mayek",
    "Mende_Kikakui",
    "Meroitic_Cursive",
    "Meroitic_Hieroglyphs",
    "Miao",
    "Mn",
    "Modi",
    "Modifierletter",
    "Modifiersymbol",
    "Mongolian",
    "Mro",
    "Multani",
    "Myanmar",
    "N",
    "Nabataean",
    "Nag_Mundari",
    "Nandinagari",
    "Nd",
    "New_Tai_Lue",
    "Newa",
    "Nko",
    "Nl",
    "No",
    "Nonspacingmark",
    "Number",
    "Nushu",
    "Nyiakeng_Puachue_Hmong",
    "Ogham",
    "Ol_Chiki",
    "Old_Hungarian",
    "Old_Italic",
    "Old_North_Arabian",
    "Old_Permic",
    "Old_Persian",
    "Old_Sogdian",
    "Old_South_Arabian",
    "Old_Turkic",
    "Old_Uyghur",
    "Openpunctuation",
    "Oriya",
    "Osage",
    "Osmanya",
    "Other",
    "Otherletter",
    "Othernumber",
    "Otherpunctuation",
    "Othersymbol",
    "P",
    "Pahawh_Hmong",
    "Palmyrene",
    "Paragraphseparator",
    "Pau_Cin_Hau",
    "Pc",
    "Pd",
    "Pe",
    "Pf",
    "Phags_Pa",
    "Phoenician",
    "Pi",
    "Po",
    "Privateuse",
    "Ps",
    "Psalter_Pahlavi",
    "Punct",
    "Punctuation",
    "Rejang",
    "Runic",
    "S",
    "Samaritan",
    "Saurashtra",
    "Sc",
    "Separator",
    "Sharada",
    "Shavian",
    "Siddham",
    "SignWriting",
    "Sinhala",
    "Sk",
    "Sm",
    "So",
    "Sogdian",
    "Sora_Sompeng",
    "Soyombo",
    "Spaceseparator",
    "Spacingmark",
    "Sundanese",
    "Surrogate",
    "Syloti_Nagri",
    "Symbol",
    "Syriac",
    "Tagalog",
    "Tagbanwa",
    "Tai_Le",
    "Tai_Tham",
    "Tai_Viet",
    "Takri",
    "Tamil",
    "Tangsa",
    "Tangut",
    "Telugu",
    "Thaana",
    "Thai",
    "Tibetan",
    "Tifinagh",
    "Tirhuta",
    "Titlecaseletter",
    "Toto",
    "Ugaritic",
    "Unassigned",
    "Uppercaseletter",
    "Vai",
    "Vithkuqi",
    "Wancho",
    "Warang_Citi",
    "Yezidi",
    "Yi",
    "Z",
    "Zanabazar_Square",
    "Zl",
    "Zp",
    "Zs",
];

/// `posixGroup` keys (perl_groups.go), without the optional `^`.
const POSIX_CLASSES: [&str; 14] = [
    "alnum", "alpha", "ascii", "blank", "cntrl", "digit", "graph", "lower", "print", "punct",
    "space", "upper", "word", "xdigit",
];

/// `canonicalName` (parse.go): a leading uppercase letter, then lowercase
/// letters, with `_`, `-` and space removed.
fn canonical_name(name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(name.len());
    let mut first = true;
    for &c in name.as_bytes() {
        let mut c = c;
        if c == b'_' || c == b'-' || c == b' ' {
            continue;
        } else if first {
            if c.is_ascii_lowercase() {
                c -= b'a' - b'A';
            }
            first = false;
        } else if c.is_ascii_uppercase() {
            c += b'a' - b'A';
        }
        out.push(c);
    }
    out
}

/// `unicodeTable(name) != nil`.
fn unicode_table_exists(name: &str) -> bool {
    let canon = canonical_name(name);
    UNICODE_NAMES
        .iter()
        .any(|n| n.as_bytes() == canon.as_slice())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Literal,
    CharClass,
    AnyChar,
    AnyCharNotNL,
    BeginLine,
    EndLine,
    BeginText,
    EndText,
    WordBoundary,
    NoWordBoundary,
    Capture,
    Star,
    Plus,
    Quest,
    Repeat,
    Concat,
    Alternate,
    EmptyMatch,
    NoMatch,
    // Pseudo-ops for the parse stack.
    LeftParen,
    VerticalBar,
}

impl Op {
    fn is_pseudo(self) -> bool {
        matches!(self, Op::LeftParen | Op::VerticalBar)
    }
}

struct Node {
    op: Op,
    subs: Vec<usize>,
    min: i32,
    max: i32,
    flags: u16,
    cap: i32,
    /// `len(re.Rune)` of a `Literal`.
    runes: usize,
}

struct Parser<'a> {
    s: &'a str,
    flags: u16,
    stack: Vec<usize>,
    nodes: Vec<Node>,
    num_cap: i32,
    /// `p.numRegexp`: regexps allocated (not counting free-list reuse).
    num_regexp: i64,
    /// Length of `p.free`.
    free: i64,
    /// `p.repeats`: product of all repetitions seen.
    repeats: i64,
    /// `p.size`: nil until size tracking starts.
    size: Option<HashMap<usize, i64>>,
    /// `p.height`: nil until height tracking starts.
    height: Option<HashMap<usize, usize>>,
}

fn err(code: &'static str, expr: &str) -> Error {
    Error {
        code,
        expr: expr.to_string(),
    }
}

/// `regexp.Compile`'s syntax check: `Ok(())` when Go would accept `expr`.
pub fn compile(expr: &str) -> Result<(), Error> {
    Parser {
        s: expr,
        flags: PERL,
        stack: Vec::new(),
        nodes: Vec::new(),
        num_cap: 0,
        num_regexp: 0,
        free: 0,
        repeats: 0,
        size: None,
        height: None,
    }
    .parse()
}

fn is_alnum(c: char) -> bool {
    c.is_ascii_alphanumeric()
}

/// `unhex`.
fn unhex(c: i32) -> i32 {
    match u32::try_from(c).ok().and_then(char::from_u32) {
        Some(ch @ '0'..='9') => ch as i32 - '0' as i32,
        Some(ch @ 'a'..='f') => ch as i32 - 'a' as i32 + 10,
        Some(ch @ 'A'..='F') => ch as i32 - 'A' as i32 + 10,
        _ => -1,
    }
}

impl<'a> Parser<'a> {
    fn bytes(&self) -> &'a [u8] {
        self.s.as_bytes()
    }

    /// `nextRune`: at the end of the string Go's `utf8.DecodeRuneInString`
    /// yields `(RuneError, 0)`, which `nextRune` does not treat as an error.
    fn next_rune(&self, pos: usize) -> (char, usize) {
        match self.s[pos..].chars().next() {
            Some(c) => (c, pos + c.len_utf8()),
            None => ('\u{FFFD}', pos),
        }
    }

    /// `newRegexp`: takes from the free list, else allocates.
    fn new_node(&mut self, op: Op) -> usize {
        if self.free > 0 {
            self.free -= 1;
        } else {
            self.num_regexp += 1;
        }
        self.nodes.push(Node {
            op,
            subs: Vec::new(),
            min: 0,
            max: 0,
            flags: self.flags,
            cap: 0,
            runes: usize::from(op == Op::Literal),
        });
        self.nodes.len() - 1
    }

    /// `reuse`: passes a removed regexp to the free list.
    fn reuse(&mut self, re: usize) {
        if let Some(h) = self.height.as_mut() {
            h.remove(&re);
        }
        self.free += 1;
    }

    // ---- limits (parse.go checkLimits, checkSize, calcSize, checkHeight) ----

    fn check_limits(&mut self, re: usize) -> Res<()> {
        self.check_size(re)?;
        self.check_height(re)
    }

    fn check_size(&mut self, re: usize) -> Res<()> {
        if self.size.is_none() {
            // We haven't started tracking size yet. Do a relatively cheap
            // check to see if we need to start: maintain the product of all
            // the repeats we've seen and don't track if the total number of
            // regexp nodes we've seen times the repeat product is in budget.
            if self.repeats == 0 {
                self.repeats = 1;
            }
            if self.nodes[re].op == Op::Repeat {
                let mut n = self.nodes[re].max;
                if n == -1 {
                    n = self.nodes[re].min;
                }
                if n <= 0 {
                    n = 1;
                }
                if i64::from(n) > MAX_SIZE / self.repeats {
                    self.repeats = MAX_SIZE;
                } else {
                    self.repeats *= i64::from(n);
                }
            }
            if self.num_regexp < MAX_SIZE / self.repeats {
                return Ok(());
            }
            // We need to start tracking size. Make the map and belatedly
            // populate it with info about everything constructed so far.
            self.size = Some(HashMap::new());
            for re in self.stack.clone() {
                self.check_size(re)?;
            }
        }
        if self.calc_size(re, true) > MAX_SIZE {
            return Err(err(ERR_LARGE, self.s));
        }
        Ok(())
    }

    fn check_height(&mut self, re: usize) -> Res<()> {
        if self.num_regexp < MAX_HEIGHT as i64 {
            return Ok(());
        }
        if self.height.is_none() {
            self.height = Some(HashMap::new());
            for re in self.stack.clone() {
                self.check_height(re)?;
            }
        }
        if self.calc_height(re, true) > MAX_HEIGHT {
            return Err(err(ERR_NESTING_DEPTH, self.s));
        }
        Ok(())
    }

    fn calc_size(&mut self, re: usize, force: bool) -> i64 {
        if !force {
            if let Some(&size) = self.size.as_ref().and_then(|m| m.get(&re)) {
                return size;
            }
        }
        let (op, min, max, runes, subs) = {
            let n = &self.nodes[re];
            (n.op, n.min, n.max, n.runes, n.subs.clone())
        };
        let mut size: i64 = 0;
        match op {
            Op::Literal => size = runes as i64,
            Op::Capture | Op::Star => size = 2 + self.calc_size(subs[0], false),
            Op::Plus | Op::Quest => size = 1 + self.calc_size(subs[0], false),
            Op::Concat => {
                for &sub in &subs {
                    size += self.calc_size(sub, false);
                }
            }
            Op::Alternate => {
                for &sub in &subs {
                    size += self.calc_size(sub, false);
                }
                if subs.len() > 1 {
                    size += subs.len() as i64 - 1;
                }
            }
            Op::Repeat => {
                let sub = self.calc_size(subs[0], false);
                if max == -1 {
                    size = if min == 0 {
                        2 + sub
                    } else {
                        1 + i64::from(min) * sub
                    };
                } else {
                    size = i64::from(max) * sub + i64::from(max - min);
                }
            }
            _ => {}
        }
        let size = size.max(1);
        if let Some(m) = self.size.as_mut() {
            m.insert(re, size);
        }
        size
    }

    fn calc_height(&mut self, re: usize, force: bool) -> usize {
        if !force {
            if let Some(&h) = self.height.as_ref().and_then(|m| m.get(&re)) {
                return h;
            }
        }
        let subs = self.nodes[re].subs.clone();
        let mut h = 1;
        for sub in subs {
            let hsub = self.calc_height(sub, false);
            if h < 1 + hsub {
                h = 1 + hsub;
            }
        }
        if let Some(m) = self.height.as_mut() {
            m.insert(re, h);
        }
        h
    }

    // ---- parse stack ----

    /// `maybeConcat(-1, 0)`: incremental concatenation of literal runes into
    /// string nodes, called before each push (and by `concat`).
    fn maybe_concat(&mut self) {
        let n = self.stack.len();
        if n < 2 {
            return;
        }
        let re1 = self.stack[n - 1];
        let re2 = self.stack[n - 2];
        let (a, b) = (&self.nodes[re1], &self.nodes[re2]);
        if a.op != Op::Literal || b.op != Op::Literal || a.flags & FOLD_CASE != b.flags & FOLD_CASE
        {
            return;
        }
        // Push re1 into re2.
        self.nodes[re2].runes += self.nodes[re1].runes;
        self.stack.truncate(n - 1);
        self.reuse(re1);
    }

    fn push(&mut self, re: usize) -> Res<usize> {
        self.maybe_concat();
        self.stack.push(re);
        self.check_limits(re)?;
        Ok(re)
    }

    fn literal(&mut self) -> Res<()> {
        let re = self.new_node(Op::Literal);
        self.push(re).map(|_| ())
    }

    fn op(&mut self, op: Op) -> Res<usize> {
        let re = self.new_node(op);
        self.push(re)
    }

    /// `repeat`: `before`/`after` are byte offsets of the operator start and
    /// the text following it; `last_repeat` is the start of the previous
    /// repetition operator, if the previous token was one.
    fn repeat(
        &mut self,
        op: Op,
        min: i32,
        max: i32,
        before: usize,
        mut after: usize,
        last_repeat: Option<usize>,
    ) -> Res<usize> {
        let mut flags = self.flags;
        if self.flags & PERL_X != 0 {
            if after < self.s.len() && self.bytes()[after] == b'?' {
                after += 1;
                flags ^= NON_GREEDY;
            }
            if let Some(last) = last_repeat {
                // In Perl it is not allowed to stack repetition operators.
                return Err(err(ERR_INVALID_REPEAT_OP, &self.s[last..after]));
            }
        }
        let missing = || err(ERR_MISSING_REPEAT_ARGUMENT, &self.s[before..after]);
        let n = self.stack.len();
        if n == 0 {
            return Err(missing());
        }
        let sub = self.stack[n - 1];
        if self.nodes[sub].op.is_pseudo() {
            return Err(missing());
        }

        let re = self.new_node(op);
        self.nodes[re].min = min;
        self.nodes[re].max = max;
        self.nodes[re].flags = flags;
        self.nodes[re].subs = vec![sub];
        self.stack[n - 1] = re;
        self.check_limits(re)?;

        if op == Op::Repeat && (min >= 2 || max >= 2) && !self.repeat_is_valid(re, 1000) {
            return Err(err(ERR_INVALID_REPEAT_SIZE, &self.s[before..after]));
        }
        Ok(after)
    }

    /// `repeatIsValid`.
    fn repeat_is_valid(&self, re: usize, mut n: i32) -> bool {
        let node = &self.nodes[re];
        if node.op == Op::Repeat {
            let mut m = node.max;
            if m == 0 {
                return true;
            }
            if m < 0 {
                m = node.min;
            }
            if m > n {
                return false;
            }
            if m > 0 {
                n /= m;
            }
        }
        node.subs.iter().all(|&sub| self.repeat_is_valid(sub, n))
    }

    /// `collapse` without `factor` (alternation factoring only reshapes the
    /// tree).
    fn collapse(&mut self, subs: Vec<usize>, op: Op) -> usize {
        if subs.len() == 1 {
            return subs[0];
        }
        let re = self.new_node(op);
        let mut out = Vec::new();
        for sub in subs {
            if self.nodes[sub].op == op {
                out.extend(self.nodes[sub].subs.clone());
                self.reuse(sub);
            } else {
                out.push(sub);
            }
        }
        self.nodes[re].subs = out;
        re
    }

    fn split_at_pseudo(&mut self) -> Vec<usize> {
        let mut i = self.stack.len();
        while i > 0 && !self.nodes[self.stack[i - 1]].op.is_pseudo() {
            i -= 1;
        }
        self.stack.split_off(i)
    }

    fn concat(&mut self) -> Res<()> {
        self.maybe_concat();
        let subs = self.split_at_pseudo();
        if subs.is_empty() {
            let re = self.new_node(Op::EmptyMatch);
            return self.push(re).map(|_| ());
        }
        let re = self.collapse(subs, Op::Concat);
        self.push(re).map(|_| ())
    }

    fn alternate(&mut self) -> Res<()> {
        let subs = self.split_at_pseudo();
        if subs.is_empty() {
            let re = self.new_node(Op::NoMatch);
            return self.push(re).map(|_| ());
        }
        let re = self.collapse(subs, Op::Alternate);
        self.push(re).map(|_| ())
    }

    fn swap_vertical_bar(&mut self) -> bool {
        let n = self.stack.len();
        if n >= 2 {
            let re1 = self.stack[n - 1];
            let re2 = self.stack[n - 2];
            if self.nodes[re2].op == Op::VerticalBar {
                self.stack[n - 2] = re1;
                self.stack[n - 1] = re2;
                return true;
            }
        }
        false
    }

    fn parse_vertical_bar(&mut self) -> Res<()> {
        self.concat()?;
        if !self.swap_vertical_bar() {
            self.op(Op::VerticalBar)?;
        }
        Ok(())
    }

    fn parse_right_paren(&mut self) -> Res<()> {
        self.concat()?;
        if self.swap_vertical_bar() {
            self.stack.pop();
        }
        self.alternate()?;

        let n = self.stack.len();
        if n < 2 {
            return Err(err(ERR_UNEXPECTED_PAREN, self.s));
        }
        let re1 = self.stack[n - 1];
        let re2 = self.stack[n - 2];
        self.stack.truncate(n - 2);
        if self.nodes[re2].op != Op::LeftParen {
            return Err(err(ERR_UNEXPECTED_PAREN, self.s));
        }
        // Restore flags at time of paren.
        self.flags = self.nodes[re2].flags;
        if self.nodes[re2].cap == 0 {
            // Just for grouping.
            self.push(re1)?;
        } else {
            self.nodes[re2].op = Op::Capture;
            self.nodes[re2].subs = vec![re1];
            self.push(re2)?;
        }
        Ok(())
    }

    // ---- parse ----

    fn parse(&mut self) -> Res<()> {
        let s = self.s;
        let b = s.as_bytes();
        let n = b.len();
        let mut t = 0usize;
        let mut last_repeat: Option<usize> = None;
        while t < n {
            let mut repeat: Option<usize> = None;
            match b[t] {
                b'(' => {
                    if self.flags & PERL_X != 0 && n - t >= 2 && b[t + 1] == b'?' {
                        // Flag changes and non-capturing groups.
                        t = self.parse_perl_flags(t)?;
                    } else {
                        self.num_cap += 1;
                        let re = self.op(Op::LeftParen)?;
                        self.nodes[re].cap = self.num_cap;
                        t += 1;
                    }
                }
                b'|' => {
                    self.parse_vertical_bar()?;
                    t += 1;
                }
                b')' => {
                    self.parse_right_paren()?;
                    t += 1;
                }
                b'^' => {
                    self.op(if self.flags & ONE_LINE != 0 {
                        Op::BeginText
                    } else {
                        Op::BeginLine
                    })?;
                    t += 1;
                }
                b'$' => {
                    self.op(if self.flags & ONE_LINE != 0 {
                        Op::EndText
                    } else {
                        Op::EndLine
                    })?;
                    t += 1;
                }
                b'.' => {
                    self.op(if self.flags & DOT_NL != 0 {
                        Op::AnyChar
                    } else {
                        Op::AnyCharNotNL
                    })?;
                    t += 1;
                }
                b'[' => {
                    t = self.parse_class(t)?;
                }
                b'*' | b'+' | b'?' => {
                    let before = t;
                    let op = match b[t] {
                        b'*' => Op::Star,
                        b'+' => Op::Plus,
                        _ => Op::Quest,
                    };
                    let after = self.repeat(op, 0, 0, before, t + 1, last_repeat)?;
                    repeat = Some(before);
                    t = after;
                }
                b'{' => {
                    let before = t;
                    let (min, max, after, ok) = self.parse_repeat(t);
                    if !ok {
                        // If the repeat cannot be parsed, { is a literal.
                        self.literal()?;
                        t += 1;
                    } else {
                        if !(0..=1000).contains(&min) || max > 1000 || (max >= 0 && min > max) {
                            // Numbers were too big, or max is present and min > max.
                            return Err(err(ERR_INVALID_REPEAT_SIZE, &s[before..after]));
                        }
                        let after =
                            self.repeat(Op::Repeat, min, max, before, after, last_repeat)?;
                        repeat = Some(before);
                        t = after;
                    }
                }
                b'\\' => {
                    'esc: {
                        if self.flags & PERL_X != 0 && n - t >= 2 {
                            match b[t + 1] {
                                b'A' => {
                                    self.op(Op::BeginText)?;
                                    t += 2;
                                    break 'esc;
                                }
                                b'b' => {
                                    self.op(Op::WordBoundary)?;
                                    t += 2;
                                    break 'esc;
                                }
                                b'B' => {
                                    self.op(Op::NoWordBoundary)?;
                                    t += 2;
                                    break 'esc;
                                }
                                b'C' => {
                                    // any byte; not supported
                                    return Err(err(ERR_INVALID_ESCAPE, &s[t..t + 2]));
                                }
                                b'Q' => {
                                    // \Q ... \E: the ... is always literals
                                    let rest = &s[t + 2..];
                                    let (lit, next) = match rest.find("\\E") {
                                        Some(i) => (&rest[..i], t + 2 + i + 2),
                                        None => (rest, n),
                                    };
                                    t = next;
                                    for _ in lit.chars() {
                                        self.literal()?;
                                    }
                                    break 'esc;
                                }
                                b'z' => {
                                    self.op(Op::EndText)?;
                                    t += 2;
                                    break 'esc;
                                }
                                _ => {}
                            }
                        }

                        // Look for Unicode character group like \p{Han}
                        if n - t >= 2 && (b[t + 1] == b'p' || b[t + 1] == b'P') {
                            if let Some(rest) = self.parse_unicode_class(t)? {
                                self.op(Op::CharClass)?;
                                t = rest;
                                break 'esc;
                            }
                        }

                        // Perl character class escape.
                        if let Some(rest) = self.parse_perl_class_escape(t) {
                            self.op(Op::CharClass)?;
                            t = rest;
                            break 'esc;
                        }

                        // Ordinary single-character escape.
                        let (_, rest) = self.parse_escape(t)?;
                        t = rest;
                        self.literal()?;
                    }
                }
                _ => {
                    let (_, next) = self.next_rune(t);
                    t = next;
                    self.literal()?;
                }
            }
            last_repeat = repeat;
        }

        self.concat()?;
        if self.swap_vertical_bar() {
            // pop vertical bar
            self.stack.pop();
        }
        self.alternate()?;

        if self.stack.len() != 1 {
            return Err(err(ERR_MISSING_PAREN, s));
        }
        Ok(())
    }

    /// `parseRepeat`: `{min}`, `{min,}` or `{min,max}` at `pos`. Returns
    /// `(min, max, rest, ok)`.
    fn parse_repeat(&self, pos: usize) -> (i32, i32, usize, bool) {
        let b = self.bytes();
        let n = b.len();
        if pos >= n || b[pos] != b'{' {
            return (0, 0, 0, false);
        }
        let mut s = pos + 1;
        let (mut min, s1, ok1) = self.parse_int(s);
        if !ok1 {
            return (0, 0, 0, false);
        }
        s = s1;
        if s >= n {
            return (0, 0, 0, false);
        }
        let max;
        if b[s] != b',' {
            max = min;
        } else {
            s += 1;
            if s >= n {
                return (0, 0, 0, false);
            }
            if b[s] == b'}' {
                max = -1;
            } else {
                let (m, s2, ok2) = self.parse_int(s);
                if !ok2 {
                    return (0, 0, 0, false);
                }
                s = s2;
                max = m;
                if max < 0 {
                    // parseInt found too big a number
                    min = -1;
                }
            }
        }
        if s >= n || b[s] != b'}' {
            return (0, 0, 0, false);
        }
        (min, max, s + 1, true)
    }

    /// `parseInt`: a decimal integer, `-1` when too big.
    fn parse_int(&self, pos: usize) -> (i32, usize, bool) {
        let b = self.bytes();
        let n = b.len();
        if pos >= n || !b[pos].is_ascii_digit() {
            return (0, pos, false);
        }
        // Disallow leading zeros.
        if n - pos >= 2 && b[pos] == b'0' && b[pos + 1].is_ascii_digit() {
            return (0, pos, false);
        }
        let mut end = pos;
        while end < n && b[end].is_ascii_digit() {
            end += 1;
        }
        let mut v: i32 = 0;
        for &d in &b[pos..end] {
            // Avoid overflow.
            if v >= 100_000_000 {
                v = -1;
                break;
            }
            v = v * 10 + i32::from(d - b'0');
        }
        (v, end, true)
    }

    /// `parsePerlFlags`: `pos` is at `(?`. Returns the offset after the
    /// group opener or flag setting.
    fn parse_perl_flags(&mut self, pos: usize) -> Res<usize> {
        let s = self.s;
        let b = s.as_bytes();
        let t = &s[pos..];
        let tb = t.as_bytes();

        // Named captures: (?P<name>expr) and (?<name>expr).
        let starts_with_p = tb.len() > 4 && tb[2] == b'P' && tb[3] == b'<';
        let starts_with_name = tb.len() > 3 && tb[2] == b'<';

        if starts_with_p || starts_with_name {
            // position of expr start
            let expr_start = if starts_with_name { 3 } else { 4 };
            // Pull out name.
            let Some(end) = t.find('>') else {
                return Err(err(ERR_INVALID_NAMED_CAPTURE, t));
            };
            let capture = &t[..end + 1]; // "(?P<name>" or "(?<name>"
            let name = &t[expr_start..end];
            if !is_valid_capture_name(name) {
                return Err(err(ERR_INVALID_NAMED_CAPTURE, capture));
            }
            // Like ordinary capture, but named.
            self.num_cap += 1;
            let re = self.op(Op::LeftParen)?;
            self.nodes[re].cap = self.num_cap;
            return Ok(pos + end + 1);
        }

        // Non-capturing group. Might also twiddle Perl flags.
        let mut cur = pos + 2; // skip (?
        let mut flags = self.flags;
        let mut sign = 1;
        let mut saw_flag = false;
        while cur < b.len() {
            let (c, next) = self.next_rune(cur);
            cur = next;
            match c {
                // Flags.
                'i' => {
                    flags |= FOLD_CASE;
                    saw_flag = true;
                }
                'm' => {
                    flags &= !ONE_LINE;
                    saw_flag = true;
                }
                's' => {
                    flags |= DOT_NL;
                    saw_flag = true;
                }
                'U' => {
                    flags |= NON_GREEDY;
                    saw_flag = true;
                }
                // Switch to negation.
                '-' => {
                    if sign < 0 {
                        break;
                    }
                    sign = -1;
                    // Invert flags so that | above turn into &^ and vice versa.
                    flags = !flags;
                    saw_flag = false;
                }
                // End of flags, starting group or not.
                ':' | ')' => {
                    if sign < 0 {
                        if !saw_flag {
                            break;
                        }
                        flags = !flags;
                    }
                    if c == ':' {
                        // Open new group
                        self.op(Op::LeftParen)?;
                    }
                    self.flags = flags;
                    return Ok(cur);
                }
                _ => break,
            }
        }
        Err(err(ERR_INVALID_PERL_OP, &s[pos..cur]))
    }

    /// `parseEscape`: the escape sequence at `pos` (a backslash). Returns the
    /// rune and the offset after it.
    fn parse_escape(&self, pos: usize) -> Res<(i32, usize)> {
        let s = self.s;
        let b = s.as_bytes();
        let n = b.len();
        let mut t = pos + 1;
        if t >= n {
            return Err(err(ERR_TRAILING_BACKSLASH, ""));
        }
        let (c, nt) = self.next_rune(t);
        t = nt;
        let is_octal = |t: usize| t < n && (b'0'..=b'7').contains(&b[t]);

        // `break 'bad` is Go's `break Switch`: fall out to the error.
        'bad: {
            match c {
                '1'..='7' | '0' => {
                    // Single non-zero digit is a backreference; not supported
                    if c != '0' && !is_octal(t) {
                        break 'bad;
                    }
                    // Consume up to three octal digits; already have one.
                    let mut r = c as i32 - '0' as i32;
                    for _ in 1..3 {
                        if !is_octal(t) {
                            break;
                        }
                        r = r * 8 + i32::from(b[t] - b'0');
                        t += 1;
                    }
                    return Ok((r, t));
                }
                // Hexadecimal escapes.
                'x' => {
                    if t >= n {
                        break 'bad;
                    }
                    let (c, nt) = self.next_rune(t);
                    t = nt;
                    if c == '{' {
                        // Any number of digits in braces.
                        let mut nhex = 0;
                        let mut r: i32 = 0;
                        loop {
                            if t >= n {
                                break 'bad;
                            }
                            let (c, nt) = self.next_rune(t);
                            t = nt;
                            if c == '}' {
                                break;
                            }
                            let v = unhex(c as i32);
                            if v < 0 {
                                break 'bad;
                            }
                            r = r * 16 + v;
                            if r > MAX_RUNE {
                                break 'bad;
                            }
                            nhex += 1;
                        }
                        if nhex == 0 {
                            break 'bad;
                        }
                        return Ok((r, t));
                    }

                    // Easy case: two hex digits.
                    let x = unhex(c as i32);
                    let (c2, nt) = self.next_rune(t);
                    t = nt;
                    let y = unhex(c2 as i32);
                    if x < 0 || y < 0 {
                        break 'bad;
                    }
                    return Ok((x * 16 + y, t));
                }
                // C escapes. There is no case 'b'.
                'a' => return Ok((7, t)),
                'f' => return Ok((12, t)),
                'n' => return Ok((10, t)),
                'r' => return Ok((13, t)),
                't' => return Ok((9, t)),
                'v' => return Ok((11, t)),
                _ => {
                    if (c as u32) < 0x80 && !is_alnum(c) {
                        // Escaped non-word characters are always themselves.
                        return Ok((c as i32, t));
                    }
                }
            }
        }
        Err(err(ERR_INVALID_ESCAPE, &s[pos..t]))
    }

    /// `parseClassChar`.
    fn parse_class_char(&self, pos: usize, whole_class: usize) -> Res<(i32, usize)> {
        if pos >= self.s.len() {
            return Err(err(ERR_MISSING_BRACKET, &self.s[whole_class..]));
        }
        // Allow regular escape sequences even though many need not be
        // escaped in this context.
        if self.bytes()[pos] == b'\\' {
            return self.parse_escape(pos);
        }
        let (c, next) = self.next_rune(pos);
        Ok((c as i32, next))
    }

    /// `parsePerlClassEscape`: `\d \D \s \S \w \W` at `pos`.
    fn parse_perl_class_escape(&self, pos: usize) -> Option<usize> {
        let b = self.bytes();
        if self.flags & PERL_X == 0 || b.len() - pos < 2 || b[pos] != b'\\' {
            return None;
        }
        matches!(b[pos + 1], b'd' | b'D' | b's' | b'S' | b'w' | b'W').then_some(pos + 2)
    }

    /// `parseNamedClass`: a POSIX class like `[:alnum:]` at `pos`.
    fn parse_named_class(&self, pos: usize) -> Res<Option<usize>> {
        let s = self.s;
        let b = s.as_bytes();
        if b.len() - pos < 2 || b[pos] != b'[' || b[pos + 1] != b':' {
            return Ok(None);
        }
        let Some(i) = s[pos + 2..].find(":]") else {
            return Ok(None);
        };
        let end = pos + 2 + i + 2;
        let name = &s[pos..end];
        let inner = &name[2..name.len() - 2];
        let inner = inner.strip_prefix('^').unwrap_or(inner);
        if !POSIX_CLASSES.contains(&inner) {
            return Err(err(ERR_INVALID_CHAR_RANGE, name));
        }
        Ok(Some(end))
    }

    /// `parseUnicodeClass`: `\p{Han}`, `\pL`, `\P{^Han}` at `pos`.
    fn parse_unicode_class(&self, pos: usize) -> Res<Option<usize>> {
        let s = self.s;
        let b = s.as_bytes();
        if self.flags & UNICODE_GROUPS == 0
            || b.len() - pos < 2
            || b[pos] != b'\\'
            || (b[pos + 1] != b'p' && b[pos + 1] != b'P')
        {
            return Ok(None);
        }
        // Committed to parse or return error.
        let (c, mut t) = self.next_rune(pos + 2);
        let (seq, name): (&str, &str);
        if c != '{' {
            // Single-letter name.
            seq = &s[pos..t];
            name = &seq[2..];
        } else {
            // Name is in braces.
            let Some(end) = s[pos..].find('}').map(|i| pos + i) else {
                return Err(err(ERR_INVALID_CHAR_RANGE, &s[pos..]));
            };
            seq = &s[pos..end + 1];
            t = end + 1;
            name = &s[pos + 3..end];
        }
        // Group can have leading negation too.
        let name = name.strip_prefix('^').unwrap_or(name);
        if !unicode_table_exists(name) {
            return Err(err(ERR_INVALID_CHAR_RANGE, seq));
        }
        Ok(Some(t))
    }

    /// `parseClass`: a bracketed character class at `pos`.
    fn parse_class(&mut self, pos: usize) -> Res<usize> {
        let s = self.s;
        let b = s.as_bytes();
        let n = b.len();
        let mut t = pos + 1; // chop [
        if t < n && b[t] == b'^' {
            t += 1;
        }

        let mut first = true; // ] and - are okay as first char in class
        while t >= n || b[t] != b']' || first {
            first = false;

            // Look for POSIX [:alnum:] etc.
            if n - t > 2 && b[t] == b'[' && b[t + 1] == b':' {
                if let Some(nt) = self.parse_named_class(t)? {
                    t = nt;
                    continue;
                }
            }

            // Look for Unicode character group like \p{Han}.
            if let Some(nt) = self.parse_unicode_class(t.min(n))? {
                t = nt;
                continue;
            }

            // Look for Perl character class symbols (extension).
            if t < n {
                if let Some(nt) = self.parse_perl_class_escape(t) {
                    t = nt;
                    continue;
                }
            }

            // Single character or simple range.
            let rng = t;
            let (lo, nt) = self.parse_class_char(t, pos)?;
            t = nt;
            // [a-] means (a|-) so check for final ].
            if n - t >= 2 && b[t] == b'-' && b[t + 1] != b']' {
                t += 1;
                let (hi, nt) = self.parse_class_char(t, pos)?;
                t = nt;
                if hi < lo {
                    return Err(err(ERR_INVALID_CHAR_RANGE, &s[rng..t]));
                }
            }
        }
        t += 1; // chop ]
        self.op(Op::CharClass)?;
        Ok(t)
    }
}

/// `isValidCaptureName`: `[A-Za-z0-9_]+`.
fn is_valid_capture_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c == '_' || is_alnum(c))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `regexp.Compile(pattern)` outcomes recorded under Go 1.25.9
    /// (`None` = accepted, else `err.Error()`).
    const GO_OUTCOMES: &[(&str, Option<&str>)] = &[
        (
            r#"("#,
            Some(r#"error parsing regexp: missing closing ): `(`"#),
        ),
        (
            r#"(abc"#,
            Some(r#"error parsing regexp: missing closing ): `(abc`"#),
        ),
        (
            r#"abc)"#,
            Some(r#"error parsing regexp: unexpected ): `abc)`"#),
        ),
        (r#")"#, Some(r#"error parsing regexp: unexpected ): `)`"#)),
        (
            r#"a**"#,
            Some(r#"error parsing regexp: invalid nested repetition operator: `**`"#),
        ),
        (
            r#"a++"#,
            Some(r#"error parsing regexp: invalid nested repetition operator: `++`"#),
        ),
        (
            r#"a*+"#,
            Some(r#"error parsing regexp: invalid nested repetition operator: `*+`"#),
        ),
        (
            r#"a???"#,
            Some(r#"error parsing regexp: invalid nested repetition operator: `???`"#),
        ),
        (
            r#"*"#,
            Some(r#"error parsing regexp: missing argument to repetition operator: `*`"#),
        ),
        (
            r#"+a"#,
            Some(r#"error parsing regexp: missing argument to repetition operator: `+`"#),
        ),
        (
            r#"?"#,
            Some(r#"error parsing regexp: missing argument to repetition operator: `?`"#),
        ),
        (
            r#"(*)"#,
            Some(r#"error parsing regexp: missing argument to repetition operator: `*`"#),
        ),
        (
            r#"|*"#,
            Some(r#"error parsing regexp: missing argument to repetition operator: `*`"#),
        ),
        (r#"^*"#, None),
        (
            r#"a{2}{3}"#,
            Some(r#"error parsing regexp: invalid nested repetition operator: `{2}{3}`"#),
        ),
        (
            r#"a{1001}"#,
            Some(r#"error parsing regexp: invalid repeat count: `{1001}`"#),
        ),
        (
            r#"a{1,1001}"#,
            Some(r#"error parsing regexp: invalid repeat count: `{1,1001}`"#),
        ),
        (
            r#"a{2,1}"#,
            Some(r#"error parsing regexp: invalid repeat count: `{2,1}`"#),
        ),
        (
            r#"a{99999999999}"#,
            Some(r#"error parsing regexp: invalid repeat count: `{99999999999}`"#),
        ),
        (
            r#"(a{600}){2}"#,
            Some(r#"error parsing regexp: invalid repeat count: `{2}`"#),
        ),
        (
            r#"(a{20}){20}{3}"#,
            Some(r#"error parsing regexp: invalid nested repetition operator: `{20}{3}`"#),
        ),
        (r#"a{1000}"#, None),
        (r#"a{,5}"#, None),
        (r#"a{"#, None),
        (r#"a{x}"#, None),
        (r#"{"#, None),
        (r#"a{1"#, None),
        (r#"a{1,"#, None),
        (r#"a{01}"#, None),
        (
            r#"[a"#,
            Some(r#"error parsing regexp: missing closing ]: `[a`"#),
        ),
        (
            r#"[a-"#,
            Some(r#"error parsing regexp: missing closing ]: `[a-`"#),
        ),
        (
            r#"[]"#,
            Some(r#"error parsing regexp: missing closing ]: `[]`"#),
        ),
        (r#"[]a]"#, None),
        (
            r#"[^]"#,
            Some(r#"error parsing regexp: missing closing ]: `[^]`"#),
        ),
        (
            r#"[z-a]"#,
            Some(r#"error parsing regexp: invalid character class range: `z-a`"#),
        ),
        (
            r#"[a-\d]"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\d`"#),
        ),
        (
            r#"[[:foo:]]"#,
            Some(r#"error parsing regexp: invalid character class range: `[:foo:]`"#),
        ),
        (
            r#"[[:alpha:]"#,
            Some(r#"error parsing regexp: missing closing ]: `[[:alpha:]`"#),
        ),
        (r#"[[:alpha:]]"#, None),
        (r#"[[:^alpha:]]"#, None),
        (
            r#"[[:alpha:"#,
            Some(r#"error parsing regexp: missing closing ]: `[[:alpha:`"#),
        ),
        (
            r#"[\q]"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\q`"#),
        ),
        (
            r#"[\"#,
            Some(r#"error parsing regexp: trailing backslash at end of expression: ``"#),
        ),
        (
            r#"\"#,
            Some(r#"error parsing regexp: trailing backslash at end of expression: ``"#),
        ),
        (
            r#"\q"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\q`"#),
        ),
        (
            r#"\8"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\8`"#),
        ),
        (
            r#"\1"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\1`"#),
        ),
        (r#"\12"#, None),
        (r#"\0"#, None),
        (r#"\01"#, None),
        (
            r#"\x"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\x`"#),
        ),
        (
            r#"\x4"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\x4`"#),
        ),
        (
            r#"\x4g"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\x4g`"#),
        ),
        (r#"\x41"#, None),
        (r#"\x{41}"#, None),
        (
            r#"\x{}"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\x{}`"#),
        ),
        (
            r#"\x{110000}"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\x{110000`"#),
        ),
        (
            r#"\x{41"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\x{41`"#),
        ),
        (
            r#"\x{zz}"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\x{z`"#),
        ),
        (
            r#"\C"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\C`"#),
        ),
        (
            r#"\Z"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\Z`"#),
        ),
        (r#"\z"#, None),
        (r#"\A"#, None),
        (r#"\b"#, None),
        (r#"\B"#, None),
        (r#"\Qab(\E+"#, None),
        (r#"\Qab("#, None),
        (r#"\Q"#, None),
        (
            r#"\p"#,
            Some(r#"error parsing regexp: invalid character class range: `\p`"#),
        ),
        (r#"\pL"#, None),
        (
            r#"\pX"#,
            Some(r#"error parsing regexp: invalid character class range: `\pX`"#),
        ),
        (r#"\p{L}"#, None),
        (r#"\p{^L}"#, None),
        (r#"\P{Greek}"#, None),
        (
            r#"\p{Foo}"#,
            Some(r#"error parsing regexp: invalid character class range: `\p{Foo}`"#),
        ),
        (
            r#"\p{Old_Italic}"#,
            Some(r#"error parsing regexp: invalid character class range: `\p{Old_Italic}`"#),
        ),
        (
            r#"\p{OldItalic}"#,
            Some(r#"error parsing regexp: invalid character class range: `\p{OldItalic}`"#),
        ),
        (r#"\p{letter}"#, None),
        (r#"\p{Letter}"#, None),
        (r#"\p{ascii}"#, None),
        (r#"\p{Any}"#, None),
        (r#"\p{Assigned}"#, None),
        (r#"\p{Lc}"#, None),
        (r#"\p{LC}"#, None),
        (
            r#"\p{L"#,
            Some(r#"error parsing regexp: invalid character class range: `\p{L`"#),
        ),
        (
            r#"\p{}"#,
            Some(r#"error parsing regexp: invalid character class range: `\p{}`"#),
        ),
        (
            r#"[\p{Foo}]"#,
            Some(r#"error parsing regexp: invalid character class range: `\p{Foo}`"#),
        ),
        (r#"[\p{L}\d]"#, None),
        (
            r#"(?"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?`"#),
        ),
        (
            r#"(?i"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?i`"#),
        ),
        (r#"(?i)"#, None),
        (r#"(?i:abc)"#, None),
        (r#"(?-i)"#, None),
        (
            r#"(?-)"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?-)`"#),
        ),
        (
            r#"(?--i)"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?--`"#),
        ),
        (
            r#"(?z)"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?z`"#),
        ),
        (r#"(?P<name>x)"#, None),
        (
            r#"(?P<name"#,
            Some(r#"error parsing regexp: invalid named capture: `(?P<name`"#),
        ),
        (
            r#"(?P<>x)"#,
            Some(r#"error parsing regexp: invalid named capture: `(?P<>`"#),
        ),
        (
            r#"(?P<a-b>x)"#,
            Some(r#"error parsing regexp: invalid named capture: `(?P<a-b>`"#),
        ),
        (r#"(?<name>x)"#, None),
        (
            r#"(?<>x)"#,
            Some(r#"error parsing regexp: invalid named capture: `(?<>`"#),
        ),
        (
            r#"(?'name'x)"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?'`"#),
        ),
        (
            r#"(?=x)"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?=`"#),
        ),
        (
            r#"(?!x)"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?!`"#),
        ),
        (
            r#"(?<=x)"#,
            Some(r#"error parsing regexp: invalid named capture: `(?<=x)`"#),
        ),
        (
            r#"(?#c)"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?#`"#),
        ),
        (
            r#"(?:"#,
            Some(r#"error parsing regexp: missing closing ): `(?:`"#),
        ),
        (
            r#"(?:a"#,
            Some(r#"error parsing regexp: missing closing ): `(?:a`"#),
        ),
        (r#"(?:a)"#, None),
        (r#"(?i-s:a)"#, None),
        (r#"(?is-m:a)"#, None),
        (r#"(?U)a*"#, None),
        (r#"a|"#, None),
        (r#"|a"#, None),
        (r#"||"#, None),
        (r#"(|)"#, None),
        (
            r#"(a|b"#,
            Some(r#"error parsing regexp: missing closing ): `(a|b`"#),
        ),
        (
            r#"a|b)"#,
            Some(r#"error parsing regexp: unexpected ): `a|b)`"#),
        ),
        (
            r#"((a)"#,
            Some(r#"error parsing regexp: missing closing ): `((a)`"#),
        ),
        (
            r#"(a))"#,
            Some(r#"error parsing regexp: unexpected ): `(a))`"#),
        ),
        (r#"^abc$"#, None),
        (r#"é"#, None),
        (
            r#"[é-a]"#,
            Some(r#"error parsing regexp: invalid character class range: `é-a`"#),
        ),
        (
            r#"\é"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\é`"#),
        ),
        (r#"."#, None),
        (r#"a.*b"#, None),
        (r#"[a-z]+@[a-z]+"#, None),
        (r#"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"#, None),
        (r#"(?i)\p{Greek}+"#, None),
        (
            r#"x{2}{3}"#,
            Some(r#"error parsing regexp: invalid nested repetition operator: `{2}{3}`"#),
        ),
        (r#"(x{2}){3}"#, None),
        (
            r#"(((((((((((a{2}){2}){2}){2}){2}){2}){2}){2}){2}){2}){2})"#,
            Some(r#"error parsing regexp: invalid repeat count: `{2}`"#),
        ),
        (r#"a{0}"#, None),
        (r#"a{0,0}"#, None),
        (
            r#"(a{0}){100000}"#,
            Some(r#"error parsing regexp: invalid repeat count: `{100000}`"#),
        ),
        (r#"[a-b-c]"#, None),
        (r#"[a-]"#, None),
        (r#"[-a]"#, None),
        (r#"[a\-z]"#, None),
        (r#"[\d-z]"#, None),
        (r#"(?P<n>a)(?P<n>b)"#, None),
        (r#"\_"#, None),
        (r#"\-"#, None),
        (r#"\ "#, None),
        (r#"\Qabc\Edef"#, None),
    ];

    #[test]
    fn compile_matches_go_regexp_compile() {
        for (pattern, want) in GO_OUTCOMES {
            let got = compile(pattern).err().map(|e| e.to_string());
            assert_eq!(got.as_deref(), *want, "pattern {pattern:?}");
        }
    }

    /// `checkSize`/`checkHeight` (parse.go) outcomes recorded under Go 1.25.9.
    #[test]
    fn size_and_height_limits_match_go() {
        let nest = |n: usize| format!("{}a{}", "(".repeat(n), ")".repeat(n));
        let cases: Vec<(String, Option<&str>)> = vec![
            (nest(1100), Some(ERR_NESTING_DEPTH)),
            (nest(999), None),
            (nest(400), None),
            (format!("{}a{}", "(?:".repeat(1100), ")".repeat(1100)), None),
            ("a{1000}".repeat(3300), None),
            ("a{1000}".repeat(3400), Some(ERR_LARGE)),
            ("(a{1000}|b{1000})".repeat(1700), Some(ERR_LARGE)),
            ("(a{1000}|b{1000})".repeat(1600), None),
            ("[a-z]{1000}".repeat(3400), Some(ERR_LARGE)),
            ("x*".repeat(1_700_000), Some(ERR_LARGE)),
            ("x*".repeat(1_200_000), None),
            ("(((a*)*)*)*".repeat(1000), None),
            (format!("{}b", "a|".repeat(5000)), None),
        ];
        for (pattern, want) in cases {
            let got = compile(&pattern).err().map(|e| e.code);
            assert_eq!(got, want, "pattern of {} bytes", pattern.len());
        }
    }
}
