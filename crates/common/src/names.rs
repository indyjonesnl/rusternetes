//! Port of `staging/src/k8s.io/apiserver/pkg/storage/names/generate.go`.

/// `maxNameLength` (generate.go:44).
pub const MAX_NAME_LENGTH: usize = 63;
/// `randomLength` (generate.go:45).
pub const RANDOM_LENGTH: usize = 5;
/// `MaxGeneratedNameLength` (generate.go:46): the longest `generateName` base
/// that still leaves room for the random suffix.
pub const MAX_GENERATED_NAME_LENGTH: usize = MAX_NAME_LENGTH - RANDOM_LENGTH;

/// The alphabet `utilrand.String` draws from
/// (`staging/src/k8s.io/apimachinery/pkg/util/rand/rand.go:83`): no vowels, so
/// a generated suffix can never spell a word, and no `0`/`1`/`3` that read as
/// letters.
pub const ALPHANUMS: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";

/// `simpleNameGenerator.GenerateName` (generate.go:49-54): truncate the base to
/// [`MAX_GENERATED_NAME_LENGTH`] and append five random characters.
///
/// The base is cut on a byte count exactly as upstream's `base[:58]` is. A
/// `generateName` is validated as a DNS subdomain/label before any name is
/// generated from it, so it is ASCII and the cut never lands inside a
/// character.
pub fn simple_name_generator(base: &str) -> String {
    let base = if base.len() > MAX_GENERATED_NAME_LENGTH {
        &base[..MAX_GENERATED_NAME_LENGTH]
    } else {
        base
    };
    format!("{base}{}", random_string(RANDOM_LENGTH))
}

/// `utilrand.String(n)`: `n` characters drawn uniformly from [`ALPHANUMS`].
///
/// Bytes come from `uuid::Uuid::new_v4` (the OS RNG already used for UIDs).
/// A byte is kept only when it falls below the largest multiple of the
/// alphabet size, so every character is equally likely — upstream gets the
/// same property from masking bits off `rand.Int63` and rejecting overflow.
fn random_string(n: usize) -> String {
    let limit = (256 / ALPHANUMS.len() * ALPHANUMS.len()) as u8;
    let mut out = String::with_capacity(n);
    while out.len() < n {
        for b in uuid::Uuid::new_v4().into_bytes() {
            if b < limit {
                out.push(ALPHANUMS[b as usize % ALPHANUMS.len()] as char);
                if out.len() == n {
                    break;
                }
            }
        }
    }
    out
}
