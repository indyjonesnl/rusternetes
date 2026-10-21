//! Port of the node log query engine behind the kubelet `/logs/` endpoint:
//! `pkg/kubelet/kubelet_server_journal.go` (query parsing, validation, the
//! file heuristics and the command runner) and
//! `pkg/kubelet/kubelet_server_journal_linux.go` (`getLoggingCmd`,
//! `checkForNativeLogger`).
//!
//! This module is the transport-independent half: it turns the query
//! parameters into a validated [`NodeLogQuery`] and streams the result into a
//! writer. The HTTP arm that upstream keeps in `Kubelet.Run`
//! (`pkg/kubelet/kubelet.go:1778-1810`, gated on the `NodeLogQuery` feature
//! gate and `enableSystemLogQuery`) and the `journalServer.ServeHTTP` gzip
//! wrapper (`kubelet_server_journal.go:62-89`) belong to the `/logs/` route.
//!
//! Deliberate deviations from upstream, none observable through the API:
//! - Windows (`kubelet_server_journal_windows.go`, `Get-WinEvent`) is not
//!   ported; [`get_logging_cmd`] errors off Linux exactly like
//!   `kubelet_server_journal_others.go`.
//! - `os.OpenRoot` / `os.OpenInRoot` (openat2-style, race free) is replaced by
//!   [`open_in_root`], which rejects absolute names, lexical `..` escapes and
//!   any resolved path outside the canonical root.
//! - `syntax.Parse(pattern, syntax.Perl)` is `regex::Regex::new`; both are RE2
//!   grammars.

use chrono::{DateTime, FixedOffset, SecondsFormat};
use regex::Regex;
use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Component, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{mpsc, LazyLock};
use std::time::{Duration, Instant};

/// `dateLayout` (`kubelet_server_journal.go:44`): Go `"2006-1-2 15:4:5"`.
const DATE_LAYOUT: &str = "%Y-%-m-%-d %H:%-M:%-S";
/// `maxTailLines` (`:45`).
pub const MAX_TAIL_LINES: i64 = 100_000;
/// `maxServiceLength` (`:46`).
pub const MAX_SERVICE_LENGTH: usize = 256;
/// `maxServices` (`:47`).
pub const MAX_SERVICES: usize = 4;
/// `nodeLogDir` (`:48`).
pub const NODE_LOG_DIR: &str = "/var/log/";
/// `context.WithDeadline(..., 30*time.Second)` in `Copy` (`:266`).
const COPY_DEADLINE: Duration = Duration::from_secs(30);

/// `reServiceNameUnsafeCharacters` (`:54`): the set of known safe characters
/// to pass to journalctl; anything else is rejected.
static RE_SERVICE_NAME_UNSAFE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[^a-zA-Z\-_.:0-9@]+").expect("static regex"));

/// `options` (`:93-107`). `Default` is Go's zero value, which `validate` and
/// `copyForBoot` compare against (`n.options != (options{})`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Options {
    /// RFC3339 timestamp from which to show logs.
    pub since_time: Option<DateTime<FixedOffset>>,
    /// RFC3339 timestamp until which to show logs.
    pub until_time: Option<DateTime<FixedOffset>>,
    /// Number of lines (at most 100k) from the end of the log.
    pub tail_lines: Option<i64>,
    /// Boot offset; allowed values are less than 1.
    pub boot: Option<i64>,
    /// Regex filter, read as PCRE2 by journalctl.
    pub pattern: String,
}

/// `nodeLogQuery` (`:84-91`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NodeLogQuery {
    /// Services to be queried.
    pub services: Vec<String>,
    /// Files to be queried.
    pub files: Vec<String>,
    pub options: Options,
}

/// `ErrorList.ToAggregate().Error()` (`apimachinery/pkg/util/errors`): the
/// messages are de-duplicated; one message renders bare, several render as
/// `[a, b]`. Empty list renders as an empty string.
pub fn aggregate_message(errs: &[FieldError]) -> String {
    let mut seen = Vec::<String>::new();
    for e in errs {
        let m = e.to_string();
        if !seen.contains(&m) {
            seen.push(m);
        }
    }
    match seen.len() {
        0 => String::new(),
        1 => seen.remove(0),
        _ => format!("[{}]", seen.join(", ")),
    }
}

/// Go `strconv.Atoi` for a 64-bit `int`, including its error text.
fn atoi(s: &str) -> Result<i64, String> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("strconv.Atoi: parsing {s:?}: invalid syntax"));
    }
    s.parse::<i64>()
        .map_err(|_| format!("strconv.Atoi: parsing {s:?}: value out of range"))
}

fn first<'a>(query: &'a [(String, String)], key: &str) -> &'a str {
    query
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .unwrap_or("")
}

fn time_value(t: &DateTime<FixedOffset>) -> String {
    t.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

/// `newNodeLogQuery` (`:109-196`): parse query values into a
/// [`NodeLogQuery`]. `Ok(None)` is Go's `nil, empty` (no recognised
/// parameter present).
pub fn new_node_log_query(query: &[(String, String)]) -> Result<Option<NodeLogQuery>, ErrorList> {
    let mut errs = ErrorList::new();
    let mut nlq = NodeLogQuery::default();

    let queries: Vec<&String> = query
        .iter()
        .filter(|(k, _)| k == "query")
        .map(|(_, v)| v)
        .collect();
    let ok = !queries.is_empty();
    for q in &queries {
        // The presence of / or \ is a hint that the query is for a log file.
        if q.contains(['/', '\\']) {
            nlq.files.push((*q).clone());
        } else if !q.trim().is_empty() {
            // Prevent queries with just spaces.
            nlq.services.push((*q).clone());
        }
    }

    // Prevent specifying an empty or blank space query.
    if ok && nlq.files.is_empty() && nlq.services.is_empty() {
        errs.push(FieldError::invalid(
            &Path::new("query"),
            queries.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            "may not be empty",
        ));
    }

    for (name, slot) in [
        ("sinceTime", &mut nlq.options.since_time),
        ("untilTime", &mut nlq.options.until_time),
    ] {
        let v = first(query, name);
        if !v.is_empty() {
            match DateTime::parse_from_rfc3339(v) {
                Ok(t) => *slot = Some(t),
                Err(_) => errs.push(FieldError::invalid(
                    &Path::new(name),
                    v,
                    "invalid time format",
                )),
            }
        }
    }

    for (name, slot) in [
        ("boot", &mut nlq.options.boot),
        ("tailLines", &mut nlq.options.tail_lines),
    ] {
        let v = first(query, name);
        if !v.is_empty() {
            match atoi(v) {
                Ok(n) => *slot = Some(n),
                Err(e) => errs.push(FieldError::invalid(&Path::new(name), v, e)),
            }
        }
    }

    let pattern = first(query, "pattern");
    if !pattern.is_empty() {
        nlq.options.pattern = pattern.to_string();
    }

    if !errs.is_empty() {
        return Err(errs);
    }
    if nlq == NodeLogQuery::default() {
        return Ok(None);
    }
    Ok(Some(nlq))
}

/// `safeServiceName` (`:432-442`). The first message is upstream's, typo
/// included ("less than 100" while the limit is 256).
fn safe_service_name(s: &str) -> Result<(), String> {
    if s.len() > MAX_SERVICE_LENGTH {
        return Err("length must be less than 100".to_string());
    }
    if RE_SERVICE_NAME_UNSAFE.is_match(s) {
        return Err("input contains unsupported characters".to_string());
    }
    Ok(())
}

/// `validateServices` (`:198-211`).
pub fn validate_services(services: &[String]) -> ErrorList {
    let mut errs = ErrorList::new();
    for s in services {
        if let Err(e) = safe_service_name(s) {
            errs.push(FieldError::invalid(&Path::new("query"), s.as_str(), e));
        }
    }
    if services.len() > MAX_SERVICES {
        errs.push(FieldError::too_many(
            &Path::new("query"),
            services.len(),
            MAX_SERVICES,
        ));
    }
    errs
}

fn escapes_error() -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, "path escapes from parent")
}

/// Stand-in for `os.OpenInRoot(dir, name)` / `Root.Stat(name)`: resolves
/// `name` beneath `dir`, failing for absolute names (`splitPathInRoot`,
/// `os/root.go:194-196`), `..` that climbs above the root, and symlinks that
/// resolve outside it.
fn resolve_in_root(dir: &str, name: &str) -> io::Result<PathBuf> {
    if name.is_empty() {
        return Err(io::Error::other("empty path"));
    }
    if name.starts_with('/') {
        return Err(escapes_error());
    }
    let mut depth = 0usize;
    for c in std::path::Path::new(name).components() {
        match c {
            Component::Normal(_) => depth += 1,
            Component::ParentDir => {
                if depth == 0 {
                    return Err(escapes_error());
                }
                depth -= 1;
            }
            Component::CurDir => {}
            _ => return Err(escapes_error()),
        }
    }
    let root = std::fs::canonicalize(dir)?;
    let full = std::fs::canonicalize(root.join(name))?;
    if !full.starts_with(&root) {
        return Err(escapes_error());
    }
    Ok(full)
}

impl NodeLogQuery {
    /// `validate` (`:213-264`), with `nodeLogDir` as the file root.
    pub fn validate(&self) -> ErrorList {
        self.validate_in(NODE_LOG_DIR)
    }

    /// `validate` against an explicit log directory (`nodeLogDir` is a
    /// constant upstream; tests need a temp dir).
    pub fn validate_in(&self, log_dir: &str) -> ErrorList {
        let mut errs = validate_services(&self.services);
        let query = Path::new("query");
        let files_val = || self.files.clone();
        if self.files.is_empty() && self.services.is_empty() {
            errs.push(FieldError::required(&query, "cannot be empty with options"));
        } else if !self.files.is_empty() && !self.services.is_empty() {
            errs.push(FieldError::invalid(
                &query,
                format!("[{}], [{}]", self.files.join(" "), self.services.join(" ")),
                "cannot specify a file and service",
            ));
        } else if self.files.len() > 1 {
            errs.push(FieldError::invalid(
                &query,
                files_val(),
                "cannot specify more than one file",
            ));
        } else if self.files.len() == 1 && self.options != Options::default() {
            errs.push(FieldError::invalid(
                &query,
                files_val(),
                "cannot specify file with options",
            ));
        } else if self.files.len() == 1 {
            if let Err(e) = resolve_in_root(log_dir, &self.files[0]) {
                errs.push(FieldError::invalid(&query, files_val(), e.to_string()));
            }
        }

        let o = &self.options;
        if let (Some(s), Some(u)) = (&o.since_time, &o.until_time) {
            if s > u {
                errs.push(FieldError::invalid(
                    &Path::new("untilTime"),
                    time_value(u),
                    "must be after `sinceTime`",
                ));
            }
        }
        if let Some(b) = o.boot {
            if cfg!(windows) {
                errs.push(FieldError::invalid(
                    &Path::new("boot"),
                    b,
                    "not supported on Windows",
                ));
            }
            if b > 0 {
                errs.push(FieldError::invalid(
                    &Path::new("boot"),
                    b,
                    "must be less than 1",
                ));
            }
        }
        if let Some(t) = o.tail_lines {
            // utilvalidation.IsInRange(v, 0, maxTailLines) -> InclusiveRangeError.
            if !(0..=MAX_TAIL_LINES).contains(&t) {
                errs.push(FieldError::invalid(
                    &Path::new("tailLines"),
                    t,
                    format!("must be between 0 and {MAX_TAIL_LINES}, inclusive"),
                ));
            }
        }
        if let Err(e) = Regex::new(&o.pattern) {
            errs.push(FieldError::invalid(
                &Path::new("pattern"),
                o.pattern.as_str(),
                e.to_string(),
            ));
        }
        errs
    }

    /// `Copy` (`:266-276`): stream the logging command's output into `w`
    /// within a 30s deadline. Errors are written as a line to `w`.
    pub fn copy(&self, w: &mut dyn Write) {
        let deadline = Instant::now() + COPY_DEADLINE;
        self.copy_for_boot(deadline, w, self.options.boot.unwrap_or(0));
    }

    /// `copyForBoot` (`:278-298`).
    fn copy_for_boot(&self, deadline: Instant, w: &mut dyn Write, previous_boot: i64) {
        if Instant::now() >= deadline {
            return;
        }
        let (native, files) = self.split_native_vs_file_loggers(deadline);
        if !native.is_empty() {
            self.copy_service_logs(deadline, w, &native, previous_boot);
        }
        if !files.is_empty() && self.options != Options::default() {
            let _ = write!(
                w,
                "\noptions present and query resolved to log files for {files:?}\ntry without specifying options\n"
            );
            return;
        }
        if !files.is_empty() {
            copy_file_logs(deadline, w, &files);
        }
    }

    /// `splitNativeVsFileLoggers` (`:300-318`).
    fn split_native_vs_file_loggers(&self, deadline: Instant) -> (Vec<String>, Vec<String>) {
        let (mut native, mut files) = (Vec::new(), Vec::new());
        for s in &self.services {
            if check_for_native_logger(deadline, s) {
                native.push(s.clone());
            } else {
                files.push(s.clone());
            }
        }
        (native, files)
    }

    /// `copyServiceLogs` (`:320-343`).
    fn copy_service_logs(
        &self,
        deadline: Instant,
        w: &mut dyn Write,
        services: &[String],
        previous_boot: i64,
    ) {
        let (cmd, args) = match get_logging_cmd(self, services) {
            Ok(c) => c,
            Err(e) => {
                let _ = write!(w, "\nfailed to get logging cmd: {e}\n");
                return;
            }
        };
        match run_command(&cmd, &args, deadline, w) {
            // `*exec.ExitError`: the command ran; its stderr is already in w.
            Ok(_) => {}
            Err(_) if previous_boot == 0 => {
                let _ = write!(w, "\nerror: journal output not available\n");
            }
            Err(_) => {}
        }
    }
}

/// Runs `program` with stdout and stderr both streamed into `w`
/// (`cmd.Stdout = w; cmd.Stderr = w`), killing it at `deadline`
/// (`exec.CommandContext`). `Err` means the command could not be started or
/// read; a non-zero exit or a deadline kill is `Ok`.
fn run_command(
    program: &str,
    args: &[String],
    deadline: Instant,
    w: &mut dyn Write,
) -> io::Result<std::process::ExitStatus> {
    let (mut reader, writer) = io::pipe()?;
    let mut cmd = Command::new(program);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(writer.try_clone()?)
        .stderr(writer);
    let mut child = cmd.spawn()?;
    // `cmd` still owns the parent's copies of the pipe's write end; drop them
    // or the read below never sees EOF.
    drop(cmd);

    let pid = child.id();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let remaining = deadline.saturating_duration_since(Instant::now());
    let watchdog = std::thread::spawn(move || {
        if done_rx.recv_timeout(remaining) == Err(mpsc::RecvTimeoutError::Timeout) {
            // SAFETY: plain kill(2) on the child we spawned; the pid cannot be
            // recycled before `child.wait()` below, which runs only after the
            // read hits EOF or this thread is told we are done.
            #[cfg(unix)]
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGKILL);
            }
            #[cfg(not(unix))]
            let _ = pid;
        }
    });

    let mut buf = [0u8; 8192];
    let copy_result = loop {
        match reader.read(&mut buf) {
            Ok(0) => break Ok(()),
            Ok(n) => {
                if let Err(e) = w.write_all(&buf[..n]) {
                    break Err(e);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => break Err(e),
        }
    };
    if copy_result.is_err() {
        let _ = child.kill();
    }
    let status = child.wait();
    let _ = done_tx.send(());
    let _ = watchdog.join();
    copy_result?;
    status
}

/// `copyFileLogs` (`:345-356`).
fn copy_file_logs(deadline: Instant, w: &mut dyn Write, services: &[String]) {
    if Instant::now() >= deadline {
        let _ = write!(w, "\ncontext error: context deadline exceeded\n");
        return;
    }
    for s in services {
        heuristics_copy_file_logs(deadline, w, NODE_LOG_DIR, s);
    }
}

/// `heuristicsCopyFileLogs` (`:358-389`): try `/var/log/service`,
/// `/var/log/service.log` then `/var/log/service/service.log`, stopping on the
/// first success.
pub fn heuristics_copy_file_logs(
    deadline: Instant,
    w: &mut dyn Write,
    log_dir: &str,
    service: &str,
) {
    let names = [
        service.to_string(),
        format!("{service}.log"),
        format!("{service}/{service}.log"),
    ];
    let mut err = None;
    for name in &names {
        match heuristics_copy_file_log(deadline, w, log_dir, name) {
            Ok(()) => {
                err = None;
                break;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => err = Some(e),
            Err(e) => {
                err = Some(e);
                break;
            }
        }
    }
    if let Some(e) = err {
        // If the last error was file not found no log file exists for the service.
        if e.kind() == io::ErrorKind::NotFound {
            let _ = write!(w, "\nlog not found for {service}\n");
            return;
        }
        let _ = write!(w, "\nerror getting log for {service}: {e}\n");
    }
}

/// `readerCtx` (`:391-408`): a reader that fails once the deadline passes.
struct DeadlineReader<R> {
    deadline: Instant,
    inner: R,
}

impl<R: Read> Read for DeadlineReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if Instant::now() >= self.deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "context deadline exceeded",
            ));
        }
        self.inner.read(buf)
    }
}

/// `heuristicsCopyFileLog` (`:410-430`): a directory is "not exist" so that
/// logs for service foo may live in `/var/log/foo/`.
fn heuristics_copy_file_log(
    deadline: Instant,
    w: &mut dyn Write,
    log_dir: &str,
    log_file_name: &str,
) -> io::Result<()> {
    let path = resolve_in_root(log_dir, log_file_name)?;
    let f = File::open(path)?;
    if f.metadata()?.is_dir() {
        return Err(io::Error::from(io::ErrorKind::NotFound));
    }
    io::copy(&mut DeadlineReader { deadline, inner: f }, w)?;
    Ok(())
}

/// `getLoggingCmd` (`kubelet_server_journal_linux.go:29-62`): the journalctl
/// command and arguments for the query. `services` is passed explicitly to
/// account for the file-vs-journal heuristics. Upstream's `cmdEnv` is always
/// empty on Linux and is omitted.
#[cfg(target_os = "linux")]
pub fn get_logging_cmd(n: &NodeLogQuery, services: &[String]) -> io::Result<(String, Vec<String>)> {
    let mut args = vec![
        "--utc".to_string(),
        "--no-pager".to_string(),
        "--output=short-precise".to_string(),
    ];
    let o = &n.options;
    if let Some(t) = &o.since_time {
        args.push(format!("--since={}", t.format(DATE_LAYOUT)));
    }
    if let Some(t) = &o.until_time {
        args.push(format!("--until={}", t.format(DATE_LAYOUT)));
    }
    if let Some(l) = o.tail_lines {
        args.push("--pager-end".to_string());
        args.push(format!("--lines={l}"));
    }
    for s in services {
        if !s.is_empty() {
            args.push(format!("--unit={s}"));
        }
    }
    if !o.pattern.is_empty() {
        args.push(format!("--grep={}", o.pattern));
    }
    if let Some(b) = o.boot {
        args.push("--boot".to_string());
        args.push(b.to_string());
    }
    Ok(("journalctl".to_string(), args))
}

/// `getLoggingCmd` off Linux (`kubelet_server_journal_others.go`): unsupported.
#[cfg(not(target_os = "linux"))]
pub fn get_logging_cmd(
    _n: &NodeLogQuery,
    _services: &[String],
) -> io::Result<(String, Vec<String>)> {
    Err(io::Error::other("platform not supported"))
}

/// `checkForNativeLogger` (`kubelet_server_journal_linux.go:65-77`):
/// journalctl does not fail for an unknown unit, so look the service up in
/// the list of journald units. Any failure means "maybe a file logger".
fn check_for_native_logger(deadline: Instant, service: &str) -> bool {
    let mut out = Vec::new();
    let args = ["--field".to_string(), "_SYSTEMD_UNIT".to_string()];
    match run_command("journalctl", &args, deadline, &mut out) {
        Ok(status) if status.success() => {
            String::from_utf8_lossy(&out).contains(&format!("{service}.service"))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn far() -> Instant {
        Instant::now() + Duration::from_secs(30)
    }

    /// `Test_getLoggingCmd` (linux arm).
    #[test]
    #[cfg(target_os = "linux")]
    fn get_logging_cmd_matches_upstream() {
        let base = ["--utc", "--no-pager", "--output=short-precise"];
        let cases: Vec<(&str, Vec<&str>, Vec<&str>)> = vec![
            ("basic", vec![], vec![]),
            (
                "two providers",
                vec!["p1", "p2"],
                vec!["--unit=p1", "--unit=p2"],
            ),
            (
                "empty provider",
                vec!["p1", "", "p2"],
                vec!["--unit=p1", "--unit=p2"],
            ),
        ];
        for (name, services, extra) in cases {
            let (cmd, got) = get_logging_cmd(&NodeLogQuery::default(), &s(&services)).expect(name);
            let mut want: Vec<String> = base.iter().map(|x| x.to_string()).collect();
            want.extend(extra.iter().map(|x| x.to_string()));
            assert_eq!(cmd, "journalctl");
            assert_eq!(got, want, "{name}");
        }
    }

    /// Every option, in upstream's argument order; `dateLayout` is not
    /// zero-padded for month/day/minute/second.
    #[test]
    #[cfg(target_os = "linux")]
    fn get_logging_cmd_all_options() {
        let n = NodeLogQuery {
            services: s(&["kubelet"]),
            files: vec![],
            options: Options {
                since_time: Some(DateTime::parse_from_rfc3339("2019-01-04T02:03:04Z").unwrap()),
                until_time: Some(DateTime::parse_from_rfc3339("2019-12-14T12:13:14Z").unwrap()),
                tail_lines: Some(50),
                boot: Some(-1),
                pattern: "err.*".into(),
            },
        };
        let (_, got) = get_logging_cmd(&n, &n.services).unwrap();
        assert_eq!(
            got,
            s(&[
                "--utc",
                "--no-pager",
                "--output=short-precise",
                "--since=2019-1-4 02:3:4",
                "--until=2019-12-14 12:13:14",
                "--pager-end",
                "--lines=50",
                "--unit=kubelet",
                "--grep=err.*",
                "--boot",
                "-1",
            ])
        );
    }

    /// `Test_newNodeLogQuery`.
    #[test]
    fn new_node_log_query_matches_upstream() {
        let valid = "2019-12-04T02:00:00Z";
        let t = DateTime::parse_from_rfc3339(valid).unwrap();
        let opts = |f: &dyn Fn(&mut Options)| {
            let mut o = Options::default();
            f(&mut o);
            Some(NodeLogQuery {
                options: o,
                ..Default::default()
            })
        };
        type Case = (
            Vec<(&'static str, &'static str)>,
            Option<Option<NodeLogQuery>>,
        );
        let cases: Vec<Case> = vec![
            (vec![], Some(None)),
            (vec![("unknown", "true")], Some(None)),
            (vec![("sinceTime", "")], Some(None)),
            (vec![("sinceTime", "2019-12-04 02:00:00")], None),
            (vec![("sinceTime", "2019-12-04 02:00:00.000")], None),
            (vec![("sinceTime", "2019-12-04 02")], None),
            (vec![("sinceTime", "2019-12-04 02:00")], None),
            (
                vec![("sinceTime", valid)],
                Some(opts(&|o| o.since_time = Some(t))),
            ),
            (vec![("untilTime", "")], Some(None)),
            (vec![("untilTime", "2019-12-04 02:00:00")], None),
            (vec![("untilTime", "2019-12-04 02:00:00.000")], None),
            (vec![("untilTime", "2019-12-04 02")], None),
            (vec![("untilTime", "2019-12-04 02:00")], None),
            (
                vec![("untilTime", valid)],
                Some(opts(&|o| o.until_time = Some(t))),
            ),
            (
                vec![("tailLines", "100")],
                Some(opts(&|o| o.tail_lines = Some(100))),
            ),
            (vec![("tailLines", "foo")], None),
            (vec![("tailLines", " ")], None),
            (
                vec![("pattern", "foo")],
                Some(opts(&|o| o.pattern = "foo".into())),
            ),
            (vec![("boot", "")], Some(None)),
            (vec![("boot", "0")], Some(opts(&|o| o.boot = Some(0)))),
            (vec![("boot", "-23")], Some(opts(&|o| o.boot = Some(-23)))),
            (vec![("boot", "foo")], None),
            (vec![("boot", " ")], None),
            (vec![("query", "")], None),
            (vec![("query", "   "), ("query", "    ")], None),
            (
                vec![("query", "foo")],
                Some(Some(NodeLogQuery {
                    services: s(&["foo"]),
                    ..Default::default()
                })),
            ),
            (
                vec![("query", "foo"), ("query", "bar")],
                Some(Some(NodeLogQuery {
                    services: s(&["foo", "bar"]),
                    ..Default::default()
                })),
            ),
            (
                vec![("query", "foo"), ("query", "/bar")],
                Some(Some(NodeLogQuery {
                    services: s(&["foo"]),
                    files: s(&["/bar"]),
                    ..Default::default()
                })),
            ),
            (
                vec![("query", "/foo"), ("query", "\\bar")],
                Some(Some(NodeLogQuery {
                    files: s(&["/foo", "\\bar"]),
                    ..Default::default()
                })),
            ),
        ];
        for (query, want) in cases {
            let got = new_node_log_query(&q(&query));
            match want {
                None => assert!(got.is_err(), "{query:?}: want error, got {got:?}"),
                Some(w) => assert_eq!(got.unwrap(), w, "{query:?}"),
            }
        }
    }

    #[test]
    fn new_node_log_query_error_text() {
        let e = new_node_log_query(&q(&[("tailLines", "foo")])).unwrap_err();
        assert_eq!(
            aggregate_message(&e),
            "tailLines: Invalid value: \"foo\": strconv.Atoi: parsing \"foo\": invalid syntax"
        );
        let e = new_node_log_query(&q(&[("query", " ")])).unwrap_err();
        assert_eq!(
            aggregate_message(&e),
            "query: Invalid value: [\" \"]: may not be empty"
        );
        let e = new_node_log_query(&q(&[("tailLines", "x"), ("boot", "y")])).unwrap_err();
        assert!(aggregate_message(&e).starts_with('['));
    }

    /// `Test_validateServices`.
    #[test]
    fn validate_services_matches_upstream() {
        let long = "svc1".repeat(100);
        let cases: Vec<(&str, Vec<String>, bool)> = vec![
            ("one service", s(&["svc1"]), false),
            ("two services", s(&["svc1", "svc2"]), false),
            ("dot service", s(&["svc.foo"]), false),
            ("underscore service", s(&["svc_foo"]), false),
            ("at service", s(&["svc@foo"]), false),
            ("colon service", s(&["svc:foo"]), false),
            ("invalid service new line", s(&["svc\n"]), true),
            ("invalid service with dot", s(&["svc.foo\n"]), true),
            ("invalid service with slash", s(&["svc/foo"]), true),
            ("long service", vec![long], true),
            (
                "max number of services",
                s(&["svc1", "svc2", "svc.foo", "svc_foo", "svc@foo"]),
                true,
            ),
        ];
        for (name, services, want_err) in cases {
            assert_eq!(!validate_services(&services).is_empty(), want_err, "{name}");
        }
    }

    /// `Test_nodeLogQuery_validate`.
    #[test]
    fn validate_matches_upstream() {
        let since = DateTime::parse_from_rfc3339("2023-01-04T02:00:00Z").unwrap();
        let until = DateTime::parse_from_rfc3339("2023-02-04T02:00:00Z").unwrap();
        let svc = |o: Options| NodeLogQuery {
            services: s(&["svc1"]),
            options: o,
            ..Default::default()
        };
        let cases: Vec<(&str, NodeLogQuery, bool)> = vec![
            ("empty", NodeLogQuery::default(), true),
            (
                "empty with options",
                NodeLogQuery {
                    options: Options {
                        since_time: Some(since),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                true,
            ),
            ("one service", svc(Options::default()), false),
            (
                "two services",
                NodeLogQuery {
                    services: s(&["svc1", "svc2"]),
                    ..Default::default()
                },
                false,
            ),
            (
                "one service one file",
                NodeLogQuery {
                    services: s(&["svc1"]),
                    files: s(&["/test1.log"]),
                    ..Default::default()
                },
                true,
            ),
            (
                "two files",
                NodeLogQuery {
                    files: s(&["/test1.log", "/test2.log"]),
                    ..Default::default()
                },
                true,
            ),
            (
                "one file options",
                NodeLogQuery {
                    files: s(&["/test1.log"]),
                    options: Options {
                        pattern: "foo".into(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                true,
            ),
            (
                "invalid pattern",
                svc(Options {
                    pattern: "foo\\".into(),
                    ..Default::default()
                }),
                true,
            ),
            (
                "since",
                svc(Options {
                    since_time: Some(since),
                    ..Default::default()
                }),
                false,
            ),
            (
                "until",
                svc(Options {
                    until_time: Some(until),
                    ..Default::default()
                }),
                false,
            ),
            (
                "since until",
                svc(Options {
                    since_time: Some(until),
                    until_time: Some(since),
                    ..Default::default()
                }),
                true,
            ),
            (
                "boot",
                svc(Options {
                    boot: Some(-1),
                    ..Default::default()
                }),
                cfg!(windows),
            ),
            (
                "boot out of range",
                svc(Options {
                    boot: Some(1),
                    ..Default::default()
                }),
                true,
            ),
            (
                "tailLines",
                svc(Options {
                    tail_lines: Some(100),
                    ..Default::default()
                }),
                false,
            ),
            (
                "tailLines at max (inclusive)",
                svc(Options {
                    tail_lines: Some(100_000),
                    ..Default::default()
                }),
                false,
            ),
            (
                "tailLines out of range",
                svc(Options {
                    tail_lines: Some(100_001),
                    ..Default::default()
                }),
                true,
            ),
        ];
        for (name, n, want_err) in cases {
            let errs = n.validate();
            assert_eq!(!errs.is_empty(), want_err, "{name}: {errs:?}");
        }
    }

    /// The single-file arm of `validate` stats inside the log root only.
    #[test]
    fn validate_single_file_is_confined_to_root() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.log"), "x").unwrap();
        let root = dir.path().to_str().unwrap();
        let file = |f: &str| NodeLogQuery {
            files: s(&[f]),
            ..Default::default()
        };
        assert!(file("a.log").validate_in(root).is_empty());
        assert!(!file("missing.log").validate_in(root).is_empty());
        // os.Root rejects absolute names and `..` above the root.
        assert!(!file("/a.log").validate_in(root).is_empty());
        assert!(!file("../a.log").validate_in(root).is_empty());
        assert!(!file("sub/../../a.log").validate_in(root).is_empty());
    }

    #[test]
    fn symlink_out_of_root_is_rejected() {
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret"), "s").unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path().join("secret"), dir.path().join("l")).unwrap();
        let root = dir.path().to_str().unwrap();
        let mut out = Vec::new();
        heuristics_copy_file_logs(far(), &mut out, root, "l");
        let out = String::from_utf8(out).unwrap();
        assert!(out.contains("error getting log for l"), "{out}");
    }

    /// `Test_heuristicsCopyFileLogs`.
    #[test]
    fn heuristics_copy_file_logs_matches_upstream() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let run = |svc: &str| {
            let mut out = Vec::new();
            heuristics_copy_file_logs(far(), &mut out, root, svc);
            String::from_utf8(out).unwrap()
        };

        assert!(run("service.log").contains("log not found for service.log"));
        assert!(run("service").contains("log not found for service"));

        std::fs::write(dir.path().join("service.log"), "valid logs").unwrap();
        assert_eq!(run("service.log"), "valid logs");
        assert_eq!(run("service"), "valid logs");

        // A directory doesn't cause errors.
        std::fs::create_dir(dir.path().join("service")).unwrap();
        assert_eq!(run("service"), "valid logs");

        // Service logs return the first matching file.
        std::fs::write(dir.path().join("service").join("service.log"), "error").unwrap();
        assert_eq!(run("service"), "valid logs");
    }

    #[test]
    fn heuristics_falls_through_to_service_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("svc")).unwrap();
        std::fs::write(dir.path().join("svc").join("svc.log"), "nested").unwrap();
        let mut out = Vec::new();
        heuristics_copy_file_logs(far(), &mut out, dir.path().to_str().unwrap(), "svc");
        assert_eq!(String::from_utf8(out).unwrap(), "nested");
    }

    #[test]
    fn run_command_merges_stdout_and_stderr() {
        let mut out = Vec::new();
        let args = s(&["-c", "echo out; echo err 1>&2; exit 3"]);
        let status = run_command("sh", &args, far(), &mut out).unwrap();
        assert_eq!(status.code(), Some(3));
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("out") && text.contains("err"), "{text}");
    }

    #[test]
    fn run_command_kills_at_deadline() {
        let mut out = Vec::new();
        let start = Instant::now();
        let status = run_command(
            "sh",
            &s(&["-c", "exec sleep 20"]),
            Instant::now() + Duration::from_millis(300),
            &mut out,
        )
        .unwrap();
        assert!(!status.success());
        assert!(start.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn run_command_missing_binary_is_an_error() {
        let mut out = Vec::new();
        assert!(run_command("definitely-not-a-binary-3890", &[], far(), &mut out).is_err());
    }
}
