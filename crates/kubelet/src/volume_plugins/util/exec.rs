//! Port of the slice of `k8s.io/utils/exec` (`exec.go`) the volume plugins use:
//! `Interface.Command(..).CombinedOutput()`, `ExitError.ExitStatus()` and
//! `ErrExecutableNotFound` (`exec.go:29`, `:204-207`); plus the `FakeExec` /
//! `volumetest.ScriptCommands` pair (`k8s.io/utils/exec/testing/fake_exec.go`,
//! `pkg/volume/testing/testing.go:104-135`) that upstream's iscsi and
//! mount-utils tests drive.

use std::process::Command;

/// The error half of `CombinedOutput()`: `utilexec.ErrExecutableNotFound`, a
/// `utilexec.ExitError`, or any other start failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecError {
    /// `utilexec.ErrExecutableNotFound` (`exec.go:29`).
    NotFound,
    /// `utilexec.ExitError` with `ExitStatus()` = `code` (`-1` when the process
    /// was killed by a signal, as `os/exec` reports it).
    Exit { code: i32, message: String },
    /// Any other failure to run the command.
    Other(String),
}

impl ExecError {
    /// `ExitError.ExitStatus()`; `None` when this is not an `ExitError`
    /// (the `err.(utilexec.ExitError)` assertion failing).
    pub fn exit_status(&self) -> Option<i32> {
        match self {
            ExecError::Exit { code, .. } => Some(*code),
            _ => None,
        }
    }
}

impl std::fmt::Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecError::NotFound => write!(f, "executable file not found in $PATH"),
            ExecError::Exit { message, .. } => write!(f, "{message}"),
            ExecError::Other(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for ExecError {}

/// `utilexec.Interface` restricted to `Command(cmd, args...).CombinedOutput()`,
/// the only form the iscsi plugin and mount-utils' format path use. Returns the
/// combined output together with the error, as Go's `([]byte, error)` does.
pub trait Exec: Send + Sync {
    fn combined_output(&self, cmd: &str, args: &[String]) -> (String, Result<(), ExecError>);
}

/// `utilexec.New()`.
#[derive(Clone, Copy, Debug, Default)]
pub struct RealExec;

impl Exec for RealExec {
    fn combined_output(&self, cmd: &str, args: &[String]) -> (String, Result<(), ExecError>) {
        match Command::new(cmd).args(args).output() {
            Ok(out) => {
                // Go's `CombinedOutput` points stdout and stderr at one buffer.
                let combined = format!(
                    "{}{}",
                    String::from_utf8_lossy(&out.stdout),
                    String::from_utf8_lossy(&out.stderr)
                );
                if out.status.success() {
                    (combined, Ok(()))
                } else {
                    (
                        combined,
                        Err(ExecError::Exit {
                            code: out.status.code().unwrap_or(-1),
                            message: out.status.to_string(),
                        }),
                    )
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                (String::new(), Err(ExecError::NotFound))
            }
            Err(e) => (String::new(), Err(ExecError::Other(e.to_string()))),
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    //! `FakeExec` with `ExactOrder`, scripted by `CommandScript`s.
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// `volumetest.CommandScript` (`testing.go:106-116`).
    #[derive(Clone, Debug, Default)]
    pub(crate) struct CommandScript {
        pub cmd: &'static str,
        pub args: Vec<String>,
        pub output: String,
        pub return_code: i32,
        /// A non-`ExitError` failure (`fmt.Errorf("formatting failed")`).
        pub error: Option<&'static str>,
    }

    pub(crate) fn script(cmd: &'static str, args: &[&str]) -> CommandScript {
        CommandScript {
            cmd,
            args: args.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    /// `testingexec.FakeExec` after `ScriptCommands` with `ExactOrder = true`
    /// (`fake_exec.go:52-71`): each call consumes the next script and panics on a
    /// different command or arguments, or when the scripts run out.
    pub(crate) struct FakeExec {
        scripts: Mutex<VecDeque<CommandScript>>,
        pub(crate) calls: std::sync::atomic::AtomicUsize,
        /// Every `(cmd, args)` seen, for assertions beyond the scripts.
        pub(crate) log: Mutex<Vec<Vec<String>>>,
    }

    impl FakeExec {
        pub(crate) fn new(scripts: Vec<CommandScript>) -> Self {
            Self {
                scripts: Mutex::new(scripts.into()),
                calls: Default::default(),
                log: Default::default(),
            }
        }

        /// `fakeExec.CommandCalls`.
        pub(crate) fn command_calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl Exec for FakeExec {
        fn combined_output(&self, cmd: &str, args: &[String]) -> (String, Result<(), ExecError>) {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut argv = vec![cmd.to_string()];
            argv.extend(args.iter().cloned());
            self.log.lock().unwrap().push(argv);
            let Some(s) = self.scripts.lock().unwrap().pop_front() else {
                panic!("ran out of Command() actions. Could not handle command [{n}]: {cmd} args: {args:?}");
            };
            if cmd != s.cmd {
                panic!("received command: {cmd}, expected: {}", s.cmd);
            }
            if args != s.args.as_slice() {
                panic!(
                    "command ({cmd}) called with unexpected arguments. Expected {:?}, Received {args:?}",
                    s.args
                );
            }
            let err = if let Some(m) = s.error {
                Some(ExecError::Other(m.to_string()))
            } else {
                (s.return_code != 0).then(|| ExecError::Exit {
                    code: s.return_code,
                    message: format!("exit status {}", s.return_code),
                })
            };
            (s.output, err.map_or(Ok(()), Err))
        }
    }

    /// A `FakeExec` whose every call panics: `&testingexec.FakeExec{}` with no
    /// scripts (`doTestPlugin`).
    pub(crate) fn no_commands() -> FakeExec {
        FakeExec::new(vec![])
    }
}
