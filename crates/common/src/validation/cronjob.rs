//! CronJob validation — port of upstream Kubernetes
//! `pkg/apis/batch/validation/validation.go:756-873` (release-1.35).
//!
//! `ValidateCronJobCreate` and `ValidateCronJobUpdate`, both over
//! `validateCronJobSpec`: schedule (required, cron syntax, inline TZ),
//! `startingDeadlineSeconds` ≥ 0, `timeZone` naming rules, `concurrencyPolicy`
//! enum, the embedded `jobTemplate` (`ValidateJobTemplateSpec`), and the
//! history-limit non-negativity checks. Create also caps the name at 52
//! characters (the controller appends an 11-char `-$TIMESTAMP` suffix when
//! creating Jobs); update does not, so older CronJobs stay editable.
//!
//! `timeZone` is fully validated: the IANA naming-character rules, the `Local`
//! rejection, and the tz-DB existence check (upstream `time.LoadLocation`),
//! resolved via `chrono_tz` — the same lookup the CronJob controller schedules
//! with, so a zone accepted at create is also schedulable.

use crate::resources::{CronJob, CronJobSpec};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::job::validate_job_template_spec;
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_nonnegative_field, validate_object_meta,
    validate_object_meta_update,
};
use once_cell::sync::Lazy;
use regex::Regex;

const ALLOW_CONCURRENT: &str = "Allow";
const FORBID_CONCURRENT: &str = "Forbid";
const REPLACE_CONCURRENT: &str = "Replace";

/// IANA timezone name-component charset (mirrors upstream `validTimeZoneCharacters`).
static VALID_TZ_CHARS: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[A-Za-z\.\-_0-9+]{1,14}$").unwrap());

/// Parse a Kubernetes 5-field cron schedule using the same normalization the
/// CronJob controller applies (`?`→`*`, pad to the `cron` crate's 7-field form),
/// so create-time validation accepts exactly what the controller will run.
///
/// A leading `TZ=<zone>` / `CRON_TZ=<zone>` is stripped and its zone resolved
/// first, as `robfig/cron/v3` `Parser.Parse` does (parser.go:95-103), so a
/// grandfathered inline-TZ schedule parses. With no space after the prefix
/// upstream's slice panics, which `ParseCronScheduleWithPanicRecovery`
/// (pkg/util/parsers/parsers.go:59-69) turns into an error.
pub fn parse_schedule(schedule: &str) -> Result<(), String> {
    let mut spec = schedule;
    if spec.starts_with("TZ=") || spec.starts_with("CRON_TZ=") {
        let Some(i) = spec.find(' ') else {
            return Err(format!(
                "invalid schedule format: slice bounds out of range in {spec:?}"
            ));
        };
        let eq = spec.find('=').unwrap_or(0);
        let loc = &spec[eq + 1..i];
        if loc.parse::<chrono_tz::Tz>().is_err() {
            return Err(format!(
                "provided bad location {loc}: unknown time zone {loc}"
            ));
        }
        spec = spec[i..].trim();
    }
    let normalized = spec.replace('?', "*");
    let normalized = match normalized.split_whitespace().count() {
        5 => format!("0 {} *", normalized),
        6 => format!("0 {}", normalized),
        _ => normalized,
    };
    cron::Schedule::try_from(normalized.as_str())
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Port of upstream `validateScheduleFormat` (validation.go:833-848). An
/// inline TZ is allowed only when the old schedule already had one, and never
/// together with `timeZone`.
fn validate_schedule_format(
    schedule: &str,
    allow_tz_in_schedule: bool,
    time_zone: Option<&str>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Err(e) = parse_schedule(schedule) {
        errs.push(Error::invalid(fld_path, schedule.to_string(), e));
    }
    let has_tz = schedule.contains("TZ");
    if allow_tz_in_schedule && has_tz && time_zone.is_some() {
        errs.push(Error::invalid(
            fld_path,
            schedule.to_string(),
            "cannot use both timeZone field and TZ or CRON_TZ in schedule",
        ));
    } else if !allow_tz_in_schedule && has_tz {
        errs.push(Error::invalid(
            fld_path,
            schedule.to_string(),
            "cannot use TZ or CRON_TZ in schedule, use timeZone field instead",
        ));
    }
    errs
}

/// Port of upstream `validateTimeZone`: naming-character rules, the `Local`
/// rejection, and the tz-DB existence check (upstream `time.LoadLocation`),
/// resolved via `chrono_tz` — the same lookup the CronJob controller uses.
pub fn validate_time_zone(time_zone: Option<&str>, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(tz) = time_zone else {
        return errs;
    };
    if tz.is_empty() {
        errs.push(Error::invalid(
            fld_path,
            String::new(),
            "timeZone must be nil or non-empty string",
        ));
        return errs;
    }
    for part in tz.split('/') {
        if part == "." || part == ".." || part.starts_with('-') || !VALID_TZ_CHARS.is_match(part) {
            errs.push(Error::invalid(
                fld_path,
                tz.to_string(),
                format!("unknown time zone {}", tz),
            ));
            return errs;
        }
    }
    if tz.eq_ignore_ascii_case("Local") {
        errs.push(Error::invalid(
            fld_path,
            tz.to_string(),
            "timeZone must be an explicit time zone as defined in https://www.iana.org/time-zones",
        ));
        // Upstream `time.LoadLocation("Local")` succeeds, so it adds no further
        // error here — return before the tz-DB check to match that behaviour.
        return errs;
    }
    // tz-DB existence — upstream `time.LoadLocation`. Resolve against the same
    // IANA database the CronJob controller schedules with so a zone accepted at
    // create is also schedulable.
    if tz.parse::<chrono_tz::Tz>().is_err() {
        errs.push(Error::invalid(
            fld_path,
            tz.to_string(),
            format!("unknown time zone {tz}"),
        ));
    }
    errs
}

/// Upstream `validateConcurrencyPolicy` (validation.go:818-831). The field is
/// defaulted to `Allow`, so an empty one is a client clearing it.
fn validate_concurrency_policy(policy: Option<&str>, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    match policy.unwrap_or("") {
        ALLOW_CONCURRENT | FORBID_CONCURRENT | REPLACE_CONCURRENT => {}
        "" => errs.push(Error::required(fld_path, "")),
        other => errs.push(Error::not_supported(
            fld_path,
            other.to_string(),
            &[ALLOW_CONCURRENT, FORBID_CONCURRENT, REPLACE_CONCURRENT],
        )),
    }
    errs
}

/// Upstream `validateCronJobSpec` (validation.go:782-816). `old_spec` is the
/// stored spec on update: it decides whether an inline TZ is tolerated, and an
/// unchanged `timeZone` is not revalidated.
fn validate_cron_job_spec(
    spec: &CronJobSpec,
    old_spec: Option<&CronJobSpec>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if spec.schedule.is_empty() {
        errs.push(Error::required(&fld_path.child("schedule"), ""));
    } else {
        let allow_tz_in_schedule = old_spec.is_some_and(|old| old.schedule.contains("TZ"));
        errs.extend(validate_schedule_format(
            &spec.schedule,
            allow_tz_in_schedule,
            spec.time_zone.as_deref(),
            &fld_path.child("schedule"),
        ));
    }

    if let Some(sds) = spec.starting_deadline_seconds {
        errs.extend(validate_nonnegative_field(
            sds,
            &fld_path.child("startingDeadlineSeconds"),
        ));
    }

    if old_spec.is_none_or(|old| old.time_zone != spec.time_zone) {
        errs.extend(validate_time_zone(
            spec.time_zone.as_deref(),
            &fld_path.child("timeZone"),
        ));
    }

    errs.extend(validate_concurrency_policy(
        spec.concurrency_policy.as_deref(),
        &fld_path.child("concurrencyPolicy"),
    ));
    // `old_spec` carries the stored template: upstream derives the pod
    // options from it (`GetValidationOptionsFromPodTemplate(new, old)`,
    // cronjob/strategy.go:114,147).
    errs.extend(validate_job_template_spec(
        &spec.job_template,
        old_spec.map(|o| &o.job_template.spec.template),
        &fld_path.child("jobTemplate"),
    ));

    // Zero is a valid history limit.
    if let Some(s) = spec.successful_jobs_history_limit {
        errs.extend(validate_nonnegative_field(
            s as i64,
            &fld_path.child("successfulJobsHistoryLimit"),
        ));
    }
    if let Some(f) = spec.failed_jobs_history_limit {
        errs.extend(validate_nonnegative_field(
            f as i64,
            &fld_path.child("failedJobsHistoryLimit"),
        ));
    }

    errs
}

/// Upstream `ValidateCronJobCreate` (validation.go:757-770). "CronJobs and
/// rcs have the same name validation": `NameIsDNSSubdomain`.
pub fn validate_cron_job_create(cj: &CronJob) -> ErrorList {
    let mut errs = validate_object_meta(
        &cj.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_cron_job_spec(&cj.spec, None, &Path::new("spec")));
    // DNS1035LabelMaxLength - 11: the Job name the controller derives must
    // stay within 63 characters.
    if cj.metadata.name.len() > 52 {
        errs.push(Error::invalid(
            &Path::new("metadata").child("name"),
            cj.metadata.name.clone(),
            "must be no more than 52 characters",
        ));
    }
    errs
}

/// Upstream `ValidateCronJobUpdate` (validation.go:772-780). The 52-character
/// name cap is skipped so older CronJobs can still be updated and deleted.
pub fn validate_cron_job_update(cj: &CronJob, old: &CronJob) -> ErrorList {
    let mut errs = validate_object_meta_update(&cj.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_cron_job_spec(
        &cj.spec,
        Some(&old.spec),
        &Path::new("spec"),
    ));
    errs
}
