//! The apiextensions controllers that complete a CustomResourceDefinition's
//! lifecycle, as pure functions over the object — ports of
//!
//! - `pkg/controller/status/naming_controller.go` (`calculateNamesAndConditions`,
//!   `sync`): `status.acceptedNames` and the `NamesAccepted` condition,
//! - `pkg/controller/establish/establishing_controller.go` (`sync`): the
//!   `Established` condition,
//! - `pkg/controller/finalizer/crd_finalizer.go` (`sync`): the `Terminating`
//!   condition around deleting the instances (its queue, event filters and
//!   workers are in `finalizer_queue.rs`),
//!
//! all under `staging/src/k8s.io/apiextensions-apiserver/`, plus the helpers
//! of `pkg/apis/apiextensions/helpers.go` they share.
//!
//! Upstream runs them as informer-driven controllers started by a post-start
//! hook of the apiextensions-apiserver. Here the `start-apiextensions-
//! controllers` hook starts the workers and a CRD informer (a storage watch,
//! `run_crd_informer` in `customresourcedefinition.rs`) enqueues each CRD
//! event on their workqueues (`controller_queue.rs`); a CRD write through
//! the request path enqueues it too (deduped). The workers run the same
//! functions through the `/status` store (see `CrdRest`), so the create
//! response is not Established and the condition arrives on a later update,
//! as upstream's does.
//!
//! The `nonstructuralschema` and `apiapproval` controllers live in
//! `condition_controllers.rs`.
//!
//! The finalizer's wait for the instances to be gone is ported, and instances
//! are deleted through their store, so one holding a finalizer keeps the CRD
//! Terminating until it is really gone.

use std::collections::HashSet;

use rusternetes_common::resources::{
    CustomResourceDefinition, CustomResourceDefinitionCondition, CustomResourceDefinitionNames,
    CustomResourceDefinitionStatus,
};

/// `apiextensionsv1.Established`.
pub const ESTABLISHED: &str = "Established";
/// `apiextensionsv1.NamesAccepted`.
pub const NAMES_ACCEPTED: &str = "NamesAccepted";
/// `apiextensionsv1.Terminating`.
pub const TERMINATING: &str = "Terminating";

fn opt(s: &Option<String>) -> &str {
    s.as_deref().unwrap_or("")
}

fn list(l: &Option<Vec<String>>) -> &[String] {
    l.as_deref().unwrap_or_default()
}

/// `reflect.DeepEqual` / `Semantic.DeepEqual` over names. A Go string is
/// never absent, so `None` and `""` are the same name, and a nil slice is an
/// empty one.
pub fn names_equal(a: &CustomResourceDefinitionNames, b: &CustomResourceDefinitionNames) -> bool {
    a.plural == b.plural
        && opt(&a.singular) == opt(&b.singular)
        && a.kind == b.kind
        && opt(&a.list_kind) == opt(&b.list_kind)
        && list(&a.short_names) == list(&b.short_names)
        && list(&a.categories) == list(&b.categories)
}

fn now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// `FindCRDCondition` (`helpers.go:59-67`).
pub fn find_crd_condition<'a>(
    crd: &'a CustomResourceDefinition,
    condition_type: &str,
) -> Option<&'a CustomResourceDefinitionCondition> {
    crd.status
        .as_ref()?
        .conditions
        .as_ref()?
        .iter()
        .find(|c| c.type_ == condition_type)
}

/// A condition as the controllers build them: no transition time yet.
pub fn condition(
    type_: &str,
    status: &str,
    reason: &str,
    message: &str,
) -> CustomResourceDefinitionCondition {
    CustomResourceDefinitionCondition {
        type_: type_.to_string(),
        status: status.to_string(),
        last_transition_time: None,
        reason: Some(reason.to_string()),
        message: Some(message.to_string()),
        observed_generation: None,
    }
}

/// `SetCRDCondition` (`helpers.go:29-45`): overwrite the condition of that
/// type or append it; the transition time moves only when the status does.
pub fn set_crd_condition(
    crd: &mut CustomResourceDefinition,
    mut new_condition: CustomResourceDefinitionCondition,
) {
    new_condition.last_transition_time = Some(now());
    let conditions = crd
        .status
        .get_or_insert_with(CustomResourceDefinitionStatus::default)
        .conditions
        .get_or_insert_with(Vec::new);
    match conditions
        .iter_mut()
        .find(|c| c.type_ == new_condition.type_)
    {
        None => conditions.push(new_condition),
        Some(existing) => {
            if existing.status != new_condition.status
                || existing
                    .last_transition_time
                    .as_deref()
                    .is_none_or(str::is_empty)
            {
                existing.last_transition_time = new_condition.last_transition_time;
            }
            existing.status = new_condition.status;
            existing.reason = new_condition.reason;
            existing.message = new_condition.message;
        }
    }
}

/// `IsCRDConditionEquivalent` (`helpers.go:89-99`): equal but for the times.
pub fn is_crd_condition_equivalent(
    lhs: Option<&CustomResourceDefinitionCondition>,
    rhs: Option<&CustomResourceDefinitionCondition>,
) -> bool {
    match (lhs, rhs) {
        (None, None) => true,
        (Some(l), Some(r)) => {
            l.message.as_deref().unwrap_or("") == r.message.as_deref().unwrap_or("")
                && l.reason.as_deref().unwrap_or("") == r.reason.as_deref().unwrap_or("")
                && l.status == r.status
                && l.type_ == r.type_
        }
        _ => false,
    }
}

/// `CRDHasFinalizer` (`helpers.go:102-110`).
pub fn crd_has_finalizer(crd: &CustomResourceDefinition, needle: &str) -> bool {
    crd.metadata
        .finalizers
        .as_ref()
        .is_some_and(|f| f.iter().any(|x| x == needle))
}

/// `CRDRemoveFinalizer` (`helpers.go:113-121`).
pub fn crd_remove_finalizer(crd: &mut CustomResourceDefinition, needle: &str) {
    if let Some(f) = crd.metadata.finalizers.as_mut() {
        f.retain(|x| x != needle);
    }
}

/// `getAcceptedNamesForGroup` (naming_controller.go:105-129): every name
/// already claimed in the group, by every CRD of it, this one included.
pub fn accepted_names_for_group(
    group: &str,
    crds: &[CustomResourceDefinition],
) -> (HashSet<String>, HashSet<String>) {
    let mut all_resources = HashSet::new();
    let mut all_kinds = HashSet::new();
    for curr in crds.iter().filter(|c| c.spec.group == group) {
        let accepted = curr.status.as_ref().and_then(|s| s.accepted_names.as_ref());
        let empty = CustomResourceDefinitionNames::default();
        let accepted = accepted.unwrap_or(&empty);
        all_resources.insert(accepted.plural.clone());
        all_resources.insert(opt(&accepted.singular).to_string());
        all_resources.extend(list(&accepted.short_names).iter().cloned());
        all_kinds.insert(accepted.kind.clone());
        all_kinds.insert(opt(&accepted.list_kind).to_string());
    }
    (all_resources, all_kinds)
}

/// `equalToAcceptedOrFresh` (naming_controller.go:217-227).
fn equal_to_accepted_or_fresh(
    requested: &str,
    accepted: &str,
    used: &HashSet<String>,
) -> Result<(), String> {
    if requested == accepted || !used.contains(requested) {
        return Ok(());
    }
    Err(format!("{requested:?} is already in use"))
}

/// `utilerrors.NewAggregate(errs).Error()`: the message itself for one
/// error, the bracketed list for several (duplicates dropped).
pub fn aggregate(errs: &[String]) -> String {
    let mut seen = HashSet::new();
    let unique: Vec<&String> = errs.iter().filter(|e| seen.insert(*e)).collect();
    match unique.as_slice() {
        [one] => (*one).clone(),
        many => format!(
            "[{}]",
            many.iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// `calculateNamesAndConditions` (naming_controller.go:131-216).
pub fn calculate_names_and_conditions(
    crd: &CustomResourceDefinition,
    all_resources: &HashSet<String>,
    all_kinds: &HashSet<String>,
) -> (
    CustomResourceDefinitionNames,
    CustomResourceDefinitionCondition,
    CustomResourceDefinitionCondition,
) {
    let requested = &crd.spec.names;
    let accepted = crd
        .status
        .as_ref()
        .and_then(|s| s.accepted_names.clone())
        .unwrap_or_default();
    let mut new_names = accepted.clone();
    let mut names_accepted = condition(NAMES_ACCEPTED, "Unknown", "", "");

    let conflict = |cond: &mut CustomResourceDefinitionCondition, reason: &str, msg: String| {
        cond.status = "False".to_string();
        cond.reason = Some(reason.to_string());
        cond.message = Some(msg);
    };

    // Check each name for mismatches. If there's a mismatch between spec and
    // status, try to deconflict. Continue on errors so that the status is the
    // best match possible.
    match equal_to_accepted_or_fresh(&requested.plural, &accepted.plural, all_resources) {
        Err(e) => conflict(&mut names_accepted, "PluralConflict", e),
        Ok(()) => new_names.plural = requested.plural.clone(),
    }
    match equal_to_accepted_or_fresh(
        opt(&requested.singular),
        opt(&accepted.singular),
        all_resources,
    ) {
        Err(e) => conflict(&mut names_accepted, "SingularConflict", e),
        Ok(()) => new_names.singular = requested.singular.clone(),
    }
    if list(&requested.short_names) != list(&accepted.short_names) {
        let existing: HashSet<&String> = list(&accepted.short_names).iter().collect();
        let errs: Vec<String> = list(&requested.short_names)
            .iter()
            // If the shortname is already ours, then we're fine.
            .filter(|s| !existing.contains(s))
            .filter_map(|s| equal_to_accepted_or_fresh(s, "", all_resources).err())
            .collect();
        if errs.is_empty() {
            new_names.short_names = requested.short_names.clone();
        } else {
            conflict(&mut names_accepted, "ShortNamesConflict", aggregate(&errs));
        }
    }
    match equal_to_accepted_or_fresh(&requested.kind, &accepted.kind, all_kinds) {
        Err(e) => conflict(&mut names_accepted, "KindConflict", e),
        Ok(()) => new_names.kind = requested.kind.clone(),
    }
    match equal_to_accepted_or_fresh(
        opt(&requested.list_kind),
        opt(&accepted.list_kind),
        all_kinds,
    ) {
        Err(e) => conflict(&mut names_accepted, "ListKindConflict", e),
        Ok(()) => new_names.list_kind = requested.list_kind.clone(),
    }
    new_names.categories = requested.categories.clone();

    // If we haven't changed the condition, then our names must be good.
    if names_accepted.status == "Unknown" {
        names_accepted.status = "True".to_string();
        names_accepted.reason = Some("NoConflicts".to_string());
        names_accepted.message = Some("no conflicts found".to_string());
    }

    // Set Established initially to false; the establishing controller sets it
    // to true once it sees NamesAccepted, so the endpoint is only served
    // after the names are.
    let mut established = condition(
        ESTABLISHED,
        "False",
        "NotAccepted",
        "not all names are accepted",
    );
    if let Some(old) = find_crd_condition(crd, ESTABLISHED) {
        established = old.clone();
    }
    if established.status != "True" && names_accepted.status == "True" {
        established = condition(
            ESTABLISHED,
            "False",
            "Installing",
            "the initial names have been accepted",
        );
    }
    (new_names, names_accepted, established)
}

/// `NamingConditionController.sync` (naming_controller.go:240-292): the CRD
/// to write, or `None` when there is nothing to do. `group` is every CRD in
/// the CRD's group (it may include the CRD itself).
pub fn sync_names(
    crd: &CustomResourceDefinition,
    group: &[CustomResourceDefinition],
) -> Option<CustomResourceDefinition> {
    let status_names = crd.status.as_ref().and_then(|s| s.accepted_names.as_ref());
    // Skip checking names if spec and status names are the same.
    if let Some(accepted) = status_names {
        if names_equal(&crd.spec.names, accepted) {
            return None;
        }
    }

    let (all_resources, all_kinds) = accepted_names_for_group(&crd.spec.group, group);
    let (accepted_names, naming_condition, established_condition) =
        calculate_names_and_conditions(crd, &all_resources, &all_kinds);

    // Nothing to do if accepted names and the NamesAccepted condition did not
    // change.
    let unchanged_names = status_names.is_some_and(|a| names_equal(a, &accepted_names))
        || (status_names.is_none() && names_equal(&accepted_names, &Default::default()));
    if unchanged_names
        && is_crd_condition_equivalent(
            Some(&naming_condition),
            find_crd_condition(crd, NAMES_ACCEPTED),
        )
    {
        return None;
    }

    let mut out = crd.clone();
    out.status
        .get_or_insert_with(CustomResourceDefinitionStatus::default)
        .accepted_names = Some(accepted_names);
    set_crd_condition(&mut out, naming_condition);
    set_crd_condition(&mut out, established_condition);
    Some(out)
}

/// `EstablishingController.sync` (establishing_controller.go:127-171): the
/// CRD to write, or `None` when it is not accepted yet or already
/// established.
pub fn sync_establishing(crd: &CustomResourceDefinition) -> Option<CustomResourceDefinition> {
    let is_true = |t: &str| find_crd_condition(crd, t).is_some_and(|c| c.status == "True");
    if !is_true(NAMES_ACCEPTED) || is_true(ESTABLISHED) {
        return None;
    }
    let mut out = crd.clone();
    // If the conversion webhook CABundle is invalid, set the Established
    // condition to false and provide a reason (:143-158).
    let invalid_ca_bundle = crd
        .spec
        .conversion
        .as_ref()
        .and_then(|c| c.webhook.as_ref())
        .and_then(|w| w.client_config.ca_bundle.as_deref())
        .is_some_and(rusternetes_common::validation::crd::is_invalid_ca_bundle);
    let established = if invalid_ca_bundle {
        condition(
            ESTABLISHED,
            "False",
            "InvalidCABundle",
            "The conversion webhook CABundle is invalid",
        )
    } else {
        condition(
            ESTABLISHED,
            "True",
            "InitialNamesAccepted",
            "the initial names have been accepted",
        )
    };
    // The same condition again is no write: an update that changes nothing
    // makes no new revision upstream, so no event for the controllers to
    // loop on.
    if invalid_ca_bundle
        && is_crd_condition_equivalent(Some(&established), find_crd_condition(crd, ESTABLISHED))
    {
        return None;
    }
    set_crd_condition(&mut out, established);
    Some(out)
}
