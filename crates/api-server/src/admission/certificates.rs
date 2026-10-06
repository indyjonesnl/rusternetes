//! The `certificates` admission plugins for CertificateSigningRequests.
//!
//! Ports of
//! `plugin/pkg/admission/certificates/approval/admission.go` (`Validate`,
//! :84-103) and `plugin/pkg/admission/certificates/signing/admission.go`
//! (`Validate`, :69-100), both built on `IsAuthorizedForSignerName`
//! (`pkg/certauthorization/certauthorization.go:31-90`).
//!
//! Not ported: `certificates/ctbattest` (no ClusterTrustBundle resource here)
//! and `certificates/subjectrestriction` (needs the parsed PKCS#10 subject).

use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Decision, RequestAttributes};
use rusternetes_common::resources::{CertificateSigningRequest, CertificateSigningRequestStatus};

use crate::state::ApiServerState;

/// `buildAttributes` (certauthorization.go:60-72): the synthetic `signers`
/// resource, named by the signer, in `certificates.k8s.io`.
fn signer_attributes(user: &UserInfo, verb: &str, signer_name: &str) -> RequestAttributes {
    RequestAttributes::new(user.clone(), verb, "signers")
        .with_api_group("certificates.k8s.io")
        .with_name(signer_name)
}

/// `IsAuthorizedForSignerName` (certauthorization.go:31-58): `verb` on the
/// named signer, or else on `<domain portion>/*`. An authorizer error counts
/// as a refusal, as upstream only returns true on `DecisionAllow`.
pub async fn is_authorized_for_signer_name(
    state: &ApiServerState,
    user: &UserInfo,
    verb: &str,
    signer_name: &str,
) -> bool {
    let attrs = signer_attributes(user, verb, signer_name);
    if matches!(
        state.authorizer.authorize(&attrs).await,
        Ok(Decision::Allow)
    ) {
        return true;
    }
    // `buildWildcardAttributes` (:74-79): `strings.Split(signerName, "/")[0]`.
    let domain = signer_name.split('/').next().unwrap_or_default();
    let attrs = signer_attributes(user, verb, &format!("{domain}/*"));
    matches!(
        state.authorizer.authorize(&attrs).await,
        Ok(Decision::Allow)
    )
}

fn certificate(s: Option<&CertificateSigningRequestStatus>) -> Option<String> {
    s.and_then(|s| s.certificate.clone())
}

/// The conditions as `apiequality.Semantic.DeepEqual` compares them: its
/// forked `DeepEqual` treats a nil and an empty slice as equal.
fn conditions(s: Option<&CertificateSigningRequestStatus>) -> Option<serde_json::Value> {
    let c = s.and_then(|s| s.conditions.as_ref());
    serde_json::to_value(c.map(Vec::as_slice).unwrap_or_default()).ok()
}

/// `approval.Plugin.Validate` and `signing.Plugin.Validate`. Both register for
/// UPDATE only; the caller passes the old and the new CSR of an update.
///
/// Returns the error text to wrap in `admission.NewForbidden`. The checks use
/// the *old* object's signer name, so an update cannot dodge them by changing
/// `spec.signerName` (approval/admission.go:73-75).
pub async fn validate_update(
    state: &ApiServerState,
    user: &UserInfo,
    subresource: Option<&str>,
    new: &CertificateSigningRequest,
    old: &CertificateSigningRequest,
) -> Option<String> {
    let signer = &old.spec.signer_name;
    match subresource {
        // approval/admission.go:67-71: only `certificatesigningrequests/approval`.
        Some("approval") => {
            if !is_authorized_for_signer_name(state, user, "approve", signer).await {
                return Some(format!(
                    "user not permitted to approve requests with signerName \"{signer}\""
                ));
            }
        }
        // signing/admission.go:69-100: only `/status`, and only when the
        // certificate or the conditions changed (:84-87).
        Some("status") => {
            let (old_status, new_status) = (old.status.as_ref(), new.status.as_ref());
            if certificate(old_status) == certificate(new_status)
                && conditions(old_status) == conditions(new_status)
            {
                return None;
            }
            if !is_authorized_for_signer_name(state, user, "sign", signer).await {
                return Some(format!(
                    "user not permitted to sign requests with signerName \"{signer}\""
                ));
            }
        }
        _ => {}
    }
    None
}
