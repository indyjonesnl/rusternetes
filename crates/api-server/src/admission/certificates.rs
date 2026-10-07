//! The `certificates` admission plugins for CertificateSigningRequests.
//!
//! Ports of
//! `plugin/pkg/admission/certificates/approval/admission.go` (`Validate`,
//! :84-103) and `plugin/pkg/admission/certificates/signing/admission.go`
//! (`Validate`, :69-100), both built on `IsAuthorizedForSignerName`
//! (`pkg/certauthorization/certauthorization.go:31-90`).
//!
//! `certificates/subjectrestriction` (`Validate`, :64-93) is
//! [`subject_restriction_error`].
//!
//! `certificates/ctbattest` (`Validate`, :86-122) is
//! [`validate_cluster_trust_bundle_attest`].

use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Decision, RequestAttributes};
use rusternetes_common::resources::podcertificaterequest::PodCertificateRequest;
use rusternetes_common::resources::{
    CertificateSigningRequest, CertificateSigningRequestStatus, ClusterTrustBundle,
};

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

/// `ctbattest.Plugin.Validate` (plugin/pkg/admission/certificates/ctbattest/
/// admission.go:86-122), registered for CREATE and UPDATE: setting
/// `spec.signerName` on a ClusterTrustBundle needs the `attest` verb on the
/// synthetic `signers` resource named by the signer (or `<domain>/*`).
///
/// `old` is the stored object of an UPDATE. Returns the error text to wrap in
/// `admission.NewForbidden`.
pub async fn validate_cluster_trust_bundle_attest(
    state: &ApiServerState,
    user: &UserInfo,
    new: &ClusterTrustBundle,
    old: Option<&ClusterTrustBundle>,
) -> Option<String> {
    // `p.enabled` (:62-65, :80-82): the plugin is inert with the gate off.
    if !rusternetes_common::feature_gates::enabled(
        rusternetes_common::feature_gates::Feature::ClusterTrustBundle,
    ) {
        return None;
    }
    // Validate against the *new* object: updates to signer name are rejected
    // during validation (:97-98). No signer, no attest check (:100-103).
    let signer = &new.spec.signer_name;
    if signer.is_empty() {
        return None;
    }
    // Skip when the semantics are unchanged, to support storage migration
    // and GC workflows (:105-108).
    if old.is_some_and(|old| {
        crate::registry::rbac::escalation_check::is_only_mutating_gc_fields(new, old)
    }) {
        return None;
    }
    if !is_authorized_for_signer_name(state, user, "attest", signer).await {
        return Some(format!(
            "user not permitted to attest for signerName \"{signer}\""
        ));
    }
    None
}

/// The `"sign"` check of `StatusStrategy.ValidateUpdate`
/// (pkg/registry/certificates/podcertificaterequest/strategy.go:153-167): a
/// caller that changes any `/status` field of a PodCertificateRequest needs
/// the `sign` verb on the *old* object's signer name.
///
/// Deviation: upstream returns `field.Forbidden(spec.signerName, ...)` from the
/// strategy once status validation has passed; the Rust strategy is
/// synchronous and cannot call the authorizer, so the check runs here with the
/// same message, as a Forbidden. Returns the error text.
pub async fn validate_pod_certificate_request_sign(
    state: &ApiServerState,
    user: &UserInfo,
    new: &PodCertificateRequest,
    old: &PodCertificateRequest,
) -> Option<String> {
    let status = |r: &PodCertificateRequest| serde_json::to_value(&r.status).ok();
    if status(new) == status(old) {
        return None;
    }
    let signer = &old.spec.signer_name;
    if !is_authorized_for_signer_name(state, user, "sign", signer).await {
        return Some(format!(
            "User \"{}\" is not permitted to \"sign\" for signer \"{signer}\"",
            user.username
        ));
    }
    None
}

/// `certificatesv1beta1.KubeAPIServerClientSignerName`
/// (staging/src/k8s.io/api/certificates/v1beta1/types.go:150).
const KUBE_APISERVER_CLIENT_SIGNER_NAME: &str = "kubernetes.io/kube-apiserver-client";

/// `certificatesapi.ParseCSR` (pkg/apis/certificates/helpers.go:31-41) over
/// `spec.request`, whose JSON form is the base64 of the PEM. Returns the
/// organizations of the subject, or the error text.
fn parse_csr_organizations(request: &str) -> Result<Vec<String>, String> {
    use base64::Engine;
    use x509_parser::prelude::FromDer;
    const NOT_CSR: &str = "PEM block type must be CERTIFICATE REQUEST";
    let pem_bytes = base64::engine::general_purpose::STANDARD
        .decode(request)
        .map_err(|_| NOT_CSR.to_string())?;
    let block = pem::parse(&pem_bytes).map_err(|_| NOT_CSR.to_string())?;
    if block.tag() != "CERTIFICATE REQUEST" {
        return Err(NOT_CSR.to_string());
    }
    let (_, csr) =
        x509_parser::certification_request::X509CertificationRequest::from_der(block.contents())
            .map_err(|e| e.to_string())?;
    Ok(csr
        .certification_request_info
        .subject
        .iter_organization()
        .filter_map(|o| o.as_str().ok().map(str::to_string))
        .collect())
}

/// `subjectrestriction.Plugin.Validate` (subjectrestriction/admission.go:64-93).
/// Registered for CREATE only (:55, `admission.NewHandler(admission.Create)`);
/// the caller has already matched `certificatesigningrequests` with no
/// subresource (:65-67). A CSR for the `kube-apiserver-client` signer may not
/// request the `system:masters` organization.
///
/// Returns the error text to wrap in `admission.NewForbidden`.
pub fn subject_restriction_error(csr: &CertificateSigningRequest) -> Option<String> {
    if csr.spec.signer_name != KUBE_APISERVER_CLIENT_SIGNER_NAME {
        return None;
    }
    let organizations = match parse_csr_organizations(&csr.spec.request) {
        Ok(o) => o,
        Err(e) => return Some(format!("failed to parse CSR: {e}")),
    };
    if organizations.iter().any(|g| g == "system:masters") {
        return Some(format!(
            "use of {KUBE_APISERVER_CLIENT_SIGNER_NAME} signer with system:masters group is not allowed"
        ));
    }
    None
}

#[cfg(test)]
mod subject_restriction_tests {
    //! Cases of subjectrestriction/admission_test.go `TestPlugin_Validate`
    //! (:34-110). "ignored resource", "ignored subresource" and "wrong type"
    //! are the caller's resource match and Rust's typing.
    use super::*;
    use base64::Engine;

    /// `pemWithGroup` (admission_test.go:166-191), base64'd as `spec.request`.
    fn request_with_group(group: &str) -> String {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::OrganizationName, group);
        let pem = params.serialize_request(&key).unwrap().pem().unwrap();
        base64::engine::general_purpose::STANDARD.encode(pem)
    }

    fn csr(request: String, signer: &str) -> CertificateSigningRequest {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": {"name": "x"},
            "spec": {"request": request, "signerName": signer, "usages": []}
        }))
        .unwrap()
    }

    #[test]
    fn some_other_signer() {
        let c = csr(
            request_with_group("system:masters"),
            "kubernetes.io/kube-apiserver-client-kubelet",
        );
        assert_eq!(subject_restriction_error(&c), None);
    }

    #[test]
    fn invalid_request() {
        let c = csr(
            base64::engine::general_purpose::STANDARD.encode("this is not a CSR"),
            KUBE_APISERVER_CLIENT_SIGNER_NAME,
        );
        assert_eq!(
            subject_restriction_error(&c).as_deref(),
            Some("failed to parse CSR: PEM block type must be CERTIFICATE REQUEST")
        );
    }

    #[test]
    fn some_other_group() {
        let c = csr(
            request_with_group("system:admin"),
            KUBE_APISERVER_CLIENT_SIGNER_NAME,
        );
        assert_eq!(subject_restriction_error(&c), None);
    }

    #[test]
    fn request_for_system_masters() {
        let c = csr(
            request_with_group("system:masters"),
            KUBE_APISERVER_CLIENT_SIGNER_NAME,
        );
        assert_eq!(
            subject_restriction_error(&c).as_deref(),
            Some(
                "use of kubernetes.io/kube-apiserver-client signer with system:masters group is not allowed"
            )
        );
    }
}
