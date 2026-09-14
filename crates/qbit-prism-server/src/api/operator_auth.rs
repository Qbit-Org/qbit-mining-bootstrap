//! Optional bearer authorization for the operator listener. The independent
//! public service never consults it.
use super::{json_response, HeaderMap, Response, StatusCode};
use axum::http::{header, HeaderValue};
use serde_json::json;
use sha2::{Digest, Sha256};

/// Accept exactly one `Bearer` credential equal to the configured token.
/// Both sides are digested first, so the fixed-length comparison reveals
/// neither a matching prefix nor the configured length.
pub(super) fn authorized(headers: &HeaderMap, expected: &str) -> bool {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return false;
    };
    let Some((scheme, presented)) = value.to_str().ok().and_then(|v| v.split_once(' ')) else {
        return false;
    };
    let presented = Sha256::digest(presented.trim().as_bytes());
    let expected = Sha256::digest(expected.as_bytes());
    let difference = presented
        .iter()
        .zip(expected.iter())
        .fold(0u8, |difference, (a, b)| difference | (a ^ b));
    scheme.eq_ignore_ascii_case("bearer") & (difference == 0)
}

/// The refusal names no credential detail, so probes cannot learn why it failed.
pub(super) fn challenge() -> Response {
    let mut response = json_response(StatusCode::UNAUTHORIZED, json!({"error":"unauthorized"}));
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Bearer realm=\"prism-operator\""),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(values: &[&str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append(header::AUTHORIZATION, HeaderValue::from_str(value).unwrap());
        }
        headers
    }

    #[test]
    fn only_one_exact_bearer_credential_is_accepted() {
        let token = "operator-token-0123456789";
        assert!(authorized(&headers(&[&format!("Bearer {token}")]), token));
        assert!(authorized(&headers(&[&format!("bearer {token}")]), token));
        for rejected in [
            vec![],
            vec!["Bearer"],
            vec!["Bearer "],
            vec!["Bearer operator-token-012345678"],
            vec!["Bearer operator-token-0123456789x"],
            vec!["Basic b3BlcmF0b3ItdG9rZW4tMDEyMzQ1Njc4OQ=="],
            vec!["operator-token-0123456789"],
        ] {
            assert!(!authorized(&headers(&rejected), token), "{rejected:?}");
        }
        let valid = format!("Bearer {token}");
        assert!(
            !authorized(&headers(&[&valid, &valid]), token),
            "repeated credentials are ambiguous"
        );
    }
}
