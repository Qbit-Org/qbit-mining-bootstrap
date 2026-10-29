//! `sslmode=verify-ca` checks the server's certificate against the trusted
//! CAs but not against the host name; `verify-full` checks both (#713): the
//! launchbadge/sqlx#3861 backport in vendor/sqlx-core.
//!
//! As libpq does, verify-ca trusts only the root certificates `sslrootcert`
//! names, and refuses to connect without one: a local change in
//! vendor/sqlx-core. With no name check, every root it trusts vouches for the
//! server under any name. sqlx-core trusts Mozilla's public roots beside
//! `sslrootcert`, so with #3861 alone verify-ca accepted any publicly issued
//! certificate, for an attacker's own domain too. verify-full keeps the
//! public roots.
//!
//! sqlx-postgres 0.8.6 runs both modes through sqlx-core's rustls client
//! with `accept_invalid_certs: false`, and verify-ca adds
//! `accept_invalid_hostnames: true`. That verifier runs rustls's WebPKI
//! checks, which test the chain before the name, and accepts the certificate
//! when the name is the only failure. rustls 0.23.24 and later report that
//! failure as `NotValidForNameContext`, which sqlx-core 0.8.6 did not match,
//! so verify-ca refused a CA-signed certificate for another name, as
//! verify-full does. The load harness's delay proxy is dialled as 127.0.0.1
//! with the database's `sslmode` kept, so every verify-ca run through it
//! failed.
//!
//! These tests run sqlx-core's rustls client against a rustls server over an
//! in-memory socket, polling by hand, so no network or timing is involved.
//! The certificates are built here: a CA the client trusts, a server
//! certificate it signs for `db.internal` only, and the same server
//! certificate from an issuer the client does not trust. The keys are Ed25519
//! from fixed seeds. Against the unpatched crate the first test fails with
//! the name error the second expects. Without the local change, verify-ca
//! without `sslrootcert`, or with one that holds no certificate, reports an
//! unknown issuer instead of the configuration error, and the public-roots
//! test sees verify-ca take the public root as the issuer, as verify-full
//! does.
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::sign::SigningKey;
use rustls::{CertificateError, ServerConfig, ServerConnection, SignatureScheme};
use sqlx_core::bytes::BufMut;
use sqlx_core::io::ReadBuf;
use sqlx_core::net::tls::{self, CertificateInput, TlsConfig};
use sqlx_core::net::{Socket, SocketIntoBox};
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// The only name the server certificate is valid for.
const SERVER_NAME: &str = "db.internal";
/// The host the delay proxy rewrites a database URL to.
const PROXY_HOST: &str = "127.0.0.1";

#[derive(Default)]
struct Wire {
    to_server: Vec<u8>,
    to_client: VecDeque<u8>,
    /// Every byte the client has written.
    client_sent: usize,
}

/// The client's end of the wire. Writes always fit; a read with nothing to
/// read blocks until the server has answered.
struct ClientSocket(Arc<Mutex<Wire>>);

impl Socket for ClientSocket {
    fn try_read(&mut self, buf: &mut dyn ReadBuf) -> io::Result<usize> {
        let mut wire = self.0.lock().unwrap();
        if wire.to_client.is_empty() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let len = buf.remaining_mut().min(wire.to_client.len());
        let bytes: Vec<u8> = wire.to_client.drain(..len).collect();
        buf.put_slice(&bytes);
        Ok(len)
    }

    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut wire = self.0.lock().unwrap();
        wire.to_server.extend_from_slice(buf);
        wire.client_sent += buf.len();
        Ok(buf.len())
    }

    fn poll_read_ready(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.0.lock().unwrap().to_client.is_empty() {
            // `drive` runs the server and polls again; no waker is needed.
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_write_ready(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Hands the server what the client sent and puts the server's answer on
/// the wire. Returns whether the server had anything to send.
fn step(server: &mut ServerConnection, wire: &Mutex<Wire>) -> bool {
    let mut wire = wire.lock().unwrap();
    let inbox = std::mem::take(&mut wire.to_server);
    let mut inbox = inbox.as_slice();
    while !inbox.is_empty() {
        server.read_tls(&mut inbox).unwrap();
        server.process_new_packets().unwrap();
    }
    let mut sent = Vec::new();
    while server.wants_write() {
        server.write_tls(&mut sent).unwrap();
    }
    wire.to_client.extend(&sent);
    !sent.is_empty()
}

/// Polls the client's `future` to completion, running the server whenever
/// the client waits on it.
fn drive<F: Future>(future: F, server: &mut ServerConnection, wire: &Mutex<Wire>) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        assert!(
            step(server, wire),
            "the handshake stalled: the client waits and the server has nothing to send"
        );
    }
}

/// The two modes that verify the certificate, as sqlx-postgres 0.8.6
/// (`connection/tls.rs`) configures sqlx-core's TLS for them.
#[derive(Clone, Copy, Debug)]
enum SslMode {
    VerifyCa,
    VerifyFull,
}

/// Connects to a server presenting `chain`, with `hostname` as the URL's
/// host and `ca` as `sslrootcert`. Returns the outcome and how many bytes
/// the client sent.
fn connect(
    mode: SslMode,
    hostname: &str,
    ca: Option<&CertificateInput>,
    chain: Vec<CertificateDer<'static>>,
) -> (Result<(), sqlx_core::Error>, usize) {
    let config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(chain, key(SERVER_SEED))
            .unwrap();
    let mut server = ServerConnection::new(Arc::new(config)).unwrap();
    let wire = Arc::new(Mutex::new(Wire::default()));
    let config = TlsConfig {
        accept_invalid_certs: false,
        accept_invalid_hostnames: matches!(mode, SslMode::VerifyCa),
        hostname,
        root_cert_path: ca,
        client_cert_path: None,
        client_key_path: None,
    };
    let handshake = tls::handshake(ClientSocket(wire.clone()), config, SocketIntoBox);
    let result = drive(handshake, &mut server, &wire).map(drop);
    let client_sent = wire.lock().unwrap().client_sent;
    (result, client_sent)
}

/// `connect` with `ca` as `sslrootcert`, for the outcome alone.
fn handshake(
    mode: SslMode,
    hostname: &str,
    ca: &CertificateInput,
    chain: Vec<CertificateDer<'static>>,
) -> Result<(), sqlx_core::Error> {
    connect(mode, hostname, Some(ca), chain).0
}

/// The configuration error that refused to connect, before any TLS.
fn configuration_error(result: Result<(), sqlx_core::Error>, sent: usize) -> String {
    let error = result.expect_err("verify-ca connected");
    let sqlx_core::Error::Configuration(message) = &error else {
        panic!("verify-ca failed with something other than a configuration error: {error}");
    };
    assert_eq!(sent, 0, "the client started the TLS handshake");
    message.to_string()
}

/// The TLS error that refused the server's certificate.
fn refusal(error: sqlx_core::Error) -> rustls::Error {
    let sqlx_core::Error::Io(error) = error else {
        panic!("the handshake failed outside TLS: {error}");
    };
    error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<rustls::Error>())
        .unwrap_or_else(|| panic!("the handshake failed outside TLS: {error}"))
        .clone()
}

const CA_SEED: u8 = 1;
const UNTRUSTED_CA_SEED: u8 = 2;
const SERVER_SEED: u8 = 3;

/// PKCS#8 v1 for the Ed25519 key with this seed: a test key that guards
/// nothing.
fn key(seed: u8) -> PrivateKeyDer<'static> {
    let pkcs8 = [
        &[
            0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70,
        ][..],
        &[0x04, 0x22, 0x04, 0x20],
        &[seed; 32],
    ]
    .concat();
    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pkcs8))
}

fn signing_key(seed: u8) -> Arc<dyn SigningKey> {
    rustls::crypto::ring::default_provider()
        .key_provider
        .load_private_key(key(seed))
        .unwrap()
}

fn der(tag: u8, parts: &[&[u8]]) -> Vec<u8> {
    let len: usize = parts.iter().map(|part| part.len()).sum();
    let [high, low] = u16::try_from(len).unwrap().to_be_bytes();
    let mut out = vec![tag];
    match (high, low) {
        (0, short) if short < 0x80 => out.push(short),
        (0, low) => out.extend([0x81, low]),
        (high, low) => out.extend([0x82, high, low]),
    }
    for part in parts {
        out.extend_from_slice(part);
    }
    out
}

/// The Ed25519 AlgorithmIdentifier, for keys and signatures alike.
fn ed25519() -> Vec<u8> {
    der(0x30, &[&der(0x06, &[&[0x2b, 0x65, 0x70]])])
}

/// A distinguished name holding one common name.
fn name(common_name: &str) -> Vec<u8> {
    let attribute = der(0x06, &[&[0x55, 0x04, 0x03]]);
    let value = der(0x0c, &[common_name.as_bytes()]);
    der(0x30, &[&der(0x31, &[&der(0x30, &[&attribute, &value])])])
}

/// ISRG Root X1's subject, byte for byte as webpki-roots holds it: C, O and
/// CN as PrintableStrings, each in its own RDN. sqlx-core loads that root
/// among Mozilla's public roots.
fn isrg_root_x1() -> Vec<u8> {
    let rdn = |attribute: u8, value: &str| {
        let attribute = der(0x06, &[&[0x55, 0x04, attribute]]);
        der(
            0x31,
            &[&der(0x30, &[&attribute, &der(0x13, &[value.as_bytes()])])],
        )
    };
    der(
        0x30,
        &[
            &rdn(0x06, "US"),
            &rdn(0x0a, "Internet Security Research Group"),
            &rdn(0x03, "ISRG Root X1"),
        ],
    )
}

/// A v3 certificate for `subject`'s key, issued by the distinguished name
/// `issuer` and signed by `issuer_seed`'s key. The validity runs from 2000 to
/// 9999-12-31, RFC 5280's "no expiry".
fn certificate(
    serial: u8,
    (issuer, issuer_seed): (&[u8], u8),
    (subject, subject_seed): (&str, u8),
    extension: &[u8],
) -> CertificateDer<'static> {
    let validity = der(
        0x30,
        &[
            &der(0x17, &[b"000101000000Z"]),
            &der(0x18, &[b"99991231235959Z"]),
        ],
    );
    let subject_key = signing_key(subject_seed);
    let spki = subject_key.public_key().unwrap();
    let tbs = der(
        0x30,
        &[
            &der(0xa0, &[&der(0x02, &[&[2]])]),
            &der(0x02, &[&[serial]]),
            &ed25519(),
            issuer,
            &validity,
            &name(subject),
            spki.as_ref(),
            &der(0xa3, &[&der(0x30, &[extension])]),
        ],
    );
    let signature = signing_key(issuer_seed)
        .choose_scheme(&[SignatureScheme::ED25519])
        .unwrap()
        .sign(&tbs)
        .unwrap();
    CertificateDer::from(der(
        0x30,
        &[&tbs, &ed25519(), &der(0x03, &[&[0], &signature])],
    ))
}

/// A self-signed CA certificate: critical basicConstraints with cA set.
fn ca_certificate(common_name: &str, seed: u8) -> CertificateDer<'static> {
    let basic_constraints = der(
        0x30,
        &[
            &der(0x06, &[&[0x55, 0x1d, 0x13]]),
            &der(0x01, &[&[0xff]]),
            &der(0x04, &[&der(0x30, &[&der(0x01, &[&[0xff]])])]),
        ],
    );
    certificate(
        1,
        (&name(common_name), seed),
        (common_name, seed),
        &basic_constraints,
    )
}

/// The server certificate `issuer` signs: subjectAltName holds one DNS name,
/// SERVER_NAME, and no IP address.
fn server_certificate(issuer: (&[u8], u8)) -> CertificateDer<'static> {
    let subject_alt_name = der(
        0x30,
        &[
            &der(0x06, &[&[0x55, 0x1d, 0x11]]),
            &der(
                0x04,
                &[&der(0x30, &[&der(0x82, &[SERVER_NAME.as_bytes()])])],
            ),
        ],
    );
    certificate(2, issuer, (SERVER_NAME, SERVER_SEED), &subject_alt_name)
}

/// `cert` as the inline PEM that `sslrootcert` can carry.
fn pem(cert: &CertificateDer<'_>) -> CertificateInput {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut base64 = Vec::new();
    for chunk in cert.as_ref().chunks(3) {
        let bits = chunk.iter().enumerate().fold(0u32, |bits, (i, &byte)| {
            bits | u32::from(byte) << (16 - 8 * i)
        });
        for i in 0..4 {
            base64.push(if i <= chunk.len() {
                ALPHABET[(bits >> (18 - 6 * i) & 0x3f) as usize]
            } else {
                b'='
            });
        }
    }
    let mut pem = b"-----BEGIN CERTIFICATE-----\n".to_vec();
    for line in base64.chunks(64) {
        pem.extend_from_slice(line);
        pem.push(b'\n');
    }
    pem.extend_from_slice(b"-----END CERTIFICATE-----\n");
    CertificateInput::Inline(pem)
}

fn trusted_ca() -> CertificateInput {
    pem(&ca_certificate("trusted CA", CA_SEED))
}

fn trusted_server() -> Vec<CertificateDer<'static>> {
    vec![server_certificate((&name("trusted CA"), CA_SEED))]
}

/// The delay proxy's case: the database's CA-signed certificate, reached as
/// 127.0.0.1.
#[test]
fn verify_ca_accepts_a_trusted_certificate_for_another_name() {
    if let Err(error) = handshake(
        SslMode::VerifyCa,
        PROXY_HOST,
        &trusted_ca(),
        trusted_server(),
    ) {
        panic!("verify-ca refused a certificate the trusted CA signed: {error}");
    }
}

/// verify-full still checks the name, and refuses for the name alone.
#[test]
fn verify_full_refuses_a_trusted_certificate_for_another_name() {
    let error = handshake(
        SslMode::VerifyFull,
        PROXY_HOST,
        &trusted_ca(),
        trusted_server(),
    )
    .expect_err("verify-full accepted a certificate for another name");

    let refusal = refusal(error);
    let rustls::Error::InvalidCertificate(CertificateError::NotValidForNameContext {
        expected,
        ..
    }) = &refusal
    else {
        panic!("verify-full refused the certificate for something else: {refusal:?}");
    };
    assert_eq!(*expected, ServerName::try_from(PROXY_HOST).unwrap());
}

/// The control: the certificate and its chain are valid under both modes when
/// the name matches, so the refusal above is for the name.
#[test]
fn both_modes_accept_a_trusted_certificate_for_its_own_name() {
    for mode in [SslMode::VerifyCa, SslMode::VerifyFull] {
        if let Err(error) = handshake(mode, SERVER_NAME, &trusted_ca(), trusted_server()) {
            panic!("{mode:?} refused a valid certificate for its own name: {error}");
        }
    }
}

/// verify-ca still checks the chain: a certificate from an issuer the client
/// does not trust is refused as such, though its name does not match either.
#[test]
fn verify_ca_refuses_a_certificate_from_an_untrusted_issuer() {
    let untrusted = vec![server_certificate((
        &name("untrusted CA"),
        UNTRUSTED_CA_SEED,
    ))];

    let error = handshake(SslMode::VerifyCa, PROXY_HOST, &trusted_ca(), untrusted)
        .expect_err("verify-ca accepted a certificate from an untrusted issuer");

    let refusal = refusal(error);
    assert!(
        matches!(
            refusal,
            rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer)
        ),
        "verify-ca refused the certificate for something else: {refusal:?}"
    );
}

/// verify-ca trusts only `sslrootcert`, so without one it has nothing to
/// check the chain against. As libpq does, it refuses to connect, naming
/// `sslrootcert`, and before the TLS handshake starts. sqlx-core 0.8.6 fell
/// back to the public roots instead.
#[test]
fn verify_ca_without_sslrootcert_is_refused_before_the_handshake() {
    let (result, sent) = connect(SslMode::VerifyCa, SERVER_NAME, None, trusted_server());

    let message = configuration_error(result, sent);
    assert!(
        message.contains("sslrootcert"),
        "the error does not name sslrootcert: {message}"
    );
}

/// An `sslrootcert` that holds no certificate leaves verify-ca nothing to
/// trust either, and is refused the same way.
#[test]
fn verify_ca_with_an_sslrootcert_holding_no_certificate_is_refused() {
    let empty = CertificateInput::Inline(b"no certificate here\n".to_vec());

    let (result, sent) = connect(
        SslMode::VerifyCa,
        SERVER_NAME,
        Some(&empty),
        trusted_server(),
    );

    let message = configuration_error(result, sent);
    assert!(
        message.contains("sslrootcert") && message.contains("no certificate"),
        "the error does not say sslrootcert holds no certificate: {message}"
    );
}

/// verify-ca's trust anchors are `sslrootcert`'s certificates and nothing
/// else. No public CA's key is at hand to show that a certificate it signs
/// is refused, so the server presents one that names ISRG Root X1, a public
/// root sqlx-core loads, as its issuer, signed with the test's own Ed25519
/// key. verify-full, which keeps the public roots, finds that root as the
/// issuer and refuses the signature, as the root's RSA key can't verify an
/// Ed25519 one; that control shows the name matches a loaded root. It checks
/// only that the issuer was not unknown, since which signature error rustls
/// reports depends on how webpki ranks its errors. verify-ca has no such
/// anchor, so the issuer is unknown.
#[test]
fn verify_ca_does_not_trust_the_public_roots() {
    let claims_public_root = || vec![server_certificate((&isrg_root_x1(), SERVER_SEED))];

    let error = handshake(
        SslMode::VerifyFull,
        SERVER_NAME,
        &trusted_ca(),
        claims_public_root(),
    )
    .expect_err("verify-full accepted a certificate signed by the wrong key");
    let refusal_under_full = refusal(error);
    assert!(
        !matches!(
            refusal_under_full,
            rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer)
        ),
        "verify-full did not take ISRG Root X1 as the issuer, so this test no longer \
         shows verify-ca ignoring a loaded public root: has webpki-roots dropped ISRG \
         Root X1, or changed how it encodes its subject? {refusal_under_full:?}"
    );

    let error = handshake(
        SslMode::VerifyCa,
        SERVER_NAME,
        &trusted_ca(),
        claims_public_root(),
    )
    .expect_err("verify-ca accepted a certificate signed by the wrong key");
    let refusal_under_ca = refusal(error);
    assert!(
        matches!(
            refusal_under_ca,
            rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer)
        ),
        "verify-ca took a public root as the issuer: {refusal_under_ca:?}"
    );
}
