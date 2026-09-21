use super::*;
use crate::coordinator::miner_tests::Fixture;

#[tokio::test]
async fn overlapped_body_preserves_exact_serial_audit_bytes() {
    for ctv in [false, true] {
        let f = Fixture::build(
            Duration::from_secs(10),
            |config| config.ctv_enabled = ctv,
            None,
        )
        .await;
        let template = f
            .coordinator
            .prepared
            .read()
            .await
            .as_ref()
            .unwrap()
            .template
            .clone();
        let inputs = BundleInputs::capture(&f.coordinator.config, None).unwrap();
        let suffix = "00".repeat(12);
        let permit = Arc::new(
            f.coordinator
                .build_slots
                .clone()
                .acquire_owned()
                .await
                .unwrap(),
        );
        let captured = f
            .coordinator
            .capture_refresh_window(
                1_000_000,
                permit,
                None,
                template.clone(),
                suffix.clone(),
                inputs.clone(),
            )
            .await
            .unwrap();
        let config = f.coordinator.config.clone();
        captured
            .spawn_blocking(move |captured| {
                let (window, body, _admission) = captured;
                let body = body.unwrap();
                let parallel = body.body.as_ref().unwrap();
                let serial = bundle_build::build_body(
                    &config,
                    &window.snapshot,
                    &template,
                    None,
                    suffix,
                    inputs,
                )
                .unwrap()
                .0;
                let parallel_bytes = qbit_prism::canonical_audit_bundle_bytes_from_parts(
                    parallel,
                    &window.snapshot.shares,
                )
                .unwrap();
                let serial_bytes =
                    serde_json::to_vec(&serial.into_bundle(window.snapshot.shares.clone()))
                        .unwrap();
                assert_eq!(parallel_bytes, serial_bytes);
                assert_eq!(
                    body.hashes.unwrap().audit_bundle_sha256,
                    hex::encode(Sha256::digest(serial_bytes))
                );
                assert_eq!(
                    window.reference,
                    WindowRef::from_snapshot(&window.snapshot).unwrap()
                );
            })
            .await
            .unwrap();
    }
}
