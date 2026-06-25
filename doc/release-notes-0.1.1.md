# qbit-mining-bootstrap 0.1.1 Release Notes

Release date: 2026-06-25

## Highlights

- Fixed AuxPoW commitment byte ordering so the coordinator and helper payloads follow the `createauxblock` commitment order.
- Updated the AuxPoW mining and merge-mining protocol docs to describe the corrected ordering.
- Added regression coverage for AuxPoW payload construction and coordinator commitment handling.
- Added the Blacksmith CI workflow for branch validation.

## Operator Notes

- AuxPoW and merged-mining operators should prefer this release over 0.1.0 before live bridge trials.
- No configuration migration is expected from 0.1.0.
- This branch is based on `origin/0.1.x` through commit `0bb8fdb`.
