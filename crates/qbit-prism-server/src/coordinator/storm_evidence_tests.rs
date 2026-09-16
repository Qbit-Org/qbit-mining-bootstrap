//! What counts as evidence of a node offer, asserted distinctly (#270).
//!
//! #266's migration 011 made this writable for the first time. The unfinished
//! states are `pending`, `offer_reserved`, `offered` and `reconciliation`;
//! `OfferReserved` is the durable reservation taken *before* the one
//! `submitblock` call, so a claim that finds a row there never offers. Only
//! `offered` — with `offered_at_ms`, `offer_outcome` and `offer_reply` — is
//! evidence that the node was called. A retry, a held lease, a
//! capacity-closed terminalization and a replay adoption are each not
//! evidence on their own, and 2.x.x asserted all four separately because its
//! selector's safety comparison was published against exactly that split.
//!
//! Every case here reaches its state through the real offer path. A test that
//! sets `state='offered'` by hand asserts its own setup and proves nothing,
//! so the offer record is produced by the coordinator, observed at the fake
//! node, and only then read back.
//!
//! **Held pending #415.** That issue may add a terminal disposition for a
//! proven orphan, which changes what a `Reconciliation` row can become. Today
//! the documented rule is that such a row is retried with read-only chain
//! observations only and never abandoned. Until #415's disposition is decided,
//! assertions written here would be written against a rule that is about to
//! change, so this module stays empty by decision rather than by omission.
