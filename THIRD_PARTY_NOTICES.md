# Third-party components and provenance

## Project sources

This is a local open-source preparation copy of an existing project, not an upstream copyright audit. The owner must confirm the provenance and redistribution rights of the original base archive before making a public release. The MIT text in LICENSE is prepared for rights the owner and contributors can lawfully grant; it does not relicense unknown upstream work. The provisional public copyright name is `Meme Quant contributors`; the owner should confirm it before release.

## Rust dependencies

Dependencies and exact resolved versions are recorded in Cargo.toml/Cargo.lock. They are fetched from their upstream registries by Cargo, not vendored in this source export. They retain their own licenses; the project's MIT license is not a replacement for those licenses. The local dependency inventory is in `docs/DEPENDENCIES.md`.

Before distributing a compiled binary, review the dependency license texts and any native libraries actually linked by that build (including SQLite, TLS libraries, and Windows runtime components) and ship required notices. The source inventory is not a legal opinion or proof of binary redistribution compliance.

## Browser libraries and services

The unused standalone TradingView Lightweight Charts 4.2.0 bundle has been intentionally omitted. The candidate dashboard does not load it; the old unused server route has been removed. This export does not bundle GMGN or TradingView website source, logos or screenshots.

GMGN is opened as an external website. GMGN, Launchpad names, contract addresses and public RPC endpoints are references to third-party services, not ownership claims, endorsements or grants of access. Review applicable platform terms and current contract/RPC behavior independently.

## Tests

Public chain samples and explicitly synthetic addresses, hashes and dummy signing keys may appear in offline tests. They are not author wallet credentials or a production monitoring list. Never fund a public dummy account.
