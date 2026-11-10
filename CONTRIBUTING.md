# Contributing

Use the default non-broadcasting build for development. Do not run tests with funded accounts or production RPC credentials. Use offline fixtures and temporary SQLite databases.

Before proposing changes:

1. Run `cargo fmt --check`, `cargo test --locked` and `cargo test --locked --features live-trading`.
2. Run `node scripts/test-js.cjs`, `python scripts/check-release.py` and the Python unit tests listed in README.
3. Keep terminal states irreversible; test Unix-aligned closed candles, skipped no-trade gaps, fresh C candles, once-only reentry, atomic Slot reservations and shutdown cancellation.
4. Never add private watchlists, local configuration, browser data, credentials, live order logs, or compiled wallet tools. The owner-authorized shared watchlist belongs only in `config/targets.example.json`; publishing additional personal lists requires their owner's permission.
5. Document third-party source provenance and preserve its license. Submit only code you have the right to contribute under the project's license.

Publish security-sensitive findings privately as described in SECURITY.md. A passing offline test suite is not evidence of successful or safe mainnet trading.
