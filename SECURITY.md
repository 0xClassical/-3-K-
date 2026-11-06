# Security policy

This repository contains experimental monitoring and optional transaction code. No independent security audit or guarantee is implied.

Do not put private keys, seed phrases, wallet passwords, RPC credentials, control tokens, encrypted key files, browser profiles, databases, or real order logs in an Issue, pull request, screenshot, CI secret, or uploaded artifact.

Before publishing the repository, the owner should enable GitHub Private Vulnerability Reporting where available. Report exploitable wallet/signing/authentication issues through that private channel, not a public Issue. No security contact address is invented in this preparation copy. If no private channel is configured, ask the maintainer to establish one without posting exploit details or secrets.

For accidental exposure, revoke/rotate affected credentials first. Deleting the current file or adding it to .gitignore does not remove Git history, clones or downloaded artifacts. Move assets using a compromised private key to a newly created account under your own control; never share the old or new key with a maintainer.

LAN read endpoints expose operational information. Default binding is loopback. Non-loopback binding requires an explicit control token, but HTTP and read endpoints remain unencrypted/unauthenticated. Do not expose port 8787 to the Internet. Browser-extension control is local only and is not a substitute for OS account security.
