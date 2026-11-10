"""Conservative source-export checks. Prints locations/rule names, never secret values.

Scans working files, not Git history, encrypted data, binaries or remote services.
This is not a replacement for manual review or a dedicated secret scanner.
"""
from pathlib import Path
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
ALLOWED_DIRS = {"src", "web", "browser-extension", "config", "scripts", "docs", ".github"}
ALLOWED_ROOT = {
    "Cargo.toml", "Cargo.lock", "README.md", "LICENSE", "SECURITY.md", "CONTRIBUTING.md",
    "THIRD_PARTY_NOTICES.md", "PUBLISHING.md", "RELEASE-REVIEW.md", "trading-config.example.json",
    "TRADING.md", "start-backend.ps1", "start-system.ps1", "initialize-runtime.ps1", ".gitignore", ".gitattributes",
}
RULES = [
    ("provider-key-in-url", re.compile(r"https?://[^\s\"'<>]*(?:/v1/|api[_-]?key=)[A-Za-z0-9_-]{16,}", re.I)),
    ("websocket-provider-key", re.compile(r"wss?://[^\s\"'<>]*/v1/[A-Za-z0-9_-]{16,}", re.I)),
    ("github-access-token", re.compile(r"(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})")),
    ("private-key-literal", re.compile(r"(?:private_?key|secret_?key|mnemonic|seed_?phrase)\s*[:=]\s*[\"'](?:0x)?[a-f0-9]{64}[\"']", re.I)),
    ("private-key-block", re.compile(r"-----BEGIN (?:[A-Z]+ )?PRIVATE KEY-----")),
    ("embedded-user-home", re.compile(r"[A-Za-z]:[\\/]+Users[\\/]+[^\\/\s\"']+", re.I)),
    ("credential-in-url", re.compile(r"https?://[^\s/:\"']+:[^\s/@\"']+@", re.I)),
]

def valid_published_watchlist(targets):
    if not isinstance(targets, list) or not targets or len(targets) > 10_000:
        return False
    normalized = []
    for address in targets:
        if not isinstance(address, str):
            return False
        address = address.strip().lower()
        if not re.fullmatch(r"0x[0-9a-f]{40}", address) or int(address[2:], 16) == 0:
            return False
        normalized.append(address)
    return len(set(normalized)) == len(normalized)


def scan(root: Path):
    issues = []
    files = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if ".git" in relative.parts or "__pycache__" in relative.parts:
            continue
        if path.is_symlink():
            issues.append((relative.as_posix(), 0, "symlink-not-allowed"))
            continue
        if not path.is_file():
            continue
        rel = relative.as_posix()
        if (len(relative.parts) == 1 and rel not in ALLOWED_ROOT) or (len(relative.parts) > 1 and relative.parts[0] not in ALLOWED_DIRS):
            issues.append((rel, 0, "outside-release-allowlist"))
            continue
        if any(part.endswith(".local.json") or ".local." in part for part in relative.parts) or path.suffix.lower() in {".exe", ".pdb", ".db", ".sqlite", ".sqlite3", ".log", ".encrypted", ".key", ".pem", ".zip"}:
            issues.append((rel, 0, "runtime-or-sensitive-file"))
            continue
        if path.stat().st_size > 2_000_000:
            issues.append((rel, 0, "unexpected-large-file"))
            continue
        try:
            text = path.read_text(encoding="utf-8-sig")
        except (UnicodeError, OSError):
            issues.append((rel, 0, "non-text-or-unreadable"))
            continue
        files.append(rel)
        for number, line in enumerate(text.splitlines(), 1):
            for name, pattern in RULES:
                if pattern.search(line):
                    issues.append((rel, number, name))
    for required in ("src/targets.rs", "config/targets.example.json", "LICENSE", "README.md", "trading-config.example.json"):
        if not (root / required).is_file():
            issues.append((required, 0, "required-file-missing"))
    try:
        example = json.loads((root / "trading-config.example.json").read_text(encoding="utf-8-sig"))
        if any(example.get(k) is not None for k in ("walletKeyFile", "rpcUrl", "simulationFrom")) or example.get("allowLive") is not False:
            issues.append(("trading-config.example.json", 0, "unsafe-example-settings"))
        targets = json.loads((root / "config/targets.example.json").read_text(encoding="utf-8-sig"))
        if not valid_published_watchlist(targets):
            issues.append(("config/targets.example.json", 0, "invalid-published-watchlist"))
    except (OSError, ValueError):
        issues.append(("configuration-examples", 0, "invalid-example"))
    return files, issues

def main():
    files, issues = scan(ROOT)
    for file, line, rule in issues:
        print(f"FAIL {file}:{line} [{rule}]")
    if issues:
        print(f"Release check failed: {len(issues)} issue(s). Values intentionally hidden.")
        return 1
    print(f"PASS: {len(files)} source files checked; no rule matches or runtime files. Manual provenance/history/secret review still required.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
