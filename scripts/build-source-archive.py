"""Create a source-only archive after export validation; never include runtime data."""
import argparse
import hashlib
import importlib.util
from pathlib import Path
import sys
import zipfile

sys.dont_write_bytecode = True
root = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("release_check", root / "scripts/check-release.py")
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path, help="New .zip outside the source directory; existing files are never overwritten")
    args = parser.parse_args()
    output = args.output.resolve()
    if output == root or root in output.parents or output.suffix.lower() != ".zip":
        parser.error("output must be a new .zip outside the source directory")
    files, issues = checker.scan(root)
    if issues:
        for file, line, rule in issues:
            print(f"FAIL {file}:{line} [{rule}]")
        return 1
    with zipfile.ZipFile(output, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative in files:
            archive.write(root / relative, "meme-quant/" + relative)
    with zipfile.ZipFile(output) as archive:
        if archive.testzip() is not None or len(archive.namelist()) != len(files):
            raise RuntimeError("archive verification failed")
    print(f"Source-only archive: {len(files)} files; SHA256 {hashlib.sha256(output.read_bytes()).hexdigest()}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
