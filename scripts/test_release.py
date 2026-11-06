import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("release_check", Path(__file__).with_name("check-release.py"))
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)

class ReleaseChecks(unittest.TestCase):
    def prepare(self, root):
        (root / "src").mkdir()
        (root / "src/targets.rs").write_text("// local configuration\n", encoding="utf-8")
        (root / "config").mkdir()
        (root / "config/targets.example.json").write_text("[]", encoding="utf-8")
        (root / "README.md").write_text("fixture", encoding="utf-8")
        (root / "LICENSE").write_text("fixture", encoding="utf-8")
        (root / "trading-config.example.json").write_text(json.dumps({"allowLive":False,"walletKeyFile":None,"rpcUrl":None,"simulationFrom":None}), encoding="utf-8")

    def test_clean_export_and_git_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); self.prepare(root)
            (root / ".git").mkdir(); (root / ".git/config").write_text("metadata", encoding="utf-8")
            self.assertFalse(checker.scan(root)[1])

    def test_detects_runtime_and_credential_without_returning_value(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); self.prepare(root)
            secret="x" * 32
            (root / "src/test.rs").write_text("https://rpc.example.invalid/v1/"+secret, encoding="utf-8")
            (root / "config/targets.local.json").write_text("[]", encoding="utf-8")
            issues=checker.scan(root)[1]
            self.assertTrue(any(i[2]=="provider-key-in-url" for i in issues))
            self.assertTrue(any(i[2]=="runtime-or-sensitive-file" for i in issues))
            self.assertNotIn(secret,repr(issues))

    def test_unsafe_example_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); self.prepare(root)
            (root / "trading-config.example.json").write_text('{"allowLive":true}',encoding="utf-8")
            self.assertTrue(any(i[2]=="unsafe-example-settings" for i in checker.scan(root)[1]))

if __name__ == "__main__":
    unittest.main()
