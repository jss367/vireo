"""Reconciled cache changes, exercised only via selected AST and invented I/O.

Run directly; no app/conftest imports or actual cache/model/pipeline operations.
The harness reads source only and uses the descriptor test's in-memory nodes.
"""

import ast
import builtins
import contextlib
import functools
import logging
import posixpath
import runpy
import unittest
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace as S

VIREO = Path(__file__).resolve().parents[1]
HARNESS = runpy.run_path(str(Path(__file__).with_name("test_hf_cache_descriptor_safety.py")))
service, payload = HARNESS["service"], HARNESS["payload"]
ROOT, REPO, KEY = (HARNESS[key] for key in ("ROOT", "REPO", "KEY"))


def selected(path, names, namespace, *, keep_decorators=False):
    nodes = [n for n in ast.walk(ast.parse((VIREO / path).read_text()))
             if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {n.name for n in nodes} == set(names)
    if not keep_decorators:
        for node in nodes:
            node.decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "<invented-recovery>", "exec"), namespace)


def forbidden(*_args, **_kwargs):
    raise AssertionError("Unexpected system/product operation")


class ReconciledSafetyTests(unittest.TestCase):
    def test_repository_allowlist_includes_only_manifest_registered_and_exact_legacy_ids(self):
        fake_models = S(ONNX_REPO="invented/onnx", KNOWN_MODELS=[{"model_str": "hf-hub:manifest/one"}],
                        get_models=lambda: [{"source": "hf-hub:registered/two"}])
        ns = {"__builtins__": {**vars(builtins), "__import__": lambda *_a, **_k: fake_models}}
        selected(Path("services/storage_paths.py"), {"huggingface_repositories"}, ns)
        self.assertEqual(ns["huggingface_repositories"](), {
            "models--invented--onnx", "models--manifest--one", "models--registered--two",
            "models--imageomics--bioclip", "models--imageomics--bioclip-2", "models--imageomics--bioclip-2.5",
        })

    def test_explicit_scope_retains_other_repositories_in_the_same_organization(self):
        fs, ns = service()
        payload(fs)
        outsiders = []
        for org in ("imageomics", "bioclip", "jss367"):
            repo = ROOT + "/models--" + org + "--other-client"
            outsiders.append(fs.add(repo + "/blobs/foreign"))
            fs.add(repo + "/snapshots/rev/file", "link", target="../../blobs/foreign")
        self.assertEqual(ns["remove_unused"](ROOT, "/invented/models")["freed"], 10)
        self.assertTrue(set(outsiders).isdisjoint(fs.unlinked))
        self.assertFalse(fs.fds)

    def test_dynamic_purge_adds_only_the_captured_repository_and_rechecks_installs(self):
        for installed in (False, True):
            with self.subTest(installed=installed):
                repo = "external/dynamic"
                install = "/invented/models/dynamic"
                fs, ns = service(model_entries=[{"id": "dynamic", "model_str": "hf-hub:" + repo,
                                                  "weights_path": install}] if installed else [])
                target = ROOT + "/models--external--dynamic"
                inode = fs.add(target + "/blobs/dynamic", size=23)
                fs.add(target + "/snapshots/rev/file", "link", target="../../blobs/dynamic")
                unrelated = fs.add(ROOT + "/models--external--other/blobs/foreign")
                if installed:
                    fs.add(install, "dir")
                self.assertEqual(ns["purge_model"]("dynamic", ROOT, "/invented/models"), 0)
                freed = ns["purge_model"]("dynamic", ROOT, "/invented/models", hf_source="hf-hub:" + repo)
                self.assertEqual(freed, 0 if installed else 23)
                self.assertEqual(inode in fs.unlinked, not installed)
                self.assertNotIn(unrelated, fs.unlinked)
                self.assertFalse(fs.fds)

    def test_captured_repo_is_only_forwarded_from_managed_registration_before_removal(self):
        for managed in (False, True):
            for field in ("model_str", "source"):
                with self.subTest(managed=managed, field=field):
                    calls = []
                    entry = {"id": "dynamic", "weights_path": "/invented/weights", field: "hf-hub:external/dynamic"}
                    config = {"models": [entry], "active_model": "dynamic"}
                    def purge(model_id, *, hf_source, _calls=calls, _config=config):
                        self.assertEqual(_config["models"], [])
                        _calls.append((model_id, hf_source))
                        return {}
                    ns = {"__builtins__": {**vars(builtins), "__import__": forbidden},
                          "_CONFIG_LOCK": contextlib.nullcontext(), "_load_config": lambda _c=config: _c,
                          "_save_config": lambda _c: None, "KNOWN_MODELS": [],
                          "_model_is_managed": lambda _m, _managed=managed: _managed,
                          "os": S(path=S(lexists=lambda _p: False)), "log": logging.getLogger("invented"),
                          "_purge_hf_cache_for_model": purge}
                    selected(Path("models.py"), {"remove_model"}, ns)
                    self.assertEqual(ns["remove_model"]("dynamic"), {"files_deleted": False, "kept_path": None})
                    self.assertEqual(calls, [("dynamic", "hf-hub:external/dynamic" if managed else "")])

    def test_installed_weight_fast_paths_and_guarded_rechecks_do_no_download_io(self):
        cases = [
            ("detector.py", "ensure_megadetector_weights", "_hf_download_megadetector_weights", (), "/invented/md/model.onnx"),
            ("dino_embed.py", "ensure_dinov2_weights", "_hf_download_dinov2_weights", ("vit-b14",), "/invented/dino/model.onnx"),
            ("masking.py", "ensure_sam2_weights", "_hf_download_sam2_weights", ("sam2-small",), "/invented/sam"),
            ("keypoints.py", "ensure_keypoint_weights", "_hf_download_keypoint_weights", ("animal",), "/invented/kp/model.onnx"),
        ]
        for file, public, helper, args, expected in cases:
            with self.subTest(file=file):
                present = [True]
                enters = []
                @contextlib.contextmanager
                def guard(_enters=enters, _present=present):
                    _enters.append(True)
                    _present[0] = True  # Another writer finished before our acquisition.
                    yield
                ns = {"__builtins__": {**vars(builtins), "__import__": forbidden},
                      "functools": functools, "Callable": Callable, "DOWNLOAD_LOCK": guard(),
                      "os": S(path=S(isfile=lambda _p, _present=present: _present[0], join=posixpath.join)),
                      "MEGADETECTOR_ONNX_PATH": "/invented/md/model.onnx",
                      "DINOV2_VARIANTS": {"vit-b14": {}}, "SAM2_VARIANTS": {"sam2-small": {}},
                      "_dinov2_model_path": lambda _v: ("/invented/dino", "/invented/dino/model.onnx"),
                      "_sam2_model_dir": lambda _v: "/invented/sam", "_model_dir": lambda _v: "/invented/kp"}
                selected(Path("services/hf_cache_lock.py"), {"cache_access"}, ns)
                selected(Path(file), {public, helper}, ns, keep_decorators=True)
                self.assertEqual(ns[public](*args), expected)
                self.assertFalse(enters)
                present[0] = False
                self.assertEqual(ns[public](*args), expected)
                self.assertEqual(enters, [True])

    def test_canonical_alias_cannot_redirect_cleanup_after_target_is_pinned(self):
        for changed in ("/invented", ROOT, REPO):
            with self.subTest(changed=changed):
                fs, ns = service()
                payload(fs)
                fs.add("/alias", "link", target="/invented")
                original = ns["_collect"]
                swapped = [False]
                def collect(*args, _original=original, _fs=fs, _changed=changed, _swapped=swapped):
                    result = _original(*args)
                    if not _swapped[0]:
                        _fs.add(_changed, "dir")
                        _swapped[0] = True
                    return result
                ns["_collect"] = collect
                with self.assertRaises(ns["UnsafeCacheCleanup"]):
                    ns["remove"]([KEY], "/alias/hub", "/invented/models")
                self.assertFalse(fs.unlinked)
                self.assertFalse(fs.fds)

    def test_pipeline_conflict_and_guard_failure_do_not_touch_install_or_sessions(self):
        for outcome, status in ((False, 409), (OSError("invented lock failure"), 503)):
            with self.subTest(status=status):
                attempts = []
                def acquire(*, blocking, _attempts=attempts, _outcome=outcome):
                    _attempts.append(blocking)
                    if isinstance(_outcome, Exception):
                        raise _outcome
                    return _outcome
                modules = {"shutil": S(rmtree=forbidden), "services.hf_cache_lock": S(
                    DOWNLOAD_LOCK=S(acquire=acquire, release=forbidden))}
                ns = {"__builtins__": {**vars(builtins), "__import__": lambda name, *_a, _m=modules, **_k: _m[name]},
                      "request": S(get_json=lambda **_k: {"model_id": "sam2-tiny"}),
                      "PIPELINE_MODELS": {"sam2-tiny": {"subfolder": "group"}},
                      "json_error": lambda message, status=400: (message, status),
                      "os": S(path=S(join=forbidden, isdir=forbidden)), "pipeline_models_dir": forbidden}
                selected(Path("web/models.py"), {"api_models_pipeline_delete"}, ns)
                message, actual = ns["api_models_pipeline_delete"]()
                self.assertEqual(actual, status)
                self.assertEqual(attempts, [False])
                self.assertIn("model/cache" if status == 503 else "model or cache operation", message)
                self.assertNotIn("download", message.lower())

    def test_pipeline_guard_covers_fake_install_removal_purge_and_session_reset(self):
        events = []
        active = [False]
        def acquire(*, blocking):
            self.assertFalse(blocking)
            active[0] = True
            events.append("acquire")
            return True
        def release():
            self.assertTrue(active[0])
            self.assertIsNone(masking._encoder_session)
            events.append("release")
            active[0] = False
        def operation(name):
            self.assertTrue(active[0])
            events.append(name)
        masking = S(_encoder_session="before", _decoder_session="before", _sam2_variant_loaded="before")
        def purge_outcome(_op, _identifier):
            operation("purge")
            return {"ok": False, "cache_cleanup": {"freed": 0, "failed": [{"error": "invented partial"}]}}
        modules = {"shutil": S(rmtree=lambda _p: operation("remove")), "masking": masking,
                   "services.hf_cache_lock": S(DOWNLOAD_LOCK=S(acquire=acquire, release=release)),
                   "services.hf_cache": S(purge_outcome=purge_outcome, purge_subfolder=forbidden)}
        ns = {"__builtins__": {**vars(builtins), "__import__": lambda name, *_a, **_k: modules[name]},
              "request": S(get_json=lambda **_k: {"model_id": "sam2-tiny"}),
              "PIPELINE_MODELS": {"sam2-tiny": {"subfolder": "group"}},
              "json_error": forbidden, "jsonify": lambda value: value,
              "os": S(path=S(join=posixpath.join, isdir=lambda _p: True)),
              "pipeline_models_dir": lambda: "/invented/models"}
        selected(Path("web/models.py"), {"api_models_pipeline_delete"}, ns)
        result = ns["api_models_pipeline_delete"]()
        self.assertEqual(events, ["acquire", "remove", "purge", "release"])
        self.assertIs(result["ok"], False)
        self.assertFalse(active[0])
        def failed_removal(_path):
            operation("failed remove")
            raise OSError("invented install failure")
        modules["shutil"].rmtree = failed_removal
        events.clear()
        with self.assertRaisesRegex(OSError, "invented install failure"):
            ns["api_models_pipeline_delete"]()
        self.assertEqual(events, ["acquire", "failed remove", "release"])
        self.assertFalse(active[0])

    def test_storage_contention_message_remains_truthful_for_non_download_holder(self):
        _fs, service_ns = service()
        service_ns["DOWNLOAD_LOCK"] = S(acquire=lambda **_k: False, release=forbidden)
        def remove(_keys):
            return service_ns["remove"](_keys, ROOT, "/invented/models")
        ns = {"__builtins__": {**vars(builtins), "__import__": forbidden},
              "request": S(get_json=lambda **_k: {"keys": [KEY]}),
              "hf_cache_service": S(remove=remove, DownloadInProgress=service_ns["DownloadInProgress"],
                                    UnsafeCacheCleanup=service_ns["UnsafeCacheCleanup"]),
              "json_error": lambda message, status=400: (message, status), "jsonify": forbidden}
        tree = ast.parse((VIREO / "web/storage.py").read_text())
        route = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "api_storage_hf_cache_remove")
        # The function's own service import is redirected to the selected AST service.
        ns["__builtins__"]["__import__"] = lambda *_a, **_k: S(hf_cache=ns["hf_cache_service"])
        route.decorator_list = []
        exec(compile(ast.Module(body=[route], type_ignores=[]), "<invented-storage>", "exec"), ns)
        message, status = ns[route.name]()
        self.assertEqual(status, 409)
        self.assertIn("model or cache operation", message)
        self.assertNotIn("download", message.lower())


if __name__ == "__main__":
    unittest.main()
