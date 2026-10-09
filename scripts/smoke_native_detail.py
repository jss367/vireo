#!/usr/bin/env python3
"""Build and run a minimal frozen app to verify native-filter distribution."""

import argparse
import multiprocessing
import subprocess
import sys
from pathlib import Path


def check_filters():
    import numpy as np
    from vireo._native_detail import Filters

    image = np.arange(35, dtype=np.float32).reshape(5, 7) / 35
    kernel = np.array([.25, .5, .25], dtype=np.float32)
    padded = np.pad(image, ((1, 1), (0, 0)), mode="reflect")
    vertical = sum(padded[i:i + 5] * weight for i, weight in enumerate(kernel))
    padded = np.pad(vertical, ((0, 0), (1, 1)), mode="reflect")
    expected = sum(padded[:, i:i + 7] * weight for i, weight in enumerate(kernel))
    np.testing.assert_array_equal(Filters(2).gaussian(image, kernel), expected)
    np.testing.assert_array_equal(Filters(1).bilateral(image, 1.6, .07, 0), image)
    print("Frozen native detail filters loaded and matched the reference", flush=True)


def check_child(connection):
    check_filters()
    connection.send("ok")
    connection.close()


def check_spawned_filters():
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(target=check_child, args=(child,))
    try:
        process.start()
        child.close()
        if not parent.poll(30):
            raise RuntimeError("Frozen native filter child did not respond")
        assert parent.recv() == "ok"
        process.join(timeout=10)
        assert process.exitcode == 0
    finally:
        if process.pid is not None and process.is_alive():
            process.terminate()
            process.join(timeout=5)
        parent.close()
        child.close()
    print("Frozen spawned child loaded native detail filters", flush=True)


def main():
    if getattr(sys, "frozen", False):
        check_filters()
        check_spawned_filters()
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(".context/native-smoke"))
    args = parser.parse_args()
    destination = args.output.resolve()
    root = Path(__file__).resolve().parents[1]
    subprocess.run([
        sys.executable, "-m", "PyInstaller", "--noconfirm", "--clean", "--onefile",
        "--name", "native-detail-smoke", "--paths", str(root),
        "--hidden-import", "vireo._native_detail",
        "--exclude-module", "matplotlib", "--exclude-module", "scipy",
        "--exclude-module", "pandas", "--exclude-module", "IPython",
        "--exclude-module", "pytest", "--exclude-module", "tkinter",
        "--distpath", str(destination / "dist"),
        "--workpath", str(destination / "build"), "--specpath", str(destination),
        str(Path(__file__).resolve()),
    ], check=True, cwd=root)
    binary = destination / "dist" / ("native-detail-smoke.exe" if sys.platform == "win32" else "native-detail-smoke")
    from build_sidecar import assert_native_detail_bundled, assert_no_evaluation_modules

    assert_native_detail_bundled(binary)
    assert_no_evaluation_modules(binary)
    subprocess.run([str(binary)], check=True, cwd=destination, timeout=60)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
