import os
import subprocess
import tempfile


def test_build_static_produces_files():
    """build_static.py produces HTML files with no Jinja2 syntax."""
    with tempfile.TemporaryDirectory() as outdir:
        result = subprocess.run(
            ["python", "scripts/build_static.py", "--outdir", outdir],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, result.stderr

        # Check that output files exist
        expected_pages = [
            "browse.html", "review.html", "lightroom.html", "audit.html",
            "cull.html", "pipeline.html", "pipeline_review.html",
            "variants.html", "workspace.html", "compare.html",
            "settings.html", "stats.html", "logs.html",
        ]
        for page in expected_pages:
            path = os.path.join(outdir, page)
            assert os.path.isfile(path), f"Missing: {page}"
            with open(path) as f:
                content = f.read()
            # No Jinja2 syntax should remain
            assert "{%" not in content, f"{page} still has Jinja2 block tags"
            assert "{{" not in content, f"{page} still has Jinja2 variable tags"
            # Navbar content should be inlined
            assert 'class="navbar"' in content, f"{page} missing navbar"
