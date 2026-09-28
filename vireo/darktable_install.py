"""Download and install darktable from its official GitHub releases.

darktable is GPL and publishes signed-by-nobody but digest-bearing release
assets on GitHub.  We resolve the latest release at request time rather than
pinning a version, and verify integrity against the SHA256 digest the GitHub
API publishes alongside each asset.

There is deliberately no code-signature check: darktable does not successfully
notarize its macOS builds (upstream issue #19295) and its Windows installer is
unsigned, so a fail-closed signature check would reject every legitimate
download.  See docs/superpowers/specs/2026-07-26-darktable-download-design.md.
"""

import hashlib
import json
import logging
import os
import platform
import posixpath
import re
import shutil
import ssl
import stat
import subprocess
import sys
import time
import urllib.parse
import urllib.request

import certifi

try:
    from . import develop
except ImportError:
    import develop

log = logging.getLogger(__name__)

# Use certifi's CA bundle so HTTPS works on macOS without running
# Install Certificates.command — same convention as taxonomy.py:35,
# labels.py:17, model_verify.py:29 and places.py:57. Without it this
# silently degrades to "Could not reach GitHub" on affected installs.
_ssl_ctx = ssl.create_default_context(cafile=certifi.where())

RELEASES_API = "https://api.github.com/repos/darktable-org/darktable/releases/latest"

_ALLOWED_HOSTS = {"github.com"}
# Every real browser_download_url is
# https://github.com/darktable-org/darktable/releases/download/<tag>/<file>.
# Constraining to the release-download path (not just the repo) means a URL
# pointing at repo *content* — /raw/master/evil.sh — cannot pass.
_ALLOWED_PATH_PREFIX = "/darktable-org/darktable/releases/download/"

# Anything smaller than this is not a darktable build.  This compares the size
# the GitHub API *reports*, so it filters out the small siblings listed next to
# the real builds (the ~300KB .zsync manifests, the 195-byte .asc signature) and
# an asset renamed to look like a build.  It cannot detect a truncated download —
# that is the digest check's job.
_MIN_ASSET_BYTES = 10 * 1024 * 1024

# (sys.platform, platform.machine()) -> required asset-name suffix.
# Exact suffixes, never substrings: "Darktable-5.6.0-x86_64.AppImage.zsync"
# contains "AppImage" but is a 300KB delta manifest, not the application.
_ASSET_SUFFIXES = {
    ("darwin", "arm64"): "-arm64.dmg",
    ("darwin", "x86_64"): "-x86_64.dmg",
    ("win32", "amd64"): "-win64.exe",
    ("win32", "arm64"): "-woa64.exe",
    ("linux", "x86_64"): "-x86_64.AppImage",
    ("linux", "aarch64"): "-aarch64.AppImage",
}

# User-facing failure sentences returned by resolve_release().  These are shown
# verbatim next to a plain "Get darktable" link, so each one must state the
# fact the user needs in order to act: retry later, or stop looking.
REASON_UNREACHABLE = "Could not reach GitHub to check for a darktable release."
REASON_NO_PLATFORM_BUILD = "No darktable build is published for this platform."
REASON_NO_USABLE_ASSET = (
    "The latest darktable release did not contain a usable build for this platform."
)


def _url_is_trusted(url):
    """True only for HTTPS release-asset URLs under the darktable repo.

    Enforced on the API-supplied browser_download_url only.  GitHub redirects
    release downloads to release-assets.githubusercontent.com, so applying
    this to redirect targets would reject every legitimate download.

    The path is percent-decoded and normalised before the prefix test: GitHub
    resolves ".." on receipt, so a raw startswith() would accept
    /darktable-org/darktable/../../attacker/evil/releases/download/x/evil.exe.
    """
    if not isinstance(url, str):
        return False
    try:
        parts = urllib.parse.urlparse(url)
    except ValueError:
        return False
    if parts.scheme != "https":
        return False
    if (parts.hostname or "").lower() not in _ALLOWED_HOSTS:
        return False
    path = posixpath.normpath(urllib.parse.unquote(parts.path)).lower()
    return path.startswith(_ALLOWED_PATH_PREFIX)


def select_asset(release, platform_name, machine):
    """Pick the asset matching this platform, or None.

    Returns a dict with name/size/url/digest, or None when this platform has
    no build and when every asset whose name matches this platform's suffix was
    rejected as implausibly small or as having an untrusted URL.  A rejected
    candidate is skipped, not fatal: a single poisoned entry ahead of the
    genuine asset must not take the feature down.

    Tolerates a malformed release payload (anything that is not a dict, or an
    "assets" list holding non-dict entries) by returning None.
    """
    if not isinstance(release, dict):
        return None

    suffix = _ASSET_SUFFIXES.get((platform_name, str(machine).lower()))
    if not suffix:
        log.info("No darktable asset for platform=%s machine=%s", platform_name, machine)
        return None

    assets = release.get("assets")
    if not isinstance(assets, list):
        return None

    for asset in assets:
        if not isinstance(asset, dict):
            continue
        name = asset.get("name")
        if not isinstance(name, str) or not name.endswith(suffix):
            continue
        size = asset.get("size")
        if not isinstance(size, int) or size < _MIN_ASSET_BYTES:
            log.warning("Rejecting %s: %r bytes is too small to be darktable", name, size)
            continue
        url = asset.get("browser_download_url", "")
        if not _url_is_trusted(url):
            log.warning("Rejecting %s: untrusted download URL %r", name, url)
            continue
        return {
            "name": name,
            "size": size,
            "url": url,
            "digest": asset.get("digest"),
        }
    return None


def resolve_release(timeout=15):
    """Fetch the latest release and select this machine's asset.

    Returns ``(release, None)`` on success, or ``(None, reason)`` where reason
    is a user-facing sentence explaining which failure occurred.  The caller
    shows that sentence next to a plain "Get darktable" link, so it must be
    true and specific: "no build for your platform" and "we could not reach
    GitHub" are different facts and users act on them differently.

    Never raises.
    """
    machine = platform.machine()
    if (sys.platform, str(machine).lower()) not in _ASSET_SUFFIXES:
        log.info("No darktable build for platform=%s machine=%s", sys.platform, machine)
        return None, REASON_NO_PLATFORM_BUILD

    try:
        req = urllib.request.Request(
            RELEASES_API,
            headers={
                # GitHub answers 403 to requests with no User-Agent.
                "User-Agent": "vireo-darktable-install/1.0",
                "Accept": "application/vnd.github+json",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl_ctx) as resp:
            release = json.loads(resp.read().decode("utf-8"))
        if not isinstance(release, dict) or not isinstance(release.get("assets"), list):
            # A rate-limit body, a captive-portal login page or a truncated
            # response parses fine but is not a release.  Saying "no usable
            # build in the latest release" here would be a lie.
            raise ValueError("GitHub returned something that is not a release object")
        asset = select_asset(release, sys.platform, machine)
    except Exception:
        log.warning("Could not reach the GitHub releases API", exc_info=True)
        return None, REASON_UNREACHABLE

    if not asset:
        return None, REASON_NO_USABLE_ASSET

    tag = release.get("tag_name")
    version = tag.removeprefix("release-") if isinstance(tag, str) else ""
    return {"version": version, **asset}, None


# Unauthenticated GitHub API calls are limited to 60/hour per IP.  Settings can
# be opened repeatedly in a session, so cache the answer briefly rather than
# spending that budget and degrading everyone on the IP to the fallback link.
_release_cache = {"at": 0.0, "value": None}
_RELEASE_CACHE_SECS = 600


def resolve_release_cached():
    """resolve_release() with a short TTL, returning the same 2-tuple.

    Only successes are cached.  A transient outage or a rate-limit reply must
    not pin the fallback link for ten minutes — and caching a failure would
    also freeze its reason string, so a user who fixed their network would keep
    being told GitHub is unreachable.

    Never raises, for the same reason resolve_release does not.
    """
    now = time.monotonic()
    cached = _release_cache["value"]
    if cached is not None and now - _release_cache["at"] < _RELEASE_CACHE_SECS:
        return cached, None
    release, reason = resolve_release()
    if release is not None:
        _release_cache.update(at=now, value=release)
    return release, reason


def update_release_cache(release):
    """Overwrite the cached release with a freshly resolved one.

    Called by the download endpoint when its uncached resolve_release() has
    already proved the stored value stale — the asset the user just confirmed
    disagrees with what darktable-org is publishing right now.  Without this,
    the client's Re-check would hit /install/available, get the same
    ten-minute-old cached asset back, re-confirm it, and receive the same 409
    in a loop until the TTL expires.  Writing the fresh release here means the
    next Re-check shows what the server actually intends to download.

    Does nothing on a falsy release: a failed resolution must never pin the
    fallback link (mirrors resolve_release_cached's "only cache successes").
    """
    if not release:
        return
    _release_cache.update(at=time.monotonic(), value=release)


# Read the asset a megabyte at a time: these builds are 87-178MB and hashing
# happens right after the download, so f.read() with no size would hold the
# whole file in RAM on top of whatever the download already cost.
_HASH_CHUNK_BYTES = 1024 * 1024

_SHA256_HEX_RE = re.compile(r"\A[0-9a-f]{64}\Z")


def verify_digest(path, expected, *, should_cancel=None):
    """Compare the file's SHA256 against the digest published by the API.

    Returns ``(ok, human_readable_detail)``.  The detail is shown to the user
    verbatim, so it must be honest about what was and was not checked: a
    matching digest proves the bytes are what GitHub's API said they are, not
    that darktable signed them.  The digest arrives in the same HTTPS response
    as the download URL, so it shares that response's trust boundary — it is
    not an independent signature, and there is none to check (see the module
    docstring).  This is the only integrity check the feature has.

    ``expected`` comes from an external API, so its cosmetics are tolerated:
    the ``sha256:`` prefix is optional, hex case and surrounding whitespace are
    ignored.  Anything we cannot actually evaluate — another algorithm, a value
    that is not a 64-character hexdigest, a non-string — fails closed with its
    own sentence rather than being reported as a mismatch, which would blame an
    intact file, or silently passing, which would leave the download unchecked.

    ``should_cancel`` is polled between chunks so a Stop press during the
    ~178MB hash is felt within a chunk instead of after the whole file: raising
    taxonomy.DownloadCancelled propagates out through download() to the job
    thread, where it is reported as cancelled rather than as an installer
    hand-off (which would happen anyway if the digest matched).
    """
    if not expected:
        # Passing here is deliberate: every asset ships a digest today, and
        # refusing the download if one is ever missing would break the feature
        # over something the user cannot fix.  Say so instead of implying a
        # check happened.
        log.warning("No digest published for %s — accepting it unverified", path)
        return True, (
            "GitHub published no digest for this asset, so its contents could not be "
            "verified. The download came from darktable's official GitHub release, but "
            "nothing confirms the bytes arrived intact."
        )

    if not isinstance(expected, str):
        log.warning("Cannot verify %s: digest is %r, not a string", path, type(expected).__name__)
        return False, (
            "Could not verify the download: GitHub sent a digest in a form Vireo does "
            "not understand."
        )

    # rpartition, not partition: with no ":" at all it puts the whole string in
    # the last field, so a bare hexdigest is read as a hash with no algorithm
    # rather than as an algorithm with no hash.  The strips also absorb any
    # whitespace around the value, since leading whitespace lands on the
    # algorithm and trailing whitespace on the hash.
    algorithm, _, value = expected.rpartition(":")
    algorithm = algorithm.strip().lower() or "sha256"
    want = value.strip().lower()

    if algorithm != "sha256":
        log.warning("Cannot verify %s: digest algorithm is %r, not sha256", path, algorithm)
        return False, (
            # Truncated: this is an API-supplied string being rendered in the UI.
            f'Could not verify the download: GitHub published the digest as "'
            f'{algorithm[:20]}" and Vireo only checks SHA256.'
        )
    if not _SHA256_HEX_RE.match(want):
        log.warning("Cannot verify %s: %r is not a SHA256 hexdigest", path, expected)
        return False, (
            "Could not verify the download: the digest GitHub published is not a valid "
            "SHA256 hash."
        )

    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(_HASH_CHUNK_BYTES), b""):
                # Poll BEFORE the update so a cancel-just-before-EOF is felt on
                # the last iteration rather than after the hexdigest is
                # computed.  Do it here rather than inside a try/except in the
                # caller: verify_digest is what actually holds the file open,
                # and skipping the update on cancel avoids one more chunk of
                # work per polling interval.
                if should_cancel is not None and should_cancel():
                    try:
                        from .taxonomy import DownloadCancelled
                    except ImportError:
                        from taxonomy import DownloadCancelled
                    raise DownloadCancelled("Download cancelled")
                h.update(chunk)
    except OSError as exc:
        # Task 9 calls this from a job thread with no except clause of its own.
        log.warning("Could not read %s to verify it: %s", path, exc)
        return False, f"Could not read the downloaded file to verify it: {exc.strerror or exc}"
    actual = h.hexdigest()

    if actual == want:
        return True, (
            f"SHA256 matches the digest GitHub published for this asset ({actual[:16]}…). "
            "That confirms the bytes are what GitHub said to expect; darktable does not "
            "sign its builds, so there is no signature to check."
        )
    log.warning("Digest mismatch for %s: expected %s, got %s", path, want, actual)
    return False, (
        f"SHA256 mismatch — expected {want}, got {actual}. The download does not match "
        "the digest GitHub published, so it was truncated, corrupted or replaced."
    )


def install_dir():
    """Where downloads land, on every platform.

    Also where the ``.partial`` resume file lives and which filesystem the
    free-space check measures.  Installers are kept after hand-off — the user
    may want to re-run them.

    Delegates to develop.darktable_tools_dir() rather than re-deriving the
    path: on Linux that is the *only* directory darktable_search_paths probes,
    so downloading anywhere else would install darktable and still report it
    missing.
    """
    return develop.darktable_tools_dir()


def is_quarantined(path):
    """True if macOS tagged the file with com.apple.quarantine.

    Measured behaviour: urllib downloads are NOT quarantined (LaunchServices
    applies that attribute for browser-style downloads), so this is normally
    False and the Gatekeeper warning stays hidden.  Checked rather than
    assumed in either direction.

    Never raises: a platform without the attribute, or without the xattr tool,
    is simply "not quarantined" — an install that otherwise worked must not
    fail over a cosmetic warning.
    """
    if sys.platform != "darwin":
        return False
    try:
        result = subprocess.run(
            ["xattr", "-p", "com.apple.quarantine", path],
            capture_output=True, text=True, timeout=10,
        )
        return result.returncode == 0
    except Exception:
        log.debug("Could not check the quarantine attribute of %s", path, exc_info=True)
        return False


def _installs_into_tools_dir(platform_name=None):
    """True where the downloaded artifact *is* the runnable binary.

    With no override this is develop.darktable_uses_tools_dir() verbatim, so
    hand_off cannot branch differently from the directory darktable_search_paths
    probes.  ``platform_name`` is an explicit-platform override for tests and
    for callers that already know which build they fetched.
    """
    if platform_name is None:
        return develop.darktable_uses_tools_dir()
    return platform_name not in ("darwin", "win32")


def hand_off(path, platform_name=None):
    """Hand the downloaded artifact to the platform.

    Returns ``{action, location, bin_path}``.  Does NOT write config: bin_path
    is returned so the job handler writes darktable_bin in one place, next to
    the message that tells the user it happened.

    Raises RuntimeError if the installer could not be opened.  The message
    names the downloaded file, because it is still on disk and opening it by
    hand is the user's way forward — reporting "opened-installer" anyway would
    leave them waiting for a window that never appears.
    """
    if _installs_into_tools_dir(platform_name):
        # Linux: the AppImage we just downloaded is the program itself.  It is
        # deliberately left as-is rather than pre-extracted; see the AppImage
        # note in docs/superpowers/plans/2026-07-26-darktable-download.md.
        mode = os.stat(path).st_mode
        os.chmod(path, mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        return {"action": "installed", "location": path, "bin_path": path}

    if (platform_name or sys.platform) == "darwin":
        # No capture_output: `open` hands the file to LaunchServices and exits,
        # and piping its streams risks blocking on a handle the launched app
        # inherits.  The exit status is the part we need.
        result = subprocess.run(["open", path], timeout=30)
        if result.returncode != 0:
            raise RuntimeError(
                f"Downloaded to {path}, but macOS could not open it "
                f"(exit code {result.returncode}). Open it yourself to install darktable."
            )
    else:
        try:
            os.startfile(path)
        except OSError as exc:
            raise RuntimeError(
                f"Downloaded to {path}, but Windows could not open it ({exc}). "
                "Open it yourself to install darktable."
            ) from exc

    # The user chooses where the app lands, so we cannot know bin_path here.
    # Detection (Task 1) finds it on the next re-check.
    return {"action": "opened-installer", "location": path, "bin_path": None}


def download(asset, dest_dir=None, byte_callback=None, should_cancel=None):
    """Download one resolved asset, verify it, and return ``(path, detail)``.

    ``detail`` is verify_digest's sentence, returned verbatim: ok=True covers
    both "the digest matched" and "GitHub published no digest", and only that
    sentence distinguishes them.  Callers must show it rather than deriving
    their own "verified" wording from the boolean.

    Raises RuntimeError on a size or digest mismatch, deleting the bad file: a
    wrong artifact must never be handed to an installer.  A cancellation
    propagates as taxonomy.DownloadCancelled with the ``.partial`` left in
    place for resume — should_cancel is checked before the first read, so an
    immediately cancelled download leaves a 0-byte ``.partial``, which is a
    normal resume state and not a corrupt-download signal.
    """
    try:
        from .taxonomy import _download_with_resume
    except ImportError:
        from taxonomy import _download_with_resume

    # The name comes from the GitHub API, so it is joined only after being
    # reduced to a bare filename: a name carrying path separators would
    # otherwise write outside the tools directory.
    name = os.path.basename(asset["name"])
    if name != asset["name"] or name in ("", ".", ".."):
        raise RuntimeError(
            f"Refusing to download an asset with a suspicious name: {asset['name']!r}"
        )

    dest_dir = dest_dir or install_dir()
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, name)

    # Fast path: a previous attempt already finished the transfer but was
    # cancelled during verify_digest.  _download_with_resume renames its
    # .partial to dest on success, so cancellation observed inside the
    # hash loop leaves no .partial to resume from — a naive retry would
    # re-download the whole 87-178 MB asset even though the complete file
    # is sitting right there.  If dest already exists at exactly the size
    # the API published, skip straight to verify: verify_digest still
    # decides whether the bytes are good (delete on mismatch, keep on
    # match).  We deliberately gate on the API-supplied size so an
    # unrelated file the user dropped in the tools directory does not get
    # accepted as this asset.
    expected_size = asset.get("size")
    already_complete = bool(
        expected_size
        and os.path.exists(dest)
        and os.path.getsize(dest) == expected_size
    )
    if already_complete:
        # Emit final progress so the UI's byte counter jumps to full instead
        # of stalling at 0 while the (skipped) download "runs".  Match
        # _download_with_resume's contract that callbacks may not raise —
        # there they would be misreported as network failures.
        if byte_callback is not None:
            try:
                byte_callback(expected_size, expected_size)
            except Exception:
                log.debug("Progress callback raised on a completed download", exc_info=True)
    else:
        _download_with_resume(
            asset["url"], dest,
            byte_callback=byte_callback,
            should_cancel=should_cancel,
        )

    actual_size = os.path.getsize(dest)
    if asset.get("size") and actual_size != asset["size"]:
        os.remove(dest)
        raise RuntimeError(
            f"Size mismatch — expected {asset['size']} bytes, got {actual_size}"
        )

    ok, detail = verify_digest(dest, asset.get("digest"), should_cancel=should_cancel)
    if not ok:
        os.remove(dest)
        raise RuntimeError(detail)
    return dest, detail


def free_space_bytes(path):
    """Bytes free on the filesystem holding path (creating it if needed).

    shutil.disk_usage rather than os.statvfs: it works on all three platforms
    (statvfs does not exist on Windows) and reports the same non-root-reserved
    free figure, so there is nothing to fall back to.
    """
    os.makedirs(path, exist_ok=True)
    return shutil.disk_usage(path).free
