"""Source resolution, decoding and extraction for ``/photos/<id>/original``.

``web.media_originals._OriginalPhotoRequest`` serves the full-resolution image for 1:1
zoom. The steps that need no Flask live here, each collaborator owning only
the state it writes:

- ``OriginalPhoto``: the catalog facts the view looked up (the photo row, its
  folder path, the edit recipe, whether the primary is RAW) and the read-only
  probes over them: the file-state signature of the prepared-render cache
  key, the working copy trusted as the full-resolution asset, and the
  full-size companion JPEG.
- ``EditRenderSource``: the file an edited render decodes (``EditSource``)
  and its decode, with the undersized-RAW, failed-RAW and vanished
  working-copy fallbacks.
- ``UneditedSource``: the original an unedited request resolved (path,
  offline-cache use, extension, companion, RAW display-cache path), its
  routing around a recorded RAW failure, and the last-resort decode.
- ``OriginalExtraction``: the on-demand rendition's targets (``wc_abs``,
  ``wc_rel``, quality, decode mode), the private-tempfile extraction, its
  publication, the companion re-extraction of an undersized RAW result, and
  the catalog commit.

The responses (``send_file``, streamed renditions, artifact flights) and the
locks held around them stay in ``web.media_originals``. Publication runs inside the
caller's ``publication`` context (the route's ``_preparation_publication``),
which refuses a late write by raising.
"""

from __future__ import annotations

import contextlib
import logging
import os
import tempfile
import time
from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from file_replace import replace_file
from render_source import (
    companion_image_can_replace_raw_result as _companion_image_can_replace_raw_result,
)
from render_source import (
    has_current_working_copy_failure as _has_current_working_copy_failure,
)
from render_source import (
    image_is_smaller_than_expected as _image_is_smaller_than_expected,
)
from render_source import (
    image_size_after_exif_orientation as _image_size_after_exif_orientation,
)
from render_source import (
    recipe_source_dimensions as _recipe_source_dimensions,
)
from render_source import (
    record_working_copy_failure as _record_working_copy_failure,
)
from render_source import (
    scaled_recipe_source_dimensions as _scaled_recipe_source_dimensions,
)
from working_copy_cache import (
    touch_working_copy_access,
    working_copy_publication_guard,
)

if TYPE_CHECKING:
    import sqlite3

    from db import Database
    from float_image import FloatImage
    from PIL.Image import Image
    from services.service_types import JsonDict

    # What ``image_loader.load_image`` decodes: a Pillow image, or a float
    # buffer for a linear RAW decode.
    LoadedImage = Image | FloatImage

log = logging.getLogger(__name__)

# The route's ``_preparation_publication``: a context that holds the
# catalog-writer transaction while a render publishes, raising when the
# photo's source or recipe changed under a preparation request.
Publication = Callable[[], AbstractContextManager[object]]


def peg_display_cache_mtime(cache_path: str, source_paths: Iterable[str | None]) -> None:
    """Align an unedited RAW display cache's mtime to its own hit check.

    ``/photos/<id>/original`` decides the display cache is fresh when
    its mtime is ``>= max(mtime(image_path), mtime(companion))``.
    Renders are written with the wall clock, so if either live source
    has a *future* mtime — clock skew, archives that preserve future
    timestamps — the just-written cache fails the check the instant
    it lands and every request re-decodes the RAW plus the companion
    JPEG. And even without clock skew, pegging the display cache to
    ``photos.file_mtime`` (the row's stored mtime for the RAW) would
    fail the check whenever a paired companion JPEG has a later
    filesystem mtime than the RAW row does, which is the common shape
    for a fresh camera dump touched by any post-import step.

    Peg to the same max the cache-hit check consults so a valid
    render satisfies its own gate. Best-effort: a failed ``utime``
    only costs the next request a re-render.
    """
    mtimes = []
    for path in source_paths:
        if not path:
            continue
        try:
            mtimes.append(os.path.getmtime(path))
        except OSError:
            continue
    if not mtimes:
        return
    # Satisfy the gate without backdating the file. ``max(mtimes)`` alone
    # stamps a render of a 2019 RAW with a 2019 mtime, and working-copy
    # quota eviction reads mtime as recency — so a copy generated on
    # demand for an old source would be born at the head of the eviction
    # queue and reclaimed before the user opened the next photo. The gate
    # only needs ``>=``, so clamping up to the wall clock keeps it honest
    # and leaves a usable recency stamp.
    peg = max(max(mtimes), time.time())
    with contextlib.suppress(OSError, TypeError, ValueError):
        os.utime(cache_path, (peg, peg))


def _file_render_state(path: str | None) -> JsonDict | None:
    if not path:
        return None
    try:
        stat = os.stat(path)
    except OSError:
        return None
    return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


@dataclass(frozen=True)
class OriginalPhoto:
    """The catalog facts one ``/original`` request looked up before it began.

    ``photo`` is the workspace-verified photo row, ``folder_path`` its
    folder's path, ``recipe`` the stored edit recipe (``None`` when
    unedited) and ``primary_is_raw`` whether the catalogued file is a RAW.
    """

    db: Database
    photo_id: int
    photo: sqlite3.Row
    folder_path: str
    vireo_dir: str
    recipe: JsonDict | None
    primary_is_raw: bool

    def collect_file_state(self) -> JsonDict:
        """Return the live and cached source states the render cache keys on."""
        photo = self.photo
        offline_row = self.db.caches.offline_original_get(self.photo_id)
        cached_original = (
            os.path.join(self.vireo_dir, offline_row["original_path"])
            if offline_row and offline_row["original_path"]
            else None
        )
        file_state = {
            "primary": {
                "source": _file_render_state(
                    os.path.join(self.folder_path, photo["filename"]),
                ),
                "cached": _file_render_state(cached_original),
            },
            "companion": None,
        }
        if photo["companion_path"]:
            cached_companion = (
                os.path.join(self.vireo_dir, offline_row["companion_path"])
                if offline_row and offline_row["companion_path"]
                else None
            )
            file_state["companion"] = {
                "source": _file_render_state(
                    os.path.join(self.folder_path, photo["companion_path"]),
                ),
                "cached": _file_render_state(cached_companion),
            }
        return file_state

    def trusted_full_res_working_copy_path(self) -> str | None:
        """Return the working copy when it is trusted as the full-res asset.

        Decide whether to trust the working copy as the full-res asset
        by reading its actual on-disk dimensions, NOT the current
        ``working_copy_max_size`` config — the cap may have changed
        since the wc was generated, leaving stale capped wcs that
        config-based logic would misclassify as full-res.

        PIL.Image.open is lazy: it reads the JPEG SOF marker for
        ``.size`` without decoding pixels (sub-millisecond), so this
        is safe to do per request even during burst-review zoom. The
        expensive path we must avoid is the RAW re-extract below
        (5–7s per photo), not the header read.
        """
        photo = self.photo
        if not photo["working_copy_path"]:
            return None
        wc_path = os.path.join(self.vireo_dir, photo["working_copy_path"])
        if not os.path.exists(wc_path):
            return None
        from PIL import Image as _PILImage

        try:
            with _PILImage.open(wc_path) as _wc_img:
                wc_w, wc_h = _image_size_after_exif_orientation(_wc_img)
        except Exception:  # broad: Pillow decode errors are undocumented; unreadable means no size
            log.debug("Could not read working copy size of %s", wc_path, exc_info=True)
            wc_w = wc_h = 0
        # Compare in display-orientation space: ``extract_working_copy``
        # writes the EXIF-transposed JPEG (e.g. 4000x6000 for a portrait
        # RAW), while ``photo["width"]/height`` are the sensor axes
        # (6000x4000). Comparing raw sensor axes rejects a valid
        # full-resolution WC for portrait RAWs and either 500s or forces
        # a redundant re-decode. ``_recipe_source_dimensions`` swaps the
        # sensor axes when EXIF Orientation indicates it, matching what
        # ``load_image`` returns.
        orig_w, orig_h = _recipe_source_dimensions(photo)
        # Trust the wc when it meets/exceeds the believed original dims,
        # or when those dims are unknown (no basis to declare the wc stale
        # and a speculative RAW re-extract would just thrash the disk).
        if wc_w and wc_h and (
            (wc_w >= orig_w and wc_h >= orig_h) or not (orig_w and orig_h)
        ):
            return wc_path
        # The wc is smaller than the believed original. For RAW sources
        # this often means rawpy.postprocess() failed and we fell back to
        # the embedded JPEG, which can be a few pixels shy of the full
        # sensor area. Re-extracting would yield the same fallback image,
        # just slower — so trust the wc when BOTH axes are within 1% of
        # the believed dims. A long-edge-only check would silently accept
        # an embedded JPEG whose long edge is full but whose short edge is
        # substantially truncated (e.g. 6000x3376 for a 6000x4000 source),
        # then apply the edit recipe to a cropped image. This tolerance is
        # RAW-only: for JPEG/PNG/etc., the wc being smaller means the cap
        # downsized it, and re-extracting WILL produce more pixels.
        from image_loader import RAW_EXTENSIONS
        ext = os.path.splitext(photo["filename"])[1].lower()
        if (
            ext in RAW_EXTENSIONS
            and wc_w and wc_h
            and orig_w and orig_h
            and wc_w >= orig_w * 0.99
            and wc_h >= orig_h * 0.99
        ):
            return wc_path
        return None

    def full_res_companion_path(self, folder_path: str, using_offline_cache: bool = False) -> str | None:
        photo = self.photo
        companion_path = photo["companion_path"]
        if not companion_path:
            return None
        companion_abs = os.path.join(folder_path, companion_path)
        if using_offline_cache:
            offline_row = self.db.caches.offline_original_get(self.photo_id)
            if offline_row and offline_row["companion_path"]:
                offline_companion = os.path.join(
                    self.vireo_dir, offline_row["companion_path"]
                )
                if os.path.exists(offline_companion):
                    companion_abs = offline_companion
        if not os.path.exists(companion_abs):
            return None
        orig_w = photo["width"]
        orig_h = photo["height"]
        if not (orig_w and orig_h):
            return None
        from PIL import Image as _PILImage
        try:
            with _PILImage.open(companion_abs) as _cimg:
                c_w, c_h = _cimg.size
        except Exception:  # broad: Pillow decode errors are undocumented; unreadable means no size
            log.debug("Could not read companion JPEG size of %s", companion_abs, exc_info=True)
            return None
        # Camera JPEGs commonly omit a narrow sensor border. Match the
        # tolerance used by camera-rendered RAW loading so a near-full
        # sidecar remains the preferred tone-consistent display source.
        if c_w >= orig_w * 0.99 and c_h >= orig_h * 0.99:
            return companion_abs
        return None


@dataclass
class EditSource:
    """The file an edited ``/original`` render decodes, and how it was found."""

    path: str
    using_offline_cache: bool
    ext: str
    is_working_copy: bool = False


class EditRenderSource:
    """Pick and decode the source an edited ``/original`` render runs over.

    ``trusted_wc_path`` is the working copy the route trusts as the
    full-resolution asset (``None`` when there is none). The selected
    ``EditSource`` is the only state; the decode fallbacks rewrite its path
    (and extension) when another file supplied the pixels.
    """

    def __init__(self, original: OriginalPhoto, trusted_wc_path: str | None) -> None:
        self.original = original
        self.trusted_wc_path = trusted_wc_path

    def select(self) -> EditSource:
        from image_loader import RAW_EXTENSIONS

        photo = self.original.photo
        primary_is_raw = self.original.primary_is_raw
        trusted_wc_path = self.trusted_wc_path
        # For edited RAW primaries, a "trusted" working copy can still
        # predate the highlight-preserving RAW decode (the migration
        # purges previews and thumbnails but not working copies). Force
        # the RAW path so the recipe runs over preserve-highlights bytes,
        # not the older clipped-JPEG working copy.
        image_path = None if primary_is_raw else trusted_wc_path
        using_offline_cache = False
        if image_path is None:
            from offline_cache import resolve_original_path
            image_path, using_offline_cache = resolve_original_path(
                self.original.db,
                photo,
                self.original.vireo_dir,
                {photo["folder_id"]: self.original.folder_path},
                prefer_cached=True,
            )
            image_path = cast("str", image_path)  # resolve_original_path always returns a path
            companion_source = self.original.full_res_companion_path(
                self.original.folder_path, using_offline_cache,
            )
            image_ext = os.path.splitext(image_path)[1].lower()
            source_failure_current = (
                primary_is_raw
                and _has_current_working_copy_failure(
                    photo,
                    self.original.vireo_dir,
                    trust_existing_working_copy=False,
                    live_source_path=image_path,
                    folder_path=self.original.folder_path,
                )
            )
            if (
                primary_is_raw
                and trusted_wc_path
                and not companion_source
                and (
                    not os.path.exists(image_path)
                    or source_failure_current
                )
            ):
                image_path = trusted_wc_path
            elif companion_source and image_ext not in RAW_EXTENSIONS:
                image_path = companion_source
            elif (
                primary_is_raw
                and companion_source
                and source_failure_current
            ):
                # Mirror _recipe_render_source: when scanner has marked
                # this RAW as failed for the current mtime, route the
                # edited render through the companion JPEG so a previous
                # request that already succeeded via the companion
                # fallback isn't shadowed by the pre-load guard below.
                image_path = companion_source
        return EditSource(
            path=image_path,
            using_offline_cache=using_offline_cache,
            ext=os.path.splitext(image_path)[1].lower(),
        )

    def load(self, source: EditSource) -> LoadedImage | None:
        from image_loader import (
            RAW_DECODE_LINEAR,
            RAW_EXTENSIONS,
            load_image,
        )

        raw_decode = (
            RAW_DECODE_LINEAR
            if source.ext in RAW_EXTENSIONS
            else None
        )
        load_kwargs = {"raw_decode": raw_decode} if raw_decode else {}
        # Stamp the working copy as recently used whenever we read it
        # as an edit-render source. ``_serve_trusted_working_copy``
        # only touches when the WC JPEG itself is returned via
        # ``send_file``, so an actively edited non-RAW photo whose
        # renders decode from ``trusted_wc_path`` and encode to a
        # ``prepared_full_resolution_render`` cache would retain its
        # generation mtime and stay first in the eviction queue no
        # matter how often it was displayed at 1:1. Recording access
        # here keeps the LRU ordering aligned with actual use.
        #
        # ``touch_working_copy_access`` documents that callers hold
        # ``working_copy_publication_guard`` — the same lock the
        # quota pass takes — so the mtime move cannot land between
        # ``_evict_once``'s directory scan and its unlink. Without
        # the guard, an unlucky touch during a quota reduction
        # changes the file's fingerprint after eviction snapshotted
        # it; ``_file_identity(os.stat(path)) != sampled_identity``
        # then treats the file as replaced and skips it, so the
        # pass returns fewer freed bytes than needed. Because
        # ``deferred=True`` only fires on ``PRAGMA data_version``
        # invalidation (not on identity skips), the settings flow
        # never schedules its background retry and the cache can
        # sit above the requested quota until another write or
        # restart.
        #
        # The decode stays outside the guard. This is the 1:1
        # pixel-peeping path; the guard is process-wide and the
        # quota pass holds it across a scandir of the whole cache,
        # so wrapping a full-resolution Pillow decode in it would
        # make every zoomed view queue behind every other one.
        # Leaving the decode unguarded keeps main's behaviour for
        # the exists/open race (a vanished copy 500s and records a
        # failure marker) and adds only the touch.
        source.is_working_copy = (
            self.trusted_wc_path is not None
            and source.path == self.trusted_wc_path
        )
        if source.is_working_copy:
            with working_copy_publication_guard():
                touch_working_copy_access(self.trusted_wc_path)
        img = load_image(
            source.path, max_size=None, **load_kwargs,
        )
        if (
            img is not None
            and source.ext in RAW_EXTENSIONS
            and self.original.photo["width"]
            and self.original.photo["height"]
        ):
            img = self._replace_undersized_edit_decode(img, source)
        if img is None and source.ext in RAW_EXTENSIONS:
            img = self._rescue_failed_edit_decode(source)
        if img is None and source.is_working_copy:
            img = self._retry_vanished_working_copy(source)
        return img

    def _replace_undersized_edit_decode(self, img: LoadedImage, source: EditSource) -> LoadedImage:
        """Swap an undersized RAW decode for the full-size companion.

        _load_raw falls back to the embedded JPEG even in
        preserve-highlights mode when libraw can't demosaic the
        sensor data, so a successful load can still be the small
        camera preview rather than full-resolution pixels. For
        1:1 edited views that's the wrong file to cache — try
        the full-size companion before saving an undersized
        prepared full-resolution render cache.
        """
        from image_loader import load_image

        expected_w, expected_h = _scaled_recipe_source_dimensions(self.original.photo)
        if _image_is_smaller_than_expected(img, expected_w, expected_h):
            companion_fallback = self.original.full_res_companion_path(
                self.original.folder_path, source.using_offline_cache,
            )
            if companion_fallback and companion_fallback != source.path:
                companion_img = load_image(
                    companion_fallback, max_size=None,
                )
                if _companion_image_can_replace_raw_result(
                    companion_img, img, expected_w, expected_h,
                ):
                    log.info(
                        "RAW decode for photo %s edited original "
                        "returned undersized embedded preview "
                        "(%dx%d, expected %dx%d); falling back to "
                        "companion JPEG",
                        self.original.photo_id, img.size[0], img.size[1],
                        expected_w, expected_h,
                    )
                    img.close()
                    img = cast("LoadedImage", companion_img)  # only a decoded companion can replace
                    source.path = companion_fallback
                elif companion_img is not None:
                    companion_img.close()
        return img

    def _rescue_failed_edit_decode(self, source: EditSource) -> LoadedImage | None:
        """Decode the companion JPEG after the RAW failed to decode.

        RAW couldn't decode (unsupported variant, no embedded JPEG).
        Fall back to the full-resolution companion JPEG when one
        exists so an unsupported-RAW edit doesn't 500 with a usable
        sidecar sitting next to the RAW. When the companion rescues
        the render, record the RAW source-failure marker so the
        next request's RAW-failure routing branch above sends the
        render directly through the companion instead of paying
        for the same failing decode every hit. The pre-load guard
        at line ~18896 won't shadow it because that routing branch
        rewrites image_path to the companion before the guard runs.
        """
        from image_loader import load_image

        companion_fallback = self.original.full_res_companion_path(
            self.original.folder_path, source.using_offline_cache,
        )
        raw_source_path = source.path
        img = None
        if companion_fallback and companion_fallback != source.path:
            log.info(
                "RAW decode failed for photo %s edited original; "
                "falling back to companion JPEG", self.original.photo_id,
            )
            img = load_image(companion_fallback, max_size=None)
            if img is not None:
                source.path = companion_fallback
                _record_working_copy_failure(self.original.db, self.original.photo, raw_source_path)
            else:
                # Companion also failed — record the marker so repeated
                # requests fail fast instead of retrying both sources
                # on every hit.
                _record_working_copy_failure(self.original.db, self.original.photo, raw_source_path)
        return img

    def _retry_vanished_working_copy(self, source: EditSource) -> LoadedImage | None:
        """Decode the original after the selected working copy vanished.

        Quota enforcement can unlink the selected working copy
        after the existence check but before ``load_image`` opens
        it — a window this branch leaves open deliberately, since
        holding the process-wide publication guard across a
        full-resolution decode would serialize every zoomed view
        in the app. Retry the original source once so an
        otherwise healthy view does not become a transient 500
        during eviction, and do not record a working-copy failure
        for it: nothing is wrong with the source, the cache entry
        merely went away. Mirrors the recovery ``/edit-preview``,
        ``/crop`` and the preview materializer already have; this
        branch was the only reader without one.
        """
        from image_loader import (
            RAW_DECODE_LINEAR,
            RAW_EXTENSIONS,
            load_image,
        )

        original_retry_path = os.path.join(
            self.original.folder_path, self.original.photo["filename"],
        )
        img = None
        if original_retry_path != source.path:
            log.info(
                "Working copy for photo %s vanished before decode "
                "(quota eviction); retrying original source",
                self.original.photo_id,
            )
            retry_ext = os.path.splitext(
                original_retry_path
            )[1].lower()
            retry_kwargs = (
                {"raw_decode": RAW_DECODE_LINEAR}
                if retry_ext in RAW_EXTENSIONS
                else {}
            )
            img = load_image(
                original_retry_path, max_size=None, **retry_kwargs,
            )
            if img is not None:
                source.path = original_retry_path
                source.ext = retry_ext
        return img


class UneditedSource:
    """The original an unedited ``/original`` request reads.

    Built by ``resolve``. ``image_path`` and ``resolved_ext`` move to the
    companion when ``route_around_raw_failure`` or a decode fallback
    serves it instead; ``raw_source_path`` keeps the RAW resolved before
    that rewrite for the display cache's mtime peg.
    """

    def __init__(
        self,
        original: OriginalPhoto,
        image_path: str,
        using_offline_cache: bool,
        companion_for_extraction: str | None,
        display_cache_path: str | None,
    ) -> None:
        self.original = original
        self.image_path = image_path
        self.using_offline_cache = using_offline_cache
        self.resolved_ext = os.path.splitext(image_path)[1].lower()
        self.companion_for_extraction = companion_for_extraction
        self.display_cache_path = display_cache_path
        self.raw_source_path: str | None = None

    @classmethod
    def resolve(cls, original: OriginalPhoto) -> UneditedSource:
        # Resolve original file path
        from offline_cache import resolve_original_path
        image_path, using_offline_cache = resolve_original_path(
            original.db,
            original.photo,
            original.vireo_dir,
            {original.photo["folder_id"]: original.folder_path},
            prefer_cached=True,
        )
        image_path = cast("str", image_path)  # resolve_original_path always returns a path

        companion_for_extraction = original.full_res_companion_path(
            original.folder_path, using_offline_cache
        )

        # Keep unedited RAW display bytes separate from the edit-quality
        # working copy. The suffix is intentionally distinct from the legacy
        # originals/<id>.jpg render cache so an upgrade cannot reuse a dark
        # highlight-preserving render produced by an older version.
        display_cache_path = None
        if original.primary_is_raw:
            display_cache_path = os.path.join(
                original.vireo_dir, "originals", f"{original.photo_id}.display.jpg",
            )
        return cls(
            original, image_path, using_offline_cache,
            companion_for_extraction, display_cache_path,
        )

    def display_cache_is_current(self) -> bool:
        display_cache_path = cast("str", self.display_cache_path)  # only RAW primaries check it, and they have one
        if not os.path.exists(display_cache_path):
            return False
        try:
            source_mtimes = [
                os.path.getmtime(path)
                for path in (self.image_path, self.companion_for_extraction)
                if path and os.path.exists(path)
            ]
            source_is_older = (
                not source_mtimes
                or os.path.getmtime(display_cache_path)
                >= max(source_mtimes)
            )
        except OSError:
            source_is_older = False
        return source_is_older

    def route_around_raw_failure(self) -> bool:
        """Route a RAW whose extraction already failed to its companion.

        Returns True when the failure is current and no companion can stand
        in, so the route must fall back (or answer 500) without extracting.
        """
        from image_loader import RAW_EXTENSIONS

        original = self.original
        has_current_raw_failure = (
            (not self.using_offline_cache or self.resolved_ext in RAW_EXTENSIONS)
            and _has_current_working_copy_failure(
                original.photo, original.vireo_dir, trust_existing_working_copy=False,
                live_source_path=self.image_path, folder_path=original.folder_path,
            )
        )
        # Preserve the resolved RAW path before the RAW-failure branch
        # below rewrites ``image_path`` to the companion. The display
        # cache-hit check on the next request reconstructs both live
        # sources and pegs against their max, so passing only the
        # companion into ``peg_display_cache_mtime`` would fail the
        # gate whenever the RAW is newer and re-extract on every hit.
        self.raw_source_path = self.image_path
        if has_current_raw_failure:
            if (
                self.resolved_ext in RAW_EXTENSIONS
                and self.companion_for_extraction
                and self.companion_for_extraction != self.image_path
            ):
                log.info(
                    "RAW working-copy extraction already failed for photo %s; "
                    "serving full-size companion JPEG %s for original",
                    original.photo_id, self.companion_for_extraction,
                )
                self.image_path = self.companion_for_extraction
                self.resolved_ext = os.path.splitext(self.image_path)[1].lower()
            else:
                return True
        return False

    def decode(self) -> LoadedImage | None:
        """Decode the original, falling back to the companion on a RAW failure."""
        from image_loader import RAW_EXTENSIONS

        img = self._decode_original()
        if (
            img is None
            and self.resolved_ext in RAW_EXTENSIONS
            and self.companion_for_extraction
            and self.companion_for_extraction != self.image_path
        ):
            from image_loader import load_image

            log.info(
                "RAW decode failed for photo %s original; falling back to "
                "companion JPEG", self.original.photo_id,
            )
            img = load_image(self.companion_for_extraction, max_size=None)
            if img is not None:
                self.image_path = self.companion_for_extraction
        return img

    def _decode_original(self) -> LoadedImage | None:
        from image_loader import (
            RAW_DECODE_CAMERA_RENDERED,
            RAW_EXTENSIONS,
            load_image,
        )

        photo = self.original.photo
        companion = self.companion_for_extraction
        raw_decode = (
            RAW_DECODE_CAMERA_RENDERED
            if self.resolved_ext in RAW_EXTENSIONS
            else None
        )
        load_kwargs = {"raw_decode": raw_decode} if raw_decode else {}
        img = load_image(self.image_path, max_size=None, **load_kwargs)
        if (
            img is not None
            and self.resolved_ext in RAW_EXTENSIONS
            and companion
            and companion != self.image_path
            and photo["width"]
            and photo["height"]
        ):
            # Same undersized-embedded-JPEG guard as the working-copy path
            # above: if rawpy.postprocess fell back to a small embedded
            # preview, prefer the full-size companion JPEG before caching.
            expected_w, expected_h = _scaled_recipe_source_dimensions(photo)
            if _image_is_smaller_than_expected(img, expected_w, expected_h):
                companion_img = load_image(
                    companion, max_size=None,
                )
                if _companion_image_can_replace_raw_result(
                    companion_img, img, expected_w, expected_h,
                ):
                    log.info(
                        "RAW decode for photo %s original returned "
                        "undersized embedded preview (%dx%d, expected "
                        "%dx%d); falling back to companion JPEG",
                        self.original.photo_id, img.size[0], img.size[1],
                        expected_w, expected_h,
                    )
                    img.close()
                    img = companion_img
                    self.image_path = companion
                elif companion_img is not None:
                    companion_img.close()
        return img


class OriginalExtraction:
    """The on-demand rendition an unedited ``/original`` cache miss extracts.

    Built at the point the route decides to extract. A RAW primary renders
    to its display cache (``wc_abs``, with no catalogued ``wc_rel``); any
    other primary renders to ``working/<id>.jpg``. Publication runs inside
    ``publication`` and, for a RAW, pegs the display cache's mtime.
    """

    def __init__(self, original: OriginalPhoto, source: UneditedSource, publication: Publication) -> None:
        import config as cfg
        from image_loader import (
            RAW_DECODE_CAMERA_RENDERED,
            RAW_DECODE_PRESERVE_HIGHLIGHTS,
        )

        self.original = original
        self.source = source
        self._publication = publication
        self.wc_rel: str | None
        self.wc_abs: str
        if original.primary_is_raw:
            self.wc_rel = None
            self.wc_abs = cast("str", source.display_cache_path)  # set for every RAW primary
            os.makedirs(os.path.dirname(self.wc_abs), exist_ok=True)
        else:
            self.wc_rel = f"working/{original.photo_id}.jpg"
            self.wc_abs = os.path.join(original.vireo_dir, self.wc_rel)
        working_copy_config = cfg.load()
        self.quality = working_copy_config.get("working_copy_quality", 92)

        # Unedited RAW primaries use their camera-rendered full-size preview
        # when available. Edited renders took the recipe branch above and keep
        # using the highlight-preserving RAW path.
        self.source_for_extraction = source.companion_for_extraction or source.image_path

        self.extraction_decode = (
            RAW_DECODE_CAMERA_RENDERED
            if original.primary_is_raw
            else RAW_DECODE_PRESERVE_HIGHLIGHTS
        )

    def extract_to_private_tmp(self, source_path: str) -> str | None:
        """Encode ``source_path`` to a per-request private tempfile.

        Returns the tempfile path on success or ``None`` on failure —
        publishing (moving the bytes to ``wc_abs``) is the caller's
        decision. The non-cacheable branch skips publication entirely
        and streams the tempfile directly, so concurrent waiters —
        each with their own tempfile — cannot race to remove each
        other's canonical file out from under ``send_file``. When
        multiple producers miss the cache at once (single-flight
        waiters wake as new producers after the first producer
        finishes) each still writes its own bytes to a distinct path,
        so their ``extract_working_copy`` calls never interleave into a
        truncated file that ``PIL.Image.open`` later 500s on.
        """
        from image_loader import extract_working_copy

        output_dir = os.path.dirname(self.wc_abs)
        os.makedirs(output_dir, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{self.original.photo_id}.render.",
            suffix=".jpg.tmp",
            dir=output_dir,
        )
        os.close(fd)
        try:
            extracted = extract_working_copy(
                source_path,
                tmp_path,
                max_size=0,
                quality=self.quality,
                raw_decode=self.extraction_decode,
            )
        except BaseException:  # broad: remove the unpublished temporary render on any failure, then re-raise
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            raise
        if not extracted:
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            return None
        return tmp_path

    def publish(self, tmp_path: str) -> None:
        """Atomically move ``tmp_path`` to ``wc_abs`` (and peg RAW mtime).

        Callers pass a path returned from ``extract_to_private_tmp``
        and must stop referring to it after this call — the tempfile
        no longer exists at that path. The RAW branch pegs the display
        cache mtime so the source-mtime cache-hit check does not
        re-decode on every request under clock skew or preserved-
        forward archive timestamps.
        """
        try:
            with self._publication():
                replace_file(tmp_path, self.wc_abs)
                if self.original.primary_is_raw:
                    # The display cache-hit check compares against
                    # ``max(mtime(image_path), mtime(companion))``.
                    # Leaving wall-clock mtime here fails that check
                    # whenever either live source has a future mtime
                    # (clock skew, archives that preserve future
                    # timestamps), and every request re-decodes the RAW
                    # and companion. Peg to the same max the check
                    # consults. Use ``raw_source_path`` — the RAW resolved
                    # before the has_current_raw_failure branch rewrote
                    # ``image_path`` to the companion — so a newer RAW
                    # mtime doesn't fail the gate and re-extract on every
                    # hit.
                    peg_display_cache_mtime(
                        self.wc_abs,
                        (self.source.raw_source_path, self.source.companion_for_extraction),
                    )
        except BaseException:  # broad: remove the unpublished temporary render on any failure, then re-raise
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            raise

    def replace_undersized_extraction(
        self, tmp_path: str | None, uw: int, uh: int,
    ) -> tuple[str | None, int, int]:
        """Re-extract from the companion when the RAW rendition is small.

        For RAW sources, extract_working_copy can succeed via the
        embedded JPEG fallback when libraw can't demosaic the file.
        That preview is often a fraction of the sensor's full
        resolution, so persisting it as the working copy would
        silently downgrade /original for every later request. Try
        the companion JPEG instead when one can satisfy the full
        size before recording this wc.
        """
        from image_loader import RAW_EXTENSIONS
        from PIL import Image as _PILImage

        photo = self.original.photo
        if (
            self.source.resolved_ext in RAW_EXTENSIONS
            and self.source.companion_for_extraction
            and self.source.companion_for_extraction != self.source_for_extraction
            and photo["width"]
            and photo["height"]
        ):
            expected_w, expected_h = _scaled_recipe_source_dimensions(photo)
            if (
                expected_w > 0
                and expected_h > 0
                and (
                    uw + 1 < expected_w
                    or uh + 1 < expected_h
                )
            ):
                log.info(
                    "RAW working copy for photo %s is undersized "
                    "(%dx%d, expected %dx%d); re-extracting "
                    "from companion JPEG",
                    self.original.photo_id, uw, uh, expected_w, expected_h,
                )
                companion_tmp = self.extract_to_private_tmp(
                    self.source.companion_for_extraction,
                )
                if companion_tmp:
                    if self.original.primary_is_raw:
                        # RAW display cache: publish the companion bytes
                        # directly to ``wc_abs`` (overwriting the RAW
                        # extraction we published above). The subsequent
                        # size peek reads from the published path.
                        self.publish(companion_tmp)
                        with _PILImage.open(self.wc_abs) as upgraded:
                            uw, uh = upgraded.size
                    else:
                        # Non-RAW: neither the primary nor the companion
                        # rendition has been published yet. Discard the
                        # undersized primary tempfile and hand the
                        # companion bytes to ``_serve_generated_original``
                        # instead so the cacheable/transient decision
                        # runs against them.
                        if tmp_path:
                            with contextlib.suppress(OSError):
                                os.unlink(tmp_path)
                        tmp_path = companion_tmp
                        with _PILImage.open(tmp_path) as upgraded:
                            uw, uh = upgraded.size
                else:
                    log.warning(
                        "Companion re-extraction failed for photo %s; "
                        "keeping undersized RAW working copy", self.original.photo_id,
                    )
        return tmp_path, uw, uh

    def commit_generated_original(self, uw: int, uh: int, *, tracked: bool) -> None:
        photo = self.original.photo
        self.original.db.photos.record_generated_original(
            self.original.photo_id,
            self.wc_rel,
            tracked=tracked,
            dimensions=(
                (uw, uh) if not photo["width"] or not photo["height"] else None
            ),
        )

    def commit_untracked_generated_original(self, uw: int, uh: int) -> None:
        # A transient full-resolution response must not orphan
        # an existing capped working copy that remains useful
        # to previews/edits/exports and still counts toward the
        # quota. Revalidate both its catalog row and file while
        # holding the publication/eviction lock. The request's
        # ``photo`` snapshot may predate a concurrent eviction;
        # restoring that stale path would point consumers at a
        # file that no longer exists.
        # Only non-RAW primaries get here, so ``wc_rel`` is never None.
        preserve_existing_copy = bool(
            self.original.db.photos.working_copy_path(self.original.photo_id) == self.wc_rel
            and os.path.isfile(self.wc_abs)
        )
        self.commit_generated_original(
            uw, uh, tracked=preserve_existing_copy,
        )
