"""Identity of an original file's generation and of a frame decoded from it.

A mask, a detector box or a comparison measured on an original records which
generation of the file it was made from, so that a replaced or edited original
cannot be paired with evidence from its predecessor. The generation is the
file's resolved path, inode, size and modification and change times.

The volume's device number is deliberately not part of it. macOS assigns a
network volume a new device number every time it is mounted, so an identity
that included it changed whenever the NAS reconnected, and every mask and box
saved before the reconnect stopped matching the unchanged file.

Identities written before that change did include the device number. They are
still honored: ``LegacyFrame.matches`` recomputes the old identity for device
numbers near the current one (``LEGACY_DEVICE_WINDOW``). A hit is proof that
every other part of the old identity, the path, inode, size and both times,
equals the file today, because the digest commits to all of them; only the
device number is searched.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

# macOS numbers network mounts consecutively, so a reconnect moves the device
# number by one. The window covers that many mounts in either direction.
LEGACY_DEVICE_WINDOW = 1024

# A JSON integer no real device number takes, split out of the serialized
# legacy identity so each candidate costs one string join and one digest.
_DEVICE_SENTINEL = -987654321987654321


@dataclass(frozen=True)
class SourceState:
    """One generation of an original file, as ``os.stat`` saw it."""

    realpath: str
    device: int
    inode: int
    size: int
    mtime_ns: int
    ctime_ns: int

    def stable(self) -> list[object]:
        """The parts that survive a remount of the volume."""
        return [self.realpath, self.inode, self.size, self.mtime_ns, self.ctime_ns]

    def legacy(self, device: int) -> list[object]:
        """The tuple identities written before the device number was dropped."""
        return [self.realpath, device, self.inode, self.size, self.mtime_ns, self.ctime_ns]


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value).encode()).hexdigest()


def source_generation(state: SourceState) -> str:
    """The original file's generation, independent of the volume's mount."""
    return _digest([2, state.stable()])


def frame_identity(state: SourceState, raw_decode: str, rendition: str, native_size: tuple[int, int]) -> str:
    """The unscaled rendition decoded from one generation of the original."""
    return _digest([2, raw_decode, state.stable(), rendition, list(native_size)])


def _legacy_matches(template: list[object], device: int, stored: str) -> bool:
    """Whether ``stored`` is ``template``'s legacy digest for a device near ``device``."""
    head, sep, tail = json.dumps(template).partition(str(_DEVICE_SENTINEL))
    if not sep:
        return False
    for candidate in range(device - LEGACY_DEVICE_WINDOW, device + LEGACY_DEVICE_WINDOW + 1):
        if hashlib.sha256(f"{head}{candidate}{tail}".encode()).hexdigest() == stored:
            return True
    return False


@dataclass(frozen=True)
class LegacyFrame:
    """What a decode needs to recognize an identity written with a device number."""

    state: SourceState
    raw_decode: str
    rendition: str
    native_size: tuple[int, int]

    def matches(self, stored: object) -> bool:
        return isinstance(stored, str) and _legacy_frame_matches(self, stored)


@lru_cache(maxsize=4096)
def _legacy_frame_matches(frame: LegacyFrame, stored: str) -> bool:
    template = [1, frame.raw_decode, frame.state.legacy(_DEVICE_SENTINEL), frame.rendition, list(frame.native_size)]
    return _legacy_matches(template, frame.state.device, stored)


@lru_cache(maxsize=4096)
def _legacy_generation_matches(state: SourceState, stored: str) -> bool:
    return _legacy_matches(state.legacy(_DEVICE_SENTINEL), state.device, stored)


def generation_matches(stored: object, state: SourceState) -> bool:
    """Whether a recorded generation names this generation of the file."""
    if not isinstance(stored, str) or not stored:
        return False
    return stored == source_generation(state) or _legacy_generation_matches(state, stored)


# ``Any`` keys: Pillow types ``Image.info`` with str and tuple keys.
def frame_matches(stored: object, info: Mapping[Any, object]) -> bool:
    """Whether a recorded frame identity names the frame a decode produced.

    ``info`` is the decoded image's ``info``. A working copy carries the
    identity stamped when it was extracted and no ``vireo_legacy_frame``, so it
    matches only that identity exactly.
    """
    if not isinstance(stored, str) or not stored:
        return False
    if stored == info.get("vireo_subject_frame_id"):
        return True
    legacy = info.get("vireo_legacy_frame")
    return isinstance(legacy, LegacyFrame) and legacy.matches(stored)
