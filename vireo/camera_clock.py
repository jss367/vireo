"""Detect camera clocks that were wrong when photos were taken.

A camera writes its own clock time into every frame, so a clock set wrong, or a
time-zone setting changed mid-shoot, puts photos out of order in every view
sorted by capture time. Independent records let Vireo notice:

* **Shot order.** Cameras number frames (the DCF file number, 0001-9999,
  wrapping) and some count shutter actuations. Clock time must not run
  backwards in that order; a backward step of more than
  ``JUMP_THRESHOLD_MINUTES`` is a *jump*.
* **Recorded time zone.** Most cameras also record their UTC offset, and they
  keep their internal UTC when the time-zone setting changes. Two frames shot
  one after the other whose offsets differ by more than the time between them
  (by the camera's UTC) were taken in the same place, since nobody crosses
  three time zones in fourteen minutes: a *switch* of the setting, not travel.
* **GPS time.** A GPS fix carries UTC; comparing it with the camera's UTC
  measures how far the camera's clock was off. A phone-linked camera often
  reuses one fix for many frames, so each fix is judged by its earliest frame.
* **Location.** The local offset at a frame's GPS position on that date (from
  a ``zone_lookup``) is what the camera should have recorded.

One clock error applies to a camera across a folder, so each group of frames
that recorded the same offset gets one correction: set the offset to the local
zone and move the clock time by ``local - recorded - clock_error``. When the
clock error cannot be measured, both readings are offered (the clock showed
local time, or the moment of capture was right) instead of guessing.

This module only reasons about values it is given; callers read EXIF. The
import preview passes the frame numbers, clock times and offsets it already
reads for duplicate detection (jumps and switches only). The Audit's Camera
Clocks check passes everything the catalog stored, one folder at a time.
"""

from __future__ import annotations

import math
import os
import re
from collections import Counter, defaultdict
from collections.abc import Callable, Hashable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

# A backward step smaller than this is not reported: clock changes that move
# photos out of order are time-zone changes (30 minutes or more) or resets.
JUMP_THRESHOLD_MINUTES = 10

# A camera clock that is off by less than this against GPS is not reported.
CLOCK_ERROR_MIN_MINUTES = 15
# At least this many distinct GPS fixes must agree on the clock error.
GPS_MIN_FIXES = 3
# Fixes agree when their errors lie within a window this wide.
GPS_AGREEMENT_MINUTES = 10
# A measured clock error within this many minutes of a whole quarter hour is
# taken to be that quarter hour: the rest is phone-clock and fix-time noise.
GPS_SNAP_MINUTES = 5

DCF_NUMBER_SPAN = 9999

# DCF names are four characters followed by a four-digit file number
# (``_D851777``, ``DSC_0042``, ``IMG_1234``). Other names (exported copies,
# renamed files) carry no reliable shot order and are left out of it.
_DCF_STEM_RE = re.compile(r"^([A-Za-z0-9_]{4})(\d{4})$")
_OFFSET_RE = re.compile(r"^([+-])(\d{2}):(\d{2})$")

# (latitude, longitude, approximate UTC) -> (zone name, offset minutes) or None.
ZoneLookup = Callable[[float, float, datetime], "tuple[str, int] | None"]


@dataclass(frozen=True)
class ClockFrame:
    """One photo's clock evidence.

    ``ref`` identifies the photo to the caller (a photo id or a source path).
    ``captured`` is the camera's clock time, naive. ``offset_minutes`` is the
    UTC offset the camera recorded, if any. ``camera`` groups frames that share
    one clock (a serial number when known). ``gps_utc`` is the GPS fix time.
    """

    ref: Hashable
    filename: str
    captured: datetime
    offset_minutes: int | None = None
    camera: str | None = None
    shutter_count: int | None = None
    gps_utc: datetime | None = None
    latitude: float | None = None
    longitude: float | None = None

    def instant(self) -> datetime | None:
        """The camera's own idea of UTC, or None without a recorded offset."""
        if self.offset_minutes is None:
            return None
        return self.captured - timedelta(minutes=self.offset_minutes)


@dataclass(frozen=True)
class ClockJump:
    """A place in shot order where the clock time ran backwards."""

    before: ClockFrame
    after: ClockFrame
    minutes_back: float
    # "zone": the camera's UTC still moved forward (its offset changed).
    # "reset": its UTC went backwards too, or no offsets were recorded.
    kind: str
    # The frames shot before the jump, back to the previous jump.
    earlier: tuple[ClockFrame, ...]


@dataclass(frozen=True)
class ZoneSwitch:
    """Consecutive frames whose offsets differ by more than the time between them."""

    before: ClockFrame
    after: ClockFrame
    gap_minutes: float


@dataclass
class ClockFix:
    """A correction for one group of photos, applied by the capture-time job.

    ``frames`` are in shot order where it is known. ``target_offset`` None
    keeps each photo's recorded offset (a plain clock shift); otherwise the
    offset tags are set to it. ``shift_minutes`` moves
    the clock time. Fixes sharing a ``group`` are alternatives for the same
    photos: apply at most one. ``recommended`` marks the one the evidence
    supports when there are several.
    """

    frames: list[ClockFrame]
    group: str
    recorded_offset: int | None
    target_offset: int | None
    shift_minutes: int
    explanation: str
    recommended: bool = True

    def label(self) -> str:
        target, recorded, shift = self.target_offset, self.recorded_offset, self.shift_minutes
        if target is not None and target != recorded:
            if shift == 0:
                return f"Set time zone to {format_offset(target)}, keep clock time"
            if recorded is not None and shift == target - recorded:
                return f"Set time zone to {format_offset(target)}, keep moment of capture"
            return (f"Set time zone to {format_offset(target)}, move clock "
                    f"{_direction(shift)} {format_minutes(shift)}")
        return f"Move clock {_direction(shift)} {format_minutes(shift)}"

    def to_json(self, include_refs: bool) -> dict[str, Any]:
        data: dict[str, Any] = {
            "group": self.group,
            "count": len(self.frames),
            "first": self.frames[0].filename,
            "last": self.frames[-1].filename,
            "recorded_offset": format_offset(self.recorded_offset),
            "target_offset": format_offset(self.target_offset),
            "shift_minutes": self.shift_minutes,
            "label": self.label(),
            "explanation": self.explanation,
            "recommended": self.recommended,
        }
        if include_refs:
            data["refs"] = [f.ref for f in self.frames]
        return data


@dataclass
class ClockProblem:
    """What one camera's clock got wrong, the evidence, and the fixes."""

    camera: str | None
    summary: str
    evidence: list[str]
    fixes: list[ClockFix]
    photo_count: int
    jump_count: int = 0

    def to_json(self, include_refs: bool = True) -> dict[str, Any]:
        return {
            "camera": self.camera,
            "summary": self.summary,
            "evidence": list(self.evidence),
            "fixes": [f.to_json(include_refs) for f in self.fixes],
            "photo_count": self.photo_count,
            "jump_count": self.jump_count,
        }


# -- formatting and parsing -----------------------------------------------------


def parse_offset(value: object) -> int | None:
    """``'-07:00'`` -> -420; anything else -> None."""
    if value is None:
        return None
    match = _OFFSET_RE.match(str(value).strip())
    if not match:
        return None
    sign, hours, minutes = match.groups()
    total = int(hours) * 60 + int(minutes)
    if int(minutes) > 59 or total > 14 * 60:
        return None
    return -total if sign == "-" else total


def format_offset(minutes: int | None) -> str | None:
    if minutes is None:
        return None
    sign = "-" if minutes < 0 else "+"
    hours, mins = divmod(abs(int(minutes)), 60)
    return f"{sign}{hours:02d}:{mins:02d}"


def format_minutes(minutes: float) -> str:
    """A duration for people: ``115`` -> ``'1 h 55 min'``."""
    total = int(round(abs(minutes)))
    hours, mins = divmod(total, 60)
    if hours and mins:
        return f"{hours} h {mins} min"
    if hours:
        return f"{hours} h"
    return f"{mins} min"


def _gap_text(minutes: float) -> str:
    return "under a minute apart" if minutes < 1 else f"{format_minutes(minutes)} apart"


def _direction(shift: float) -> str:
    return "forward" if shift > 0 else "back"


def _plural(count: int, word: str, plural: str | None = None) -> str:
    return f"{count} {word}" if count == 1 else f"{count} {plural or word + 's'}"


def dcf_frame_number(filename: str) -> tuple[str, int] | None:
    """``'_D851777.NEF'`` -> ``('_D85', 1777)``; None for non-DCF names."""
    stem = os.path.splitext(os.path.basename(filename))[0]
    match = _DCF_STEM_RE.match(stem)
    if not match:
        return None
    number = int(match.group(2))
    if number == 0:
        return None
    return match.group(1).upper(), number


# -- shot order -----------------------------------------------------------------


def _rotate_at_wrap(frames: list[tuple[int, ClockFrame]]) -> list[ClockFrame]:
    """Order DCF-numbered frames, starting after the numbering wrapped.

    Numbers run 0001-9999 and then start over, so a card holding 9685-9999
    and 0001-1848 was shot from 9685. The sequence starts after the largest
    circular gap between used numbers: the range the camera did not use.
    """
    frames.sort(key=lambda item: (item[0], item[1].filename))
    numbers = [n for n, _ in frames]
    if len(numbers) < 2:
        return [f for _, f in frames]
    best_gap, start = numbers[0] + DCF_NUMBER_SPAN - numbers[-1], 0
    for i in range(1, len(numbers)):
        gap = numbers[i] - numbers[i - 1]
        if gap > best_gap:
            best_gap, start = gap, i
    rotated = frames[start:] + frames[:start]
    return [f for _, f in rotated]


def shot_sequences(frames: Iterable[ClockFrame]) -> list[list[ClockFrame]]:
    """Order one camera's frames by when they were shot.

    Frames with a shutter count are ordered by it. The rest are ordered by DCF
    file number within one name prefix (a card can hold more than one camera,
    each with its own prefix). Frames with neither are left out: nothing says
    where they fall.
    """
    by_counter: list[ClockFrame] = []
    by_prefix: dict[str, list[tuple[int, ClockFrame]]] = defaultdict(list)
    for frame in frames:
        if frame.shutter_count is not None:
            by_counter.append(frame)
            continue
        dcf = dcf_frame_number(frame.filename)
        if dcf is not None:
            by_prefix[dcf[0]].append((dcf[1], frame))
    sequences = [sorted(by_counter, key=lambda f: (f.shutter_count or 0, f.filename))]
    sequences.extend(_rotate_at_wrap(group) for group in by_prefix.values())
    return [s for s in sequences if len(s) >= 2]


def find_jumps(sequence: Sequence[ClockFrame],
               threshold_minutes: float = JUMP_THRESHOLD_MINUTES) -> list[ClockJump]:
    """Every backward step of the clock time in a shot-ordered sequence.

    Each frame is compared with the latest time seen since the previous jump,
    so a burst whose sub-second times interleave is not a jump.
    """
    jumps: list[ClockJump] = []
    segment: list[ClockFrame] = []
    latest: ClockFrame | None = None
    latest_instant: datetime | None = None
    for frame in sequence:
        if latest is not None:
            back = (latest.captured - frame.captured).total_seconds() / 60
            if back > threshold_minutes:
                instant = frame.instant()
                kind = "reset"
                if (
                    instant is not None and latest_instant is not None
                    and (latest_instant - instant).total_seconds() / 60 <= threshold_minutes
                ):
                    kind = "zone"
                jumps.append(ClockJump(
                    before=segment[-1], after=frame, minutes_back=back,
                    kind=kind, earlier=tuple(segment),
                ))
                segment, latest, latest_instant = [], None, None
        segment.append(frame)
        if latest is None or frame.captured > latest.captured:
            latest = frame
        instant = frame.instant()
        if instant is not None and (latest_instant is None or instant > latest_instant):
            latest_instant = instant
    return jumps


def find_zone_switches(sequence: Sequence[ClockFrame]) -> list[ZoneSwitch]:
    """Consecutive frames whose time-zone setting changed faster than travel could."""
    switches = []
    for a, b in zip(sequence, sequence[1:], strict=False):
        if a.offset_minutes is None or b.offset_minutes is None or a.offset_minutes == b.offset_minutes:
            continue
        a_instant, b_instant = a.instant(), b.instant()
        if a_instant is None or b_instant is None:
            continue
        gap = abs((b_instant - a_instant).total_seconds()) / 60
        if gap < abs(a.offset_minutes - b.offset_minutes):
            switches.append(ZoneSwitch(before=a, after=b, gap_minutes=gap))
    return switches


# -- GPS and location -------------------------------------------------------------


def estimate_clock_error(frames: Iterable[ClockFrame]) -> tuple[int, int] | None:
    """How far the camera's UTC was from GPS UTC, in minutes, and on how many fixes.

    Negative means the camera was behind. Frames sharing one GPS time share one
    fix, judged by its earliest frame (a reused fix only adds delay). The
    estimate is the lower quartile of the densest ``GPS_AGREEMENT_MINUTES``
    window of fixes, snapped to a quarter hour within ``GPS_SNAP_MINUTES``.
    None unless ``GPS_MIN_FIXES`` fixes fall in that window.
    """
    per_fix: dict[datetime, float] = {}
    for frame in frames:
        instant = frame.instant()
        if instant is None or frame.gps_utc is None:
            continue
        error = (instant - frame.gps_utc).total_seconds() / 60
        if frame.gps_utc not in per_fix or error < per_fix[frame.gps_utc]:
            per_fix[frame.gps_utc] = error
    errors = sorted(per_fix.values())
    best: list[float] = []
    for low in errors:
        window = [e for e in errors if low <= e <= low + GPS_AGREEMENT_MINUTES]
        if len(window) > len(best):
            best = window
    if len(best) < GPS_MIN_FIXES:
        return None
    value = best[len(best) // 4]
    snapped = int(round(value / 15) * 15)
    estimate = snapped if abs(value - snapped) <= GPS_SNAP_MINUTES else int(round(value))
    return estimate, len(best)


def _most_common(values: Iterable[Any]) -> Any:
    counts = Counter(v for v in values if v is not None)
    return counts.most_common(1)[0][0] if counts else None


# -- one camera -------------------------------------------------------------------


@dataclass
class _CameraClock:
    """Everything known about one camera's clock over a set of frames."""

    frames: list[ClockFrame]
    jumps: list[ClockJump] = field(default_factory=list)
    switches: list[ZoneSwitch] = field(default_factory=list)
    # Minutes the camera's UTC was off from GPS; None when unmeasured.
    clock_error: int | None = None
    error_fixes: int = 0
    zones: dict[Hashable, tuple[str, int]] = field(default_factory=dict)
    sequences: list[list[ClockFrame]] = field(default_factory=list)
    # One entry per stretch between clock resets, when there are resets:
    # (first frame, last frame, clock error or None, GPS fixes it rests on).
    epoch_errors: list[tuple[ClockFrame, ClockFrame, int | None, int]] = field(default_factory=list)
    _groups: dict[int | None, list[ClockFrame]] = field(default_factory=dict)
    _parent: dict[int, int] = field(default_factory=dict)
    _position: dict[Hashable, tuple[int, int]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for frame in self.frames:
            self._groups.setdefault(frame.offset_minutes, []).append(frame)

    def add_sequence(self, sequence: Sequence[ClockFrame]) -> None:
        index = len({p[0] for p in self._position.values()})
        for position, frame in enumerate(sequence):
            self._position.setdefault(frame.ref, (index, position))
        self.sequences.append(list(sequence))
        self.jumps.extend(find_jumps(sequence))
        self.switches.extend(find_zone_switches(sequence))

    def in_shot_order(self, frames: Iterable[ClockFrame]) -> list[ClockFrame]:
        """Frames in shot order; frames outside every sequence last, by name."""
        unplaced = (1 << 30, 0)
        return sorted(frames, key=lambda f: (self._position.get(f.ref, unplaced), f.filename))

    def epochs(self) -> list[list[ClockFrame]]:
        """The frames split, in shot order, wherever the clock itself was set back.

        A reset changes how far the clock is off, so each stretch between
        resets has its own clock error. Frames outside every shot sequence
        form one last stretch.
        """
        starts = {j.after.ref for j in self.jumps if j.kind == "reset"}
        epochs: list[list[ClockFrame]] = []
        placed: set[Hashable] = set()
        for sequence in self.sequences:
            current: list[ClockFrame] = []
            for frame in sequence:
                if frame.ref in starts and current:
                    epochs.append(current)
                    current = []
                current.append(frame)
                placed.add(frame.ref)
            if current:
                epochs.append(current)
        rest = [f for f in self.frames if f.ref not in placed]
        if rest:
            epochs.append(rest)
        return epochs

    def part(self, frames: list[ClockFrame], use_gps: bool) -> _CameraClock:
        """The same camera over one stretch between resets, with its own clock error."""
        refs = {f.ref for f in frames}
        part = _CameraClock(frames=frames, zones={r: z for r, z in self.zones.items() if r in refs})
        part._position = self._position
        part.switches = [s for s in self.switches if s.before.ref in refs and s.after.ref in refs]
        part.link_switches()
        if use_gps:
            _measure(part)
        return part

    def offset_groups(self) -> list[tuple[int, list[ClockFrame]]]:
        return sorted(((o, g) for o, g in self._groups.items() if o is not None), key=lambda kv: kv[0])

    def _root(self, offset: int) -> int:
        while self._parent.get(offset, offset) != offset:
            offset = self._parent[offset]
        return offset

    def link_switches(self) -> None:
        for switch in self.switches:
            a = self._root(switch.before.offset_minutes or 0)
            b = self._root(switch.after.offset_minutes or 0)
            if a != b:
                self._parent[a] = b

    def same_place(self, offset: int) -> list[int]:
        """Other recorded offsets a switch shows were used in the same place."""
        root = self._root(offset)
        return [o for o, _ in self.offset_groups() if o != offset and self._root(o) == root]

    def own_zone(self, offset: int) -> tuple[str, int] | None:
        return _most_common(self.zones.get(f.ref) for f in self._groups.get(offset, []))

    def group_zone(self, offset: int) -> tuple[tuple[str, int], int | None] | None:
        """The local zone for a group and, when borrowed, the offset it came from."""
        own = self.own_zone(offset)
        if own is not None:
            return own, None
        for other in self.same_place(offset):
            zone = self.own_zone(other)
            if zone is not None:
                return zone, other
        return None

    def component_majority(self, offset: int) -> int:
        """The offset most frames in this same-place group recorded.

        Ties go to the setting recorded later in shot order: the setting the
        camera ended up with is the more likely to have been corrected.
        """
        members = [offset, *self.same_place(offset)]
        order = {s.after.offset_minutes: i for i, s in enumerate(self.switches)}
        return max(members, key=lambda o: (len(self._groups[o]), order.get(o, -1)))


def _zone_text(zone: tuple[str, int], borrowed_from: int | None) -> str:
    """Why ``zone`` is the local time zone, ending in its offset."""
    if borrowed_from is None:
        return f"local time at their GPS position ({zone[0]}) was {format_offset(zone[1])}"
    return (f"the {format_offset(borrowed_from)} photos shot minutes apart have GPS positions in "
            f"{zone[0]}, where local time was {format_offset(zone[1])}")


def _measured_fix(clock: _CameraClock, recorded: int, group: list[ClockFrame],
                  zone: tuple[str, int] | None, borrowed: int | None) -> ClockFix | None:
    """The one correction for a group when the clock error is measured.

    Without a local zone, a group the camera switched away from minutes
    before or after other photos is restated in the offset most of them
    recorded, so the groups end up consistent with each other.
    """
    error = clock.clock_error or 0
    majority = clock.component_majority(recorded) if zone is None else recorded
    target = zone[1] if zone is not None else majority
    shift = target - recorded - error
    if shift == 0 and target == recorded:
        return None
    parts = []
    if zone is not None and target != recorded:
        parts.append(f"The camera recorded {format_offset(recorded)}, but {_zone_text(zone, borrowed)}.")
    elif target != recorded:
        parts.append(f"The camera recorded {format_offset(recorded)} minutes from photos that recorded "
                     f"{format_offset(target)}, most of this camera's photos here. Vireo cannot tell the "
                     f"local time zone, so check that {format_offset(target)} is right.")
    if error:
        parts.append(f"GPS time shows its clock ran {format_minutes(error)} "
                     f"{'behind' if error < 0 else 'ahead'}.")
    if shift:
        parts.append(f"Moving the clock {_direction(shift)} {format_minutes(shift)} "
                     f"{'and setting the time zone to ' + str(format_offset(target)) + ' ' if target != recorded else ''}"
                     "makes both the clock time and the moment of capture right.")
    else:
        parts.append(f"Together these mean the clock time was right; only the time zone changes, "
                     f"to {format_offset(target)}.")
    return ClockFix(
        frames=clock.in_shot_order(group), group=str(format_offset(recorded)),
        recorded_offset=recorded, target_offset=target if target != recorded else None,
        shift_minutes=shift, explanation=" ".join(parts),
    )


def _unmeasured_fixes(clock: _CameraClock, recorded: int, group: list[ClockFrame], zone: tuple[str, int],
                      borrowed: int | None, nearby_error: tuple[int, str] | None) -> list[ClockFix]:
    """Both readings of a wrong time zone when GPS cannot say which is right."""
    target = zone[1]
    frames = clock.in_shot_order(group)
    head = f"The camera recorded {format_offset(recorded)}, but {_zone_text(zone, borrowed)}."
    options = [
        (0, f"{head} If the clock showed the right local time, only the time zone is wrong."),
        (target - recorded,
         f"{head} If the camera simply was not switched to this time zone, its moment of capture was "
         f"right and the clock time moves {_direction(target - recorded)} "
         f"{format_minutes(target - recorded)}."),
    ]
    predicted = None
    if nearby_error is not None:
        predicted = target - recorded - nearby_error[0]
        if predicted not in (0, target - recorded):
            options.append((predicted,
                            f"{head} This camera's clock was {format_minutes(nearby_error[0])} "
                            f"{'behind' if nearby_error[0] < 0 else 'ahead of'} GPS on {nearby_error[1]}; "
                            f"if it still was, the clock time moves {_direction(predicted)} "
                            f"{format_minutes(predicted)}."))
    fixes = []
    for shift, text in options:
        recommended = predicted == shift
        if recommended and nearby_error is not None and len(options) == 2:
            text += (f" This matches the camera's clock on {nearby_error[1]}, when GPS showed it "
                     + ("right." if nearby_error[0] == 0 else
                        f"{format_minutes(nearby_error[0])} {'behind' if nearby_error[0] < 0 else 'ahead'}."))
        fixes.append(ClockFix(
            frames=frames, group=str(format_offset(recorded)), recorded_offset=recorded,
            target_offset=target, shift_minutes=shift, explanation=text, recommended=recommended,
        ))
    return fixes


def _restate_fix(clock: _CameraClock, recorded: int, group: list[ClockFrame]) -> ClockFix | None:
    """Restate a group in the offset the rest of its same-place photos used."""
    target = clock.component_majority(recorded)
    if target == recorded:
        return None
    return ClockFix(
        frames=clock.in_shot_order(group), group=str(format_offset(recorded)),
        recorded_offset=recorded, target_offset=target, shift_minutes=target - recorded,
        explanation=(
            f"These photos were shot minutes from photos that recorded {format_offset(target)}, "
            f"most of this camera's photos here. Restating them in {format_offset(target)} keeps "
            f"their moment of capture and moves their clock time {_direction(target - recorded)} "
            f"{format_minutes(target - recorded)}. Vireo has no GPS position for them, so check "
            f"that {format_offset(target)} is the local time zone where they were taken."
        ),
    )


def _reset_fix(jump: ClockJump, frames: list[ClockFrame], other_shift: int, *, move_before: bool) -> ClockFix:
    """Put the frames on one side of a reset back in order with the other side.

    ``move_before`` moves the frames shot before the reset back past the first
    frame after it; otherwise the frames after it move forward past the last
    frame before it. ``other_shift`` is how far another fix moves that other
    side, so both fixes together keep shot order.
    """
    # Whole minutes strictly past the jump: a shift of exactly the jump would
    # leave the two frames on the same time, which a capture-time sort can
    # still order either way.
    step = math.floor(jump.minutes_back) + 1
    reset = -step if move_before else step
    shift = reset + other_shift
    anchor = jump.after if move_before else jump.before
    explanation = (
        f"The clock was set back between {jump.before.filename} and {jump.after.filename}. "
        f"Moving {frames[0].filename}-{frames[-1].filename} {_direction(reset)} {format_minutes(reset)} "
        f"is the smallest shift that puts them {'before' if move_before else 'after'} {anchor.filename}; "
        "the real gap between the two frames is unknown, so check the preview against what you remember."
    )
    if not move_before:
        explanation += " GPS time shows the clock was right before the reset, so these are the photos to move."
    if other_shift:
        explanation += (
            f" The fix for {anchor.filename} moves it {_direction(other_shift)} "
            f"{format_minutes(other_shift)}, so these move "
            + (f"{_direction(shift)} {format_minutes(shift)} in all." if shift else "nowhere in all.")
            + " Apply both."
        )
    return ClockFix(
        frames=frames, group=f"{'before' if move_before else 'from'} {jump.after.filename}",
        recorded_offset=None, target_offset=None, shift_minutes=shift, explanation=explanation,
    )


def _fixes_across_resets(clock: _CameraClock, nearby_error: tuple[int, str] | None,
                         use_gps: bool) -> list[ClockFix]:
    """Fixes when the clock itself was set back somewhere in these frames.

    A reset changes the clock error, so GPS is measured separately on each
    stretch between resets and each stretch gets its own fixes. Around each
    reset, a stretch neither GPS nor any other fix settles moves past the
    other side, composed with whatever that side's fix moves it by, so
    applying both keeps shot order.
    """
    fixes: list[ClockFix] = []
    shifts: dict[Hashable, int] = {}
    settled: set[Hashable] = set()
    stretch_of: dict[Hashable, list[ClockFrame]] = {}
    for epoch in clock.epochs():
        part = clock.part(epoch, use_gps)
        clock.epoch_errors.append((epoch[0], epoch[-1], part.clock_error, part.error_fixes))
        part_fixes = _fixes(part, nearby_error)
        fixes.extend(part_fixes)
        for fix in part_fixes:
            if fix.recommended:
                shifts.update({f.ref: fix.shift_minutes for f in fix.frames})
        if part.clock_error is not None or part_fixes:
            settled.update(f.ref for f in epoch)
        stretch_of.update({f.ref: epoch for f in epoch})
    for jump in clock.jumps:
        if jump.kind != "reset":
            continue
        before = [f for f in stretch_of[jump.before.ref] if f.ref not in settled]
        after = [f for f in stretch_of[jump.after.ref] if f.ref not in settled]
        if before:
            fix = _reset_fix(jump, before, shifts.get(jump.after.ref, 0), move_before=True)
        elif after:
            fix = _reset_fix(jump, after, shifts.get(jump.before.ref, 0), move_before=False)
        else:
            continue
        # Zero means the other side's fix alone restores the order.
        if fix.shift_minutes:
            fixes.append(fix)
        settled.update(f.ref for f in fix.frames)
    return fixes


def _fixes(clock: _CameraClock, nearby_error: tuple[int, str] | None) -> list[ClockFix]:
    fixes: list[ClockFix] = []
    for recorded, group in clock.offset_groups():
        found = clock.group_zone(recorded)
        zone, borrowed = found if found is not None else (None, None)
        if clock.clock_error is not None and (zone is not None or clock.clock_error):
            fix = _measured_fix(clock, recorded, group, zone, borrowed)
            if fix is not None:
                fixes.append(fix)
        elif zone is not None:
            if zone[1] != recorded:
                fixes.extend(_unmeasured_fixes(clock, recorded, group, zone, borrowed, nearby_error))
        elif clock.same_place(recorded):
            fix = _restate_fix(clock, recorded, group)
            if fix is not None:
                fixes.append(fix)
    return fixes


def _evidence(clock: _CameraClock) -> list[str]:
    lines = []
    if clock.jumps:
        first = clock.jumps[0]
        more = len(clock.jumps) - 1
        lines.append(
            f"The clock went back {format_minutes(first.minutes_back)} between "
            f"{first.before.filename} ({first.before.captured:%H:%M:%S}) and "
            f"{first.after.filename} ({first.after.captured:%H:%M:%S}), which was shot after it"
            + (f", and {_plural(more, 'more time')} after that." if more else ".")
        )
        order = "shutter count" if first.before.shutter_count is not None else "file number"
        lines.append(f"Shot order comes from the {order}.")
    if clock.switches:
        first_switch = clock.switches[0]
        more = len(clock.switches) - 1
        lines.append(
            f"The time zone changed from {format_offset(first_switch.before.offset_minutes)} "
            f"({first_switch.before.filename}) to {format_offset(first_switch.after.offset_minutes)} "
            f"({first_switch.after.filename}), {_gap_text(first_switch.gap_minutes)} by the "
            "camera's own clock: too fast for travel, so the setting changed while shooting"
            + (f" ({_plural(more, 'more change')} like it)." if more else ".")
        )
    elif any(j.kind == "reset" for j in clock.jumps):
        lines.append("The camera's moment of capture went backwards too, so the clock itself was set back.")
    recorded = [(o, len(g)) for o, g in clock.offset_groups()]
    if len(recorded) > 1:
        lines.append("Recorded time zones: "
                     + ", ".join(f"{format_offset(o)} ({_plural(n, 'photo')})" for o, n in recorded) + ".")
    if clock.epoch_errors:
        lines.extend(_epoch_error_lines(clock))
    elif clock.clock_error:
        lines.append(
            f"GPS time shows the camera's clock was {format_minutes(clock.clock_error)} "
            f"{'behind' if clock.clock_error < 0 else 'ahead'} ({_plural(clock.error_fixes, 'GPS fix', 'GPS fixes')} "
            "agree; a fix reused for later frames is judged by its first frame)."
        )
    elif clock.clock_error is None and clock.zones:
        lines.append("Too few GPS fixes to measure the camera's clock against GPS time.")
    zone_counts = Counter(clock.zones.values())
    if zone_counts:
        (name, offset), _ = zone_counts.most_common(1)[0]
        lines.append(f"Local time at the GPS position was {format_offset(offset)} ({name}).")
    return lines


def _epoch_error_lines(clock: _CameraClock) -> list[str]:
    lines = []
    for first, last, error, fixes in clock.epoch_errors:
        span = f"{first.filename}-{last.filename}"
        if error is None:
            lines.append(f"{span}: too few GPS fixes to measure the clock.")
        elif error:
            lines.append(f"{span}: GPS time shows the clock was {format_minutes(error)} "
                         f"{'behind' if error < 0 else 'ahead'} ({_plural(fixes, 'GPS fix', 'GPS fixes')} agree).")
        else:
            lines.append(f"{span}: GPS time agrees with the clock ({_plural(fixes, 'GPS fix', 'GPS fixes')}).")
    return lines


def _measure(clock: _CameraClock) -> None:
    """Set the clock error from GPS; an error under the reporting floor counts as none."""
    measured = estimate_clock_error(clock.frames)
    if measured is not None:
        clock.clock_error = measured[0] if abs(measured[0]) >= CLOCK_ERROR_MIN_MINUTES else 0
        clock.error_fixes = measured[1]


def _summary(clock: _CameraClock, fixes: list[ClockFix]) -> str:
    error = clock.clock_error or 0
    zone_wrong = any(f.target_offset is not None and f.target_offset != f.recorded_offset for f in fixes)
    if clock.jumps:
        if all(j.kind == "zone" for j in clock.jumps):
            return "Photos are out of shot order because the camera's time-zone setting changed"
        return "Photos are out of shot order because the camera's clock was set back"
    if clock.switches:
        return "The camera's time-zone setting changed while shooting"
    behind = "behind" if error < 0 else "ahead of"
    if error and zone_wrong:
        return f"Camera recorded the wrong time zone, and its clock was {format_minutes(error)} {behind} GPS"
    if error:
        return f"Camera clock was {format_minutes(error)} {behind} GPS time"
    return "Camera recorded the wrong time zone for where the photos were taken"


def analyze_camera(frames: Sequence[ClockFrame], *, zone_lookup: ZoneLookup | None = None,
                   use_gps: bool = True,
                   nearby_error: tuple[int, str] | None = None) -> ClockProblem | None:
    """One camera's clock problems in these frames, or None if it looks right.

    ``nearby_error`` is this camera's clock error measured on another day
    (minutes, and how to name that day). It picks between readings when
    these frames have too few GPS fixes of their own.
    """
    clock = _CameraClock(frames=list(frames))
    for sequence in shot_sequences(clock.frames):
        clock.add_sequence(sequence)
    clock.link_switches()
    if use_gps:
        _measure(clock)
        if zone_lookup is not None:
            for frame in clock.frames:
                if frame.latitude is None or frame.longitude is None:
                    continue
                approx_utc = frame.captured - timedelta(
                    minutes=(frame.offset_minutes or 0) + (clock.clock_error or 0))
                zone = zone_lookup(frame.latitude, frame.longitude, approx_utc)
                if zone is not None:
                    clock.zones[frame.ref] = zone

    resets = any(j.kind == "reset" for j in clock.jumps)
    fixes = _fixes_across_resets(clock, nearby_error, use_gps) if resets else _fixes(clock, nearby_error)
    if not fixes and not resets:
        # Every offset is right for where the photos were taken (or nothing
        # says otherwise), so a backward step in clock time was real travel.
        return None
    return ClockProblem(
        camera=clock.frames[0].camera,
        summary=_summary(clock, fixes),
        evidence=_evidence(clock),
        fixes=fixes,
        photo_count=len(clock.frames),
        jump_count=len(clock.jumps),
    )


def find_clock_problems(frames: Iterable[ClockFrame], *, zone_lookup: ZoneLookup | None = None,
                        use_gps: bool = True,
                        nearby_errors: dict[str | None, tuple[int, str]] | None = None) -> list[ClockProblem]:
    """Clock problems for each camera among ``frames``.

    ``nearby_errors`` maps a camera to its clock error measured elsewhere.
    """
    by_camera: dict[str | None, list[ClockFrame]] = defaultdict(list)
    for frame in frames:
        by_camera[frame.camera].append(frame)
    problems = []
    for camera in sorted(by_camera, key=lambda c: (c is None, c or "")):
        problem = analyze_camera(
            by_camera[camera], zone_lookup=zone_lookup, use_gps=use_gps,
            nearby_error=(nearby_errors or {}).get(camera),
        )
        if problem is not None:
            problems.append(problem)
    return problems
