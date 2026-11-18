"""Camera-clock detection: shot order, time-zone switches, GPS error, location."""

from datetime import datetime, timedelta
from itertools import pairwise

import camera_clock
from camera_clock import ClockFrame, analyze_camera, estimate_clock_error, find_clock_problems

LA = ("America/Los_Angeles", -480)


def la_winter(_lat, _lon, _utc):
    return LA


def frames_at(start, count, *, offset, first_count, step_seconds=20, gps_error=None,
              gps_every=1, prefix="_D85", camera="D850 #1", position=True):
    """``count`` frames from clock time ``start``, shutter counts from ``first_count``.

    ``gps_error`` is how far the camera's UTC is from true UTC (minutes); a
    GPS time and position are attached to every ``gps_every``-th frame.
    """
    frames = []
    for i in range(count):
        captured = start + timedelta(seconds=step_seconds * i)
        number = (first_count + i - 1) % 9999 + 1
        gps = None
        lat = lon = None
        if gps_error is not None and i % gps_every == 0:
            instant = captured - timedelta(minutes=offset)
            gps = instant - timedelta(minutes=gps_error)
            if position:
                lat, lon = 32.85, -117.0
        frames.append(ClockFrame(
            ref=f"{prefix}{number:04d}", filename=f"{prefix}{number:04d}.NEF", captured=captured,
            offset_minutes=offset, camera=camera, shutter_count=first_count + i,
            gps_utc=gps, latitude=lat, longitude=lon,
        ))
    return frames


def test_dcf_frame_number_reads_the_four_digit_file_number():
    assert camera_clock.dcf_frame_number("_D851777.NEF") == ("_D85", 1777)
    assert camera_clock.dcf_frame_number("DSC_0042.jpg") == ("DSC_", 42)
    assert camera_clock.dcf_frame_number("_D850024_2026-01-25_112239.jpg") is None
    assert camera_clock.dcf_frame_number("IMG_0000.JPG") is None


def test_shot_order_starts_after_the_file_numbers_wrapped():
    times = datetime(2026, 1, 25, 12, 0)
    frames = [
        ClockFrame(ref=n, filename=f"_D85{n:04d}.NEF", captured=times + timedelta(minutes=i))
        for i, n in enumerate([9998, 9999, 1, 2])
    ]
    [sequence] = camera_clock.shot_sequences(reversed(frames))
    assert [f.ref for f in sequence] == [9998, 9999, 1, 2]


def test_interleaved_burst_subseconds_are_not_a_jump():
    base = datetime(2026, 1, 25, 12, 0, 0)
    frames = [
        ClockFrame(ref=i, filename=f"DSC_{i + 1:04d}.NEF",
                   captured=base + timedelta(milliseconds=ms))
        for i, ms in enumerate([0, 140, 90, 280, 200])
    ]
    assert camera_clock.find_jumps(frames) == []


def test_time_zone_change_mid_shoot_is_fixed_with_gps_and_location():
    """The 2026-01-25 shoot: -05:00 frames first, then -07:00, camera 1 h behind GPS."""
    early = frames_at(datetime(2026, 1, 25, 12, 53), 30, offset=-300, first_count=108497)
    late = frames_at(datetime(2026, 1, 25, 11, 25), 60, offset=-420, first_count=108527,
                     gps_error=-60)
    problem = analyze_camera(early + late, zone_lookup=la_winter)

    assert problem is not None
    assert problem.jump_count == 1
    assert "time-zone setting changed" in problem.summary
    fixes = {f.recorded_offset: f for f in problem.fixes}
    assert set(fixes) == {-420, -300}
    # The -07:00 clock showed Pacific time; only its time zone was wrong.
    assert fixes[-420].target_offset == -480 and fixes[-420].shift_minutes == 0
    # The -05:00 frames borrow the location of the frames shot minutes later and
    # move back 2 h: 3 h of zone, less the clock's 1 h lag.
    assert fixes[-300].target_offset == -480 and fixes[-300].shift_minutes == -120
    assert len(fixes[-300].frames) == 30
    assert any("too fast for travel" in line for line in problem.evidence)
    assert any("1 h behind" in line for line in problem.evidence)


def test_import_check_restates_the_minority_zone_without_gps():
    early = frames_at(datetime(2026, 1, 25, 12, 53), 30, offset=-300, first_count=9685,
                      camera=None)
    late = frames_at(datetime(2026, 1, 25, 11, 25), 60, offset=-420, first_count=16,
                     camera=None)
    [problem] = find_clock_problems(early + late, use_gps=False)

    [fix] = problem.fixes
    assert fix.recorded_offset == -300
    assert fix.target_offset == -420
    assert fix.shift_minutes == -120
    assert fix.label() == "Set time zone to -07:00, keep moment of capture"
    payload = problem.to_json(include_refs=False)
    assert "refs" not in payload["fixes"][0]
    assert payload["fixes"][0]["first"] == "_D859685.NEF"


def test_switch_without_a_backward_jump_is_reported():
    """The 2026-05-30 shoot: still on Hawaii time, switched 14 minutes later."""
    hawaii = frames_at(datetime(2026, 5, 30, 5, 57), 20, offset=-600, first_count=100,
                       camera="Z 8 #9")
    home = frames_at(datetime(2026, 5, 30, 9, 25), 40, offset=-420, first_count=120,
                     camera="Z 8 #9", gps_error=0)

    def la_summer(_lat, _lon, _utc):
        return ("America/Los_Angeles", -420)

    problem = analyze_camera(hawaii + home, zone_lookup=la_summer)
    assert problem is not None and problem.jump_count == 0
    assert problem.summary == "The camera's time-zone setting changed while shooting"
    [fix] = problem.fixes
    assert (fix.recorded_offset, fix.target_offset, fix.shift_minutes) == (-600, -420, 180)


def test_real_travel_with_correct_zones_is_not_a_problem():
    def zone_by_longitude(_lat, lon, _utc):
        return ("Pacific/Honolulu", -600) if lon < -150 else ("America/Los_Angeles", -420)

    morning = frames_at(datetime(2026, 5, 30, 6, 0), 10, offset=-600, first_count=1, gps_error=0)
    morning = [ClockFrame(**{**f.__dict__, "longitude": -157.8 if f.longitude else None}) for f in morning]
    evening = frames_at(datetime(2026, 5, 30, 18, 0), 10, offset=-420, first_count=11, gps_error=0)
    assert analyze_camera(morning + evening, zone_lookup=zone_by_longitude) is None


def test_clock_set_back_without_offsets_suggests_a_shift():
    early = [ClockFrame(ref=i, filename=f"DSC_{i:04d}.JPG",
                        captured=datetime(2026, 1, 1, 14, 0) + timedelta(minutes=i))
             for i in range(1, 6)]
    late = [ClockFrame(ref=i, filename=f"DSC_{i:04d}.JPG",
                       captured=datetime(2026, 1, 1, 13, 0) + timedelta(minutes=i))
            for i in range(6, 10)]
    [problem] = find_clock_problems(early + late)
    assert "clock was set back" in problem.summary
    [fix] = problem.fixes
    assert fix.target_offset is None
    # 14:05 back to 13:06 is exactly 59 min; one more puts 14:05 at 13:05, before 13:06.
    assert fix.shift_minutes == -60
    assert [f.ref for f in fix.frames] == [1, 2, 3, 4, 5]


def test_clock_error_judges_a_reused_fix_by_its_first_frame():
    frames = []
    base = datetime(2026, 3, 22, 9, 0)
    for fix in range(4):
        fix_utc = base + timedelta(hours=fix) + timedelta(minutes=420 + 60)
        for k in range(5):
            # Every frame after the first reuses the fix, minutes stale.
            captured = base + timedelta(hours=fix, minutes=15 * k)
            frames.append(ClockFrame(ref=(fix, k), filename=f"x{fix}{k}", captured=captured,
                                     offset_minutes=-420, gps_utc=fix_utc))
    assert estimate_clock_error(frames) == (-60, 4)


def test_clock_error_needs_fixes_that_agree():
    base = datetime(2026, 3, 22, 9, 0)
    frames = [
        ClockFrame(ref=i, filename=str(i), captured=base + timedelta(hours=i), offset_minutes=0,
                   gps_utc=base + timedelta(hours=i, minutes=error))
        for i, error in enumerate([10, 50, 90])
    ]
    assert estimate_clock_error(frames) is None


def test_unmeasured_clock_offers_both_readings_and_ranks_by_nearby_day():
    """Belize, 2025-12-24: -05:00 recorded, too few GPS fixes to measure the clock."""
    frames = frames_at(datetime(2025, 12, 24, 8, 53), 10, offset=-300, first_count=1,
                       gps_error=-60, gps_every=5)

    def belize(_lat, _lon, _utc):
        return ("America/Belize", -360)

    problem = analyze_camera(frames, zone_lookup=belize, nearby_error=(-60, "2026-01-02 (2026-01-02)"))
    assert problem is not None
    by_shift = {f.shift_minutes: f for f in problem.fixes}
    assert set(by_shift) == {0, -60}
    assert all(f.target_offset == -360 for f in problem.fixes)
    assert by_shift[0].recommended and not by_shift[-60].recommended
    assert "2026-01-02" in by_shift[0].explanation
    assert len({f.group for f in problem.fixes}) == 1


def _reset_frames(gps_error_before=None, gps_error_after=None):
    """14:00-14:05 at -07:00, then the clock set back so 13:06 follows 14:05."""
    def run(start, first, count, gps_error):
        out = []
        for i in range(count):
            captured = start + timedelta(minutes=i)
            gps = None
            if gps_error is not None:
                gps = captured + timedelta(minutes=420) - timedelta(minutes=gps_error)
            out.append(ClockFrame(ref=first + i, filename=f"DSC_{first + i:04d}.NEF", captured=captured,
                                  offset_minutes=-420, gps_utc=gps))
        return out
    return (run(datetime(2026, 4, 1, 14, 0), 1, 6, gps_error_before)
            + run(datetime(2026, 4, 1, 13, 6), 7, 6, gps_error_after))


def test_reset_with_gps_after_it_composes_the_fix_before_it():
    """GPS measures only the stretch after the reset (45 min behind): that
    stretch moves forward 45 min, and the stretch before moves back 60 + 45."""
    problem = analyze_camera(_reset_frames(gps_error_after=-45))
    assert problem is not None and "set back" in problem.summary
    by_first = {fix.frames[0].ref: fix for fix in problem.fixes}
    assert by_first[7].shift_minutes == 45 and [f.ref for f in by_first[7].frames] == list(range(7, 13))
    assert by_first[1].shift_minutes == -15 and [f.ref for f in by_first[1].frames] == list(range(1, 7))
    assert "Apply both" in by_first[1].explanation
    assert any("too few GPS fixes" in line for line in problem.evidence)
    assert any("45 min behind" in line for line in problem.evidence)


def test_reset_with_a_right_clock_before_it_moves_the_frames_after_it():
    problem = analyze_camera(_reset_frames(gps_error_before=0))
    assert problem is not None
    [fix] = problem.fixes
    assert [f.ref for f in fix.frames] == list(range(7, 13))
    assert fix.shift_minutes == 60


def test_reset_needs_no_fix_before_it_when_gps_fixes_the_rest_exactly():
    problem = analyze_camera(_reset_frames(gps_error_after=-60))
    assert problem is not None
    [fix] = problem.fixes
    assert [f.ref for f in fix.frames] == list(range(7, 13))
    assert fix.shift_minutes == 60


def test_repeated_resets_accumulate_so_every_stretch_ends_in_order():
    """12:00 -> 10:00 -> 08:00 by shot order: the first stretch moves past both resets."""
    starts = [datetime(2026, 4, 1, 12, 0), datetime(2026, 4, 1, 10, 0), datetime(2026, 4, 1, 8, 0)]
    frames = [
        ClockFrame(ref=n, filename=f"DSC_{n:04d}.JPG", captured=start + timedelta(minutes=k))
        for s, start in enumerate(starts) for k in range(2) for n in [s * 2 + k + 1]
    ]
    [problem] = find_clock_problems(frames, use_gps=False)
    corrected = {f.ref: f.captured for f in frames}
    for fix in problem.fixes:
        for f in fix.frames:
            corrected[f.ref] = f.captured + timedelta(minutes=fix.shift_minutes)
    in_shot_order = [corrected[n] for n in range(1, 7)]
    assert in_shot_order == sorted(in_shot_order)
    assert len(set(in_shot_order)) == 6


def test_one_offset_across_two_real_zones_gets_a_fix_per_zone():
    """-05:00 recorded all day: correct in New York, wrong in Los Angeles."""
    def zone(_lat, lon, _utc):
        return ("America/New_York", -300) if lon > -100 else ("America/Los_Angeles", -480)

    morning = frames_at(datetime(2026, 1, 10, 8, 0), 10, offset=-300, first_count=1, gps_error=0)
    evening = frames_at(datetime(2026, 1, 10, 20, 0), 20, offset=-300, first_count=11, gps_error=0)
    morning = [ClockFrame(**{**f.__dict__, "longitude": -74.0}) for f in morning]
    problem = analyze_camera(morning + evening, zone_lookup=zone)

    [fix] = problem.fixes
    assert {f.ref for f in fix.frames} == {f.ref for f in evening}
    assert fix.target_offset == -480


def test_repeated_resets_after_a_gps_verified_clock_move_forward():
    first = frames_at(datetime(2026, 4, 1, 12), 6, first_count=1, offset=0, gps_error=0)
    middle = frames_at(datetime(2026, 4, 1, 10), 3, first_count=7, offset=0)
    last = frames_at(datetime(2026, 4, 1, 8), 3, first_count=10, offset=0)
    frames = first + middle + last
    problem = analyze_camera(frames)
    assert problem is not None
    shifts = {f.ref: fix.shift_minutes for fix in problem.fixes for f in fix.frames}
    assert set(shifts) == {f.ref for f in middle + last}
    corrected = [f.captured + timedelta(minutes=shifts.get(f.ref, 0)) for f in frames]
    assert all(a < b for a, b in pairwise(corrected))


def test_sparse_dcf_numbers_do_not_create_a_reset():
    start = datetime(2026, 1, 25, 10)
    frames = [
        ClockFrame(ref=n, filename=f"DSC_{n:04d}.JPG", captured=start + timedelta(hours=i))
        for i, n in enumerate([100, 9000])
    ]
    assert find_clock_problems(frames, use_gps=False) == []


def test_sparse_dcf_runs_preserve_local_reset_evidence():
    start = datetime(2026, 1, 25, 10)
    frames = [
        ClockFrame(ref=n, filename=f"DSC_{n:04d}.JPG", captured=start + timedelta(hours=h))
        for n, h in [(100, 0), (101, 1), (9000, 4), (9001, 3)]
    ]
    [problem] = find_clock_problems(frames, use_gps=False)
    [fix] = problem.fixes
    assert [frame.ref for frame in fix.frames] == [9000]


def test_rollover_with_backward_clock_preserves_reset_evidence():
    frames = [ClockFrame(ref=n, filename=f"DSC_{n:04d}.JPG", captured=datetime(2026, 4, 1, hour))
              for n, hour in [(9999, 14), (1, 13)]]
    [problem] = find_clock_problems(frames, use_gps=False)
    [fix] = problem.fixes
    assert [f.ref for f in fix.frames] == [9999]
    assert fix.shift_minutes == -61


def test_measured_reset_epochs_need_separate_fix_groups():
    problem = analyze_camera(_reset_frames(gps_error_before=30, gps_error_after=-45))
    assert problem is not None
    assert {f.shift_minutes for f in problem.fixes} == {-30, 45}
    assert len({f.group for f in problem.fixes}) == 2


def test_reset_shift_depends_on_unmeasured_zone_choice():
    from dataclasses import replace

    frames = [replace(f, latitude=34., longitude=-118.) if f.ref <= 6 else f for f in _reset_frames()]
    problem = analyze_camera(frames, zone_lookup=la_winter)
    assert problem is not None
    assert {f.shift_minutes for f in problem.fixes} == {-60, 0}
    assert all(f.ref <= 6 for fix in problem.fixes for f in fix.frames)
    options = {fix.shift_minutes: fix for fix in problem.fixes}
    assert not options[-60].dependent_fixes
    [later] = options[0].dependent_fixes
    assert later.shift_minutes == 60
    assert [f.ref for f in later.frames] == list(range(7, 13))
    assert later.target_offset is None
    for option in options.values():
        shifts = {f.ref: step.shift_minutes for step in [option, *option.dependent_fixes] for f in step.frames}
        corrected = [f.captured + timedelta(minutes=shifts.get(f.ref, 0)) for f in frames]
        assert all(a < b for a, b in pairwise(corrected))


def test_reset_epoch_uses_its_own_clock_error_at_dst_boundary():
    from zoneinfo import ZoneInfo

    def local_zone(_lat, _lon, utc):
        from datetime import UTC
        local = utc.replace(tzinfo=UTC).astimezone(ZoneInfo("America/Los_Angeles"))
        return "America/Los_Angeles", int(local.utcoffset().total_seconds() // 60)

    before = frames_at(datetime(2026, 11, 1, 1, 50), 8, offset=-420, first_count=1, gps_error=0)
    after = frames_at(datetime(2026, 11, 1, 0, 50), 4, offset=-420, first_count=9, gps_error=-120)
    problem = analyze_camera(before + after, zone_lookup=local_zone)
    assert problem is not None
    [fix] = problem.fixes
    assert (fix.target_offset, fix.shift_minutes) == (-480, 60)


def test_rollover_with_no_gap_larger_than_half_the_counter():
    frames = [ClockFrame(ref=n, filename=f"DSC_{n:04d}.JPG", captured=datetime(2026, 4, 1, hour))
              for n, hour in [(1, 12), (5000, 13), (9999, 14)]]
    [problem] = find_clock_problems(frames, use_gps=False)
    assert problem.jump_count == 1
    assert any("DSC_9999.JPG" in line and "DSC_0001.JPG" in line for line in problem.evidence)


def test_post_reset_zone_choices_include_the_shift_needed_for_shot_order():
    from dataclasses import replace

    frames = [replace(f, latitude=34., longitude=-118.) if f.ref > 6 else f
              for f in _reset_frames(gps_error_before=0)]
    problem = analyze_camera(frames, zone_lookup=la_winter)
    assert problem is not None
    # Both readings lead to the same write: 0 + 60 and -60 + 120.
    [fix] = problem.fixes
    assert (fix.shift_minutes, fix.target_offset) == (60, -480)
    assert [f.ref for f in fix.frames] == list(range(7, 13))
    assert not fix.dependent_fixes
    corrected = [f.captured + timedelta(minutes=60 if f in fix.frames else 0) for f in frames]
    assert all(a < b for a, b in pairwise(corrected))


def test_post_reset_zone_choices_respect_corrected_anchor_and_keep_valid_options():
    from dataclasses import replace

    frames = [replace(f, latitude=34., longitude=-118.) if f.ref > 6 else f
              for f in _reset_frames(gps_error_before=30)]
    problem = analyze_camera(frames, zone_lookup=lambda *_: ("America/New_York", -300))
    assert problem is not None
    assert [(f.shift_minutes, f.target_offset) for f in problem.fixes if f.frames[0].ref == 1] == [(-30, None)]
    assert {(f.shift_minutes, f.target_offset) for f in problem.fixes if f.frames[0].ref == 7} == {
        (30, -300), (120, -300),
    }


def test_ambiguous_zones_on_both_sides_offer_only_joint_ordered_writes():
    from dataclasses import replace

    frames = [replace(f, latitude=34., longitude=-118.) for f in _reset_frames()]
    problem = analyze_camera(frames, zone_lookup=la_winter)
    assert problem is not None
    assert len({f.group for f in problem.fixes}) == 1
    pairs = set()
    for option in problem.fixes:
        [later] = option.dependent_fixes
        assert option.target_offset == later.target_offset == -480
        assert option.frames == frames[:6] and later.frames == frames[6:]
        pairs.add((option.shift_minutes, later.shift_minutes))
        assert (frames[5].captured + timedelta(minutes=option.shift_minutes)
                < frames[6].captured + timedelta(minutes=later.shift_minutes))
    assert pairs == {(0, 60), (-60, 0)}


def test_unmeasured_epoch_leaves_correct_zone_frames_available_for_reset():
    from dataclasses import replace

    frames = _reset_frames(gps_error_before=0)
    frames = [replace(f, latitude=34., longitude=-118. if f.ref <= 9 else -100.)
              if f.ref > 6 else f for f in frames]

    def zone(_lat, lon, _utc):
        return ("Zone A", -420) if lon < -110 else ("Zone B", -300)

    problem = analyze_camera(frames, zone_lookup=zone)
    assert problem is not None
    boundary = [fix for fix in problem.fixes if frames[6] in fix.frames]
    assert len(boundary) == 1
    assert boundary[0].frames == frames[6:9]
    assert boundary[0].shift_minutes == 60
    assert all(fix.frames == frames[9:] for fix in problem.fixes if fix not in boundary)


def test_distinct_paired_choices_propagate_to_the_next_reset():
    from dataclasses import replace

    frames = [replace(f, latitude=34., longitude=-118. if f.ref <= 6 else -75.) for f in _reset_frames()]
    frames += [replace(f, ref=f.ref + 6, filename=f"DSC_{f.ref + 6:04d}.NEF",
                       captured=f.captured - timedelta(minutes=54), latitude=None, longitude=None)
               for f in frames[6:]]
    problem = analyze_camera(frames, zone_lookup=lambda _lat, lon, _utc: (
        ("America/Los_Angeles", -480) if lon < -100 else ("America/New_York", -300)
    ))
    assert problem is not None
    triples = set()
    for plan in problem.fixes:
        second, third = plan.dependent_fixes
        triples.add((plan.shift_minutes, second.shift_minutes, third.shift_minutes))
        assert third.target_offset is None
        times = [f.captured + timedelta(minutes=step.shift_minutes)
                 for step in (plan, second, third) for f in step.frames]
        assert len(times) == len(frames)
        assert all(a < b for a, b in pairwise(times))
    assert triples == {(0, 60, 120), (0, 120, 180), (-60, 0, 60), (-60, 120, 180)}
