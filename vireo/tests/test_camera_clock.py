"""Camera-clock detection: shot order, time-zone switches, GPS error, location."""

from datetime import datetime, timedelta

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
