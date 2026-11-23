---
name: vireo-adjudicate-bursts
description: Use when the user asks to pick the best photos of a burst, cull a folder in Vireo, "adjudicate bursts", "review the bursts in <folder>", "which of these frames should I keep", or invokes /vireo-adjudicate-bursts. Walks a Vireo folder's bursts, compares their frames at 1:1 pixels, and records picks and rejects as flags with a source and reason, so they show in Vireo's Undo panel and photo detail.
---

# Adjudicate bursts in Vireo

Vireo ranks the frames of a burst from measurements of the animal: the detector
box, the subject mask, and the sharpness inside that mask or around the eye.
The ranking is provisional. Vireo never names a keeper itself, because the
measurements cannot see head angle, wing position, a catchlight, or whether the
detector found the right animal. You look at those and decide.

## Rules

1. **Judge focus only from `focus_crop`.** It shows unresized pixels. Use
   thumbnails and previews for composition and pose.
2. **Leave a burst alone when it has `unanalyzed_frames`.** The ranking misses
   those frames. Report the burst and say that Process needs to run on it. Do
   not substitute your own sharpness estimate.
3. **Give every decision a `source` and a `reason`.** The source is your model
   id. The reason is one short clause a photographer would recognize, such as
   "eye soft at 1:1; DSC_1744 sharper" or "wing clips frame edge".
4. **Write only decisions you are confident in.** The rest go in the report.
5. **Do not change a flag that is already set** unless the user asked you to
   revisit it.

## Steps

1. **Check the connection.** Call `vireo_status()`. If Vireo is not running,
   say so and stop. Never start another instance against the same catalog.
2. **Find the folder.** Call `list_folders(query="...")`, or ask which folder
   the user means when several match.
3. **List its bursts.** Call `list_bursts(folder_id=..., unflagged_only=true)`.
   Read `coverage` first. If `with_quality` is 0, stop and report that Process
   has not analyzed the folder. Otherwise drop the bursts whose reasons include
   `unanalyzed_frames` and count them for the report. Continue with
   `next_offset` until it is null, and finish listing before you write any flag:
   a flagged burst leaves the list, so later offsets would skip bursts.
4. **Order the rest.** Start with bursts that carry a reason besides
   `positive_keep_not_validated`, because those have a specific doubt to
   settle. Every ranked burst carries that one, so it says nothing about a
   particular burst.
5. **Look at one burst.** Call `burst(photo_id=...)` with its seed. Read
   `review_reasons`. For a burst longer than its `ranked_frames`, also look at
   the frames outside the ranking, or say that you did not.
6. **Compare the frames at 1:1.** Call `focus_crop(photo_id=...)` on every
   frame that is not labeled `REJECT`. Read `notes` before the image.
   - When the crop misses the head, call
     `view_photo(photo_id=..., view="source")`, estimate the eye's position as
     fractions of that frame's width and height, and call
     `focus_crop(photo_id=..., center_x=..., center_y=...)`.
   - When the notes say the pixels are a downsized copy, call
     `focus_crop(photo_id=..., source="original")`.
   - Compare two frames only at the same size and the same source.
7. **Check what the reasons point at.**
   - `close_call`: compare the top two crops directly.
   - `multiple_subjects`: call `focus_crop(photo_id=..., mask=true)` to see
     which animal was scored. Call
     `focus_crop(photo_id=..., detection_id=...)` to inspect another; the ids
     are in `get_photo(photo_id=...)`.
   - `subject_touches_edge`: call `view_photo(photo_id=..., view="preview")`
     and look for the cut.
   - `no_eye_located`: the sharpness is the whole subject's, so check the eye
     yourself.
   - `all_rejected`: confirm the failure before rejecting anything.
8. **Decide.** Keep the ranked leader, prefer another frame, reject a frame
   with a clear fault, or leave the burst undecided and note what you would
   need to see. Keep more than one frame when each shows something different.
   A frame that lost the comparison is not a reject for that alone.
9. **Write the flags.** Call
   `set_flag(photo_ids=[...], flag="flagged", source="...", reason="...")` for
   picks and the same with `flag="rejected"` for rejects. Make one call per
   burst and flag, so each is one Undo step.

Work in batches of about five bursts and report after each one, so the user can
redirect you.

## Report

Say how many of the folder's photos are analyzed, how many bursts you reviewed,
and how many you skipped for `unanalyzed_frames`. List each decision with its
reason, then the bursts you left undecided and why. Tell the user that every
flag can be reversed in Vireo's Undo panel, and that the source and reason show
on the photo's detail panel.
