---
name: vireo-species-views
description: Find, edit, and export the best recent photos of a named bird or other animal from Vireo. Use when the user says they just photographed a species and want the best shots, wants a set of views to check an ID, or invokes /vireo-species-views. Chooses distinct views, brings out identifying details, and exports them with available location metadata.
---

# Export the best views of a species from Vireo

The user names an animal they just photographed. Find those photos in Vireo,
choose a set that shows the animal from as many useful angles as the session
offers, edit each one so the detail is easy to see, export them, and show the
user the files.

The set supports an identification, so covering different views matters more
than a row of similar pretty frames. A soft frame of a spread wing is worth more
than a fifth sharp profile.

This writes one edit per chosen photo, or a virtual copy when the photo already
carries an edit, and the exported files. Do not change flags, ratings, species
labels or highlights unless the user asks. Do not import photos. If they are not
in Vireo yet, say so.

## 1. Find the session

1. Call `vireo_status()` for the active workspace. If Vireo is not running, say
   so and stop. Never start another instance against the same catalog.
2. Resolve the name with `search_keywords(query="...")`, because the catalog may
   use a different common name.
3. List the newest photos with
   `search_photos(keyword="...", sort="date_desc")`. "Just took" means the
   newest capture date that has this animal. Take that day's whole outing in
   that folder.
4. New photos are often unlabeled or mislabeled, most of all when the animal is
   hard to identify. If the labeled photos look too old or too few, also try:
   - `best_of_each_species(species="...", include_unconfirmed=true, date_from="...")`
     for photos held only by a prediction
   - `search_photos(sort="date_desc")` for the newest folder, then
     `search_photos(folder_id=..., sort="date")`
   - `find_photos_by_text(query="...")` with a plain description

   Frames shot seconds before or after a labeled frame usually show the same
   animal. Confirm by looking, not by the label.
5. Searches see only the active workspace. If it has nothing from that time, say
   which workspace you searched, list the others with `list_workspaces()`, and
   ask which to use. `activate_workspace(workspace_id=...)` also switches the
   user's Vireo window.
6. Say in one line which date, folder and number of candidate photos you are
   working from, then continue, so the user can correct you.

If most photos have `quality_analyzed` false, Process has not finished. Choosing
by eye still works. Say that animal boxes and burst rankings may be missing.

## 2. Choose one photo per view

Sort the candidates by what they show before comparing sharpness. Use
`view_photo(photo_id=..., view="thumbnail")` for that, and
`view_photo(photo_id=..., view="subject")` when the animal is small. Neither is
enough to judge focus. For a long session start from
`list_bursts(folder_id=...)`: the frames of a burst usually share a pose, so
each burst contributes one or two candidates.

Views worth covering for a bird, when the session has them:

- side profile of the whole bird (flank, wing pattern, proportions)
- head close enough to read the bill shape, eye and face pattern
- front (throat, breast, belly)
- back and upperparts (mantle, wing bars, rump)
- tail, from above and below, spread if possible
- underside, undertail and legs or feet
- flight from above and from below (upperwing and underwing)
- the bird in its setting, or beside another bird, for size
- behavior (feeding, calling, displaying)
- each distinct individual when there are several (male, female, juvenile)

For other animals use the equivalent set: both sides, face, markings, and
anything a field guide would ask for.

Look past both ends of the sequence. Frames shot as the animal leaves a perch or
just before it lands often have no box, no label and heavy blur, so no search
returns them. They are also the likeliest to show the underwing, the spread tail
or the rump. Page through `search_photos(folder_id=..., sort="date", page=...)`
and look at every frame for about two minutes before the first candidate and
after the last. A dark or backlit frame can hide the view, so render
`view_edit(photo_id=..., recipe={...})` with `exposure` near 1.5 and `shadows` at
80 before deciding it shows nothing. Include such a frame as a record shot when
it shows a view the others lack.

Within a view, pick the sharpest frame with `focus_crop(photo_id=...)` and read
its `notes` first. When the crop misses the head, find the eye on
`view_photo(photo_id=..., view="source")` and call
`focus_crop(photo_id=..., center_x=..., center_y=...)`.

There is no target count. A typical outing gives 6 to 12 photos. Add a second
photo of a view only when it shows something the first does not. Skip photos the
user rejected unless one holds a view nothing else covers, and say so.

## 3. Edit each chosen photo

The animal should fill the frame, feather or fur detail should be easy to see,
and colors should stay as shot. A misleading color or an invented edge can cause
a wrong identification, so leave white balance, saturation and vibrance alone.

Call `get_edit(photo_id=...)` first. If `recipe` is not null, the user edited the
photo: export it as it is. If it needs a different look because the user asked
or their crop hides the view, save yours with
`save_edit_copy(photo_id=..., name="...", recipe={...})` and export that copy on
its own.

Otherwise build the recipe in this order.

1. **Crop.** Call `suggest_crop(photo_id=...)`. Use `margin` 0.6 for flight or
   the animal in its setting, and choose a tighter rectangle by eye for a head
   study. When there is no box, or it is on the wrong animal, pass another
   `detection_id` from `get_photo(photo_id=...)` or set the crop by eye: `x`,
   `y`, `w` and `h` are fractions of the upright frame. When the output is under
   about 1000 pixels on the long edge the animal was small. Keep the photo if
   its view is needed and mention the size.
2. **Tone.** Call `suggest_tone(photo_id=..., recipe={...})` with the crop. It
   meters the animal rather than the whole frame. Copy its exposure, highlights,
   shadows, whites, blacks and contrast. Then adjust by eye: raise shadows (20
   to 40) when dark plumage hides its pattern, and lower highlights (-30 to -60)
   when white plumage has lost its texture. For a backlit bird against open sky
   the fitted exposure, often +2 EV, turns the sky white. Use about 1 EV less
   with shadows 70, highlights -50, blacks -25 and contrast 12. Leave dehaze at
   0, because it shifts plumage color.
3. **Detail.** Start from texture 30, clarity 12, sharpen 45 and
   noise_reduction 15. For a frame that is sharp but flat go up to about
   texture 55, clarity 25, sharpen 70, sharpen_radius 1.2 and noise_reduction
   25. Raise noise_reduction with ISO (around 30 to 45 above ISO 3200) and lower
   sharpen on a soft frame, because sharpening blur makes halos, not feathers.

A recipe looks like this:

```json
{"crop": {"x": 0.106, "y": 0.230, "w": 0.613, "h": 0.612},
 "adjustments": {"exposure": 0.6, "blacks": -40, "texture": 30, "clarity": 12,
                 "sharpen": 45, "noise_reduction": 15}}
```

Look at every edit with `view_edit(photo_id=..., recipe={...})` before saving. To
check halos and noise at 1:1, render the same adjustments with a crop about 1000
pixels wide on the head: `view_edit(photo_id=..., recipe={...}, size=1000)`.
Change the recipe until the detail reads well and nothing looks processed.

Save with
`save_edit(photo_id=..., recipe={...}, reason="Species views: crop and detail", source="...")`,
where the source is your model id. To revise an edit you saved, pass the recipe
that save returned as `replaces`. Never pass the user's own recipe there.

## 4. Export

1. Call `list_export_presets()`. Use the preset the user names, otherwise one
   named `Main`, otherwise the only one.
2. Choose the folder: the one the user named, otherwise the preset's. When the
   preset's `destination` is empty it means beside each original, so use
   `~/Pictures/Vireo exports` unless the user wants that. Name the subfolder
   after the species and date.
3. Call
   `preview_export(photo_ids=[...], preset="...", destination="...", subfolder="...")`.
   Location is included by default; turn it off only when the user asks. Check
   `destination_folders`, `file_count`, `skipped`, `renamed` and
   `without_coordinates`, and tell the user the destination. Preview a virtual
   copy separately with its `version_id`.
4. Call `start_export(token="...")`, then
   `get_job(job_id="...", wait_seconds=50)` until it is completed, failed or
   cancelled.
5. Check the result: `exported` equals the number of photos, `errors` is empty,
   and `written` under `metadata.categories.location` matches the photos that
   have coordinates. Use `result.files` as the list of what was written. When
   you can run exiftool, confirm the GPS tags on the files. Never add
   coordinates yourself.

## 5. Show the set and report

Show the exported images when the session can display local files, and always
give the folder path. Then report, in prose:

- each photo with the view it covers and a short reason it was chosen
- the views the session did not offer (for example "no flight frames, so the
  underwing is not shown")
- any photo included as a record shot despite softness or small size
- photos exported with the user's own edit, unchanged
- photos without location data

If the photos show marks that do not fit the species the user named, say what
you see and which photo shows it. Do not relabel anything. End by saying that
the edits can be undone in Vireo's Undo panel, and that Undo does not remove the
exported files.

For an outing with more than about 150 candidate frames, sort views burst by
burst and tell the user the shortlist before editing.
