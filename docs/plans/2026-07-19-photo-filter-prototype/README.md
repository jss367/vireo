# Vireo universal photo filter prototype

This is a disposable, backend-free interaction prototype. It does not read or
write Vireo's database and it does not modify any photo or XMP file.

## Run it

From the repository root:

```bash
python3 -m http.server 4173 --directory docs/plans/2026-07-19-photo-filter-prototype
```

Then open <http://127.0.0.1:4173>.

The prototype is entirely offline. Its 54 fictional records and stylized
wildlife thumbnails are generated in the browser. Filter state, checklist
state, and simulated saved Collections are stored in browser `localStorage`.
Use **Reset prototype** to remove all of them.

## What is real versus simulated

- Text, metadata, boolean, numeric, negative, and nested rule evaluation is
  real client-side filtering over the sample records.
- CLIP similarity is deterministic but simulated from descriptive tags.
- Browse, Map, Review, and Duplicates use the same filters but apply different
  visible page scopes.
- Saving a Collection and handing results to another page are simulations
  stored only in `localStorage`.

## Suggested evaluation

Try these without reading the implementation:

1. Find filenames containing `owl` but not `juvenile`.
2. Search visually for `bird flying at dusk`, then refine to picked photos
   rated four stars or better.
3. Build `(Picked OR 5 stars) AND Camera contains Sony` using Advanced logic.
4. Load **Crowded filter bar**, then resize the window and inspect `+N` chip
   overflow.
5. Use **Open results in…** to move the same expression through Map, Review,
   and Duplicates. Confirm that every added page scope is visible.
6. Reload the browser and verify the active page restores with visible chips.
7. Save the expression as a simulated Collection and reopen it from the
   scenario list.
8. Add a **Camera model** rule and pick a value from the typeahead — options
   come from the sample library with live match counts that respect the other
   active filters.
9. Click two color labels in Quick filters to build `Color label is one of
   Red, Yellow`; flags combine the same way.
10. Set **Capture date · is in the last · 12 months**, or use **is between**
    on a date or number field (single rule, two bounds).
11. Press `\` (or the **⏸ Pause** button) to temporarily disable all filters
    without losing them; a badge reports how many photos would match. Press
    `\` again to resume.

While testing, notice:

- whether plain text and visual search feel distinct enough;
- whether it is always clear why a photo or group is visible;
- whether locked scope chips communicate rather than confuse;
- whether live filtering feels responsive or distracting;
- whether the filter bar remains compact at narrow widths; and
- which fields deserve permanent quick controls.
