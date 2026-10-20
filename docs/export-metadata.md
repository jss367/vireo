# Photo export metadata

Metadata is opt-in. The export API's `metadata_fields` selection controls each
category; API requests do not automatically apply a saved preset. The exporter
writes an allowlist to the rendered image rather than copying source metadata
wholesale. Original orientation, dimensions, thumbnails, and unrelated metadata
are never copied as attributes of a cropped or resized derivative.

## Capture dates, times, and offsets

The catalog timestamp is authoritative, including its fractional seconds and any
explicit UTC offset. Source evidence comes from the stored `photos.exif_data`
snapshot, so offline originals remain exportable. Export does not re-read an
original, guess a timezone from coordinates, or use the computer's timezone.
Rescan the original to refresh an outdated metadata snapshot.

For a catalog timestamp without an offset:

1. Use EXIF `DateTimeOriginal` and `SubSecTimeOriginal` (falling back to
   `SubSecTime`). Only when `DateTimeOriginal` is absent, use `CreateDate` and
   `SubSecTimeDigitized` (falling back to `SubSecTime`).
2. Require the source wall time to equal the catalog wall time, including
   fractional seconds. Trailing zeroes are equivalent: `.24` equals `.240000`.
   A changed time, missing time, or inconsistent fraction cannot recover an offset.
3. Use the paired `OffsetTimeOriginal` or `OffsetTimeDigitized`, respectively,
   falling back to `OffsetTime` when the paired tag is absent. An inline source
   offset is also supported. Digitized offsets never override original offsets.
4. An XMP `DateTimeOriginal` with the same source wall time may supply an offset
   or corroborate it. Conflicting inline, paired EXIF, or matching XMP offsets
   leave the catalog time timezone-free. Invalid offsets are unavailable.

Selecting both capture date and capture time writes EXIF `DateTimeOriginal` and
`CreateDate`, their separate subsecond/offset tags, XMP EXIF `DateTimeOriginal`,
XMP `CreateDate`, and XMP Photoshop `DateCreated`. XMP carries fractions and offsets
inline. Unknown offsets stay absent in EXIF and XMP.

Date selection independently writes IPTC `DateCreated`. Time selection writes
IPTC `TimeCreated` **only when a supported offset is known**. IPTC cannot represent
fractional seconds, and ExifTool otherwise fills an omitted offset from the host
timezone. With an unknown offset, this IPTC field is reported unavailable;
EXIF/XMP still carry the timezone-free time when both date and time are selected.
Selecting time alone cannot write a full date or represent fractional seconds.

## Coordinates and named places

Coordinates use `db.locations.get_effective`: the complete catalog photo GPS pair
wins, including edited coordinates; otherwise the selected coordinate-bearing
location keyword supplies the pair. The axes are never mixed. Cached place-center
coordinates and source GPS never replace this effective pair. EXIF and XMP GPS
receive the same coordinates.

Names use the catalog location leaf and its location-only ancestors, using the
same selection as sidecar synchronization. A hierarchy needs no coordinates.
The current leaf is written as XMP IPTC Core `Location`, IPTC `Sub-location`, and
XMP IPTC Extension `LocationShownSublocation`. The full broadest-to-leaf path is
written as Lightroom `HierarchicalSubject`; this includes county and other levels
without pretending they are cities or states.

The catalog tree retains names without semantic level labels. City, state/province,
and country are populated only from supported typed evidence, in this order:

1. The newest locally cached place response with the exact current leaf place ID
   and leaf name. Components typed `locality`/`postal_town`,
   `administrative_area_level_1`, and `country` supply the respective fields only
   when their names occur in the current catalog path.
2. Stored XMP `City`/`State`/`Country` or IPTC
   `City`/`Province-State`/`Country-PrimaryLocationName`, again only when the value
   occurs in the current path. Conflicting XMP/IPTC values are not used.

These fields are written to matching XMP Photoshop, IPTC, and XMP IPTC Extension
Location Shown fields, using the [ExifTool XMP tag definitions](https://exiftool.org/TagNames/XMP.html).
Missing semantic labels are reported unavailable; they are not inferred from
tree depth. Export makes no geocoding requests. Clearing or changing a catalog
location does not restore an old source place or source coordinates. Location
hierarchies are independent of the species keyword selection. IPTC text is UTF-8.

## Export results

When metadata is selected, `metadata_summary` counts provenance and unavailable
reasons by field, for **successfully written outputs only**. For example:

```json
{
  "capture_timestamp": {"catalog": 1},
  "capture_offset": {"source_exif": 1},
  "location_coordinates": {"catalog_exif": 1},
  "location_city": {"unavailable:untyped_or_missing": 1}
}
```

Offset reasons distinguish missing/invalid source evidence, changed timestamps,
and conflicting offsets. `capture_subsecond` and `iptc_capture_time` report
representation limits. With `collect_files: true`, `metadata_details` also lists
each successful output's `photo_id`, `path`, and field provenance. Excluded
categories have no diagnostic entries. Unavailable source fields do not fail an
image export; metadata write failures remain export errors and the incomplete
output is removed.

IPTC text fields have byte limits (32 for sublocation/city/state, 64 for country).
Longer names remain complete in XMP; the corresponding IPTC field is omitted and
reported as `iptc_location_<field>: unavailable:exceeds_<limit>_bytes`, preventing
ExifTool from silently truncating a place name.
