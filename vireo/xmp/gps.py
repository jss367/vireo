"""GPS coordinates Vireo assigns, with a backup of what they replaced."""

import math

from xmp.document import _SidecarDocument
from xmp.namespaces import NS_EXIF, NS_VIREO
from xmp.photo_scope import _get_plain_property_value
from xmp.properties import _SimpleProperties


def _format_gps_coordinate(value, positive_ref, negative_ref):
    """Return an XMP GPSCoordinate string such as ``48,51.398N``."""
    ref = positive_ref if value >= 0 else negative_ref
    absolute = abs(float(value))
    degrees = int(absolute)
    minutes = (absolute - degrees) * 60.0
    return f"{degrees},{minutes:.6f}{ref}"


class _GpsEdits:
    """Writes and removes the Lightroom-compatible ``exif:GPS*`` fields.

    ``vireo:gpsSource`` marks the coordinates as Vireo's, and
    ``vireo:previousGPS*`` keeps the values another application had written,
    so removing Vireo's location restores them.
    """

    def __init__(self, document: _SidecarDocument, properties: _SimpleProperties):
        self.document = document
        self.properties = properties

    def set_gps_location(self, latitude, longitude, source="assigned"):
        """Write Lightroom-compatible GPS coordinates, creating a sidecar if needed."""
        lat = float(latitude)
        lon = float(longitude)
        if not math.isfinite(lat) or not (-90.0 <= lat <= 90.0):
            raise ValueError("latitude must be between -90 and 90")
        if not math.isfinite(lon) or not (-180.0 <= lon <= 180.0):
            raise ValueError("longitude must be between -180 and 180")

        desc = self.document.description()
        marker = f"{{{NS_VIREO}}}gpsSource"
        exif_attrs = {
            "GPSLatitude": f"{{{NS_EXIF}}}GPSLatitude",
            "GPSLongitude": f"{{{NS_EXIF}}}GPSLongitude",
            "GPSMapDatum": f"{{{NS_EXIF}}}GPSMapDatum",
            "GPSVersionID": f"{{{NS_EXIF}}}GPSVersionID",
        }

        # First Vireo write: preserve any GPS another app had already
        # written so clearing the Vireo-assigned location can restore
        # it. Rewrites of an existing Vireo GPS keep the original
        # backup. Back up the PLAIN occurrence's value specifically
        # (via ``_get_plain_property_value''): that's the occurrence
        # ``set_plain_property_value'' will overwrite, so the backup
        # matches what a later restore needs to write back into it.
        # A higher-scoring qualified sibling isn't Vireo's to back up
        # and isn't what the restore path targets; capturing its
        # value here would leave the plain occurrence's original
        # coordinate permanently lost after a clear-and-restore.
        # When no plain occurrence exists Vireo will create one and
        # a later remove can just delete it -- no backup is needed.
        changed = False
        if self.document.get(marker) is None:
            for name, attr in exif_attrs.items():
                existing = _get_plain_property_value(self.document.root, attr)
                if existing is not None:
                    changed |= self.properties.set_properties(
                        desc, {f"{{{NS_VIREO}}}previous{name}": existing},
                    )

        # Write the GPS attrs through the plain-only path so an
        # externally-authored qualified copy (say ``foo:source="camera"'')
        # isn't overwritten -- Vireo's single ``vireo:previousGPS*''
        # backup can't restore that value per-occurrence, so a later
        # clear would permanently replace the external's coordinate
        # with Vireo's assigned one. The Vireo-owned ``vireo:gpsSource''
        # marker has no external qualified copies to preserve, so
        # the standard writer still handles it.
        gps_values = {
            exif_attrs["GPSLatitude"]: _format_gps_coordinate(lat, "N", "S"),
            exif_attrs["GPSLongitude"]: _format_gps_coordinate(lon, "E", "W"),
            exif_attrs["GPSMapDatum"]: "WGS-84",
            exif_attrs["GPSVersionID"]: "2.3.0.0",
        }
        for attr, val in gps_values.items():
            changed |= self.properties.set_plain_property_value(desc, attr, val)
        changed |= self.properties.set_properties(
            desc, {marker: source or "assigned"},
        )
        return changed

    def remove_vireo_gps_location(self):
        """Remove GPS fields only when Vireo previously wrote them."""
        if not self.document.readable():
            return False
        desc = self.document.find_description()
        if desc is None:
            return False

        marker = f"{{{NS_VIREO}}}gpsSource"
        if self.document.get(marker) is None:
            return False

        removed = False
        for name in ("GPSLatitude", "GPSLongitude", "GPSMapDatum", "GPSVersionID"):
            gps_attr = f"{{{NS_EXIF}}}{name}"
            previous_attr = f"{{{NS_VIREO}}}previous{name}"
            previous = self.document.get(previous_attr)
            if previous is not None:
                # Restore the backup only onto plain (unqualified)
                # copies -- those are what Vireo overwrote. A
                # qualified copy another tool added between the
                # initial Vireo write and this removal isn't ours
                # to touch: we didn't overwrite it and we can't
                # back it up per-occurrence, so leave its metadata
                # AND its value alone.
                restored = self.properties.restore_plain_property_value(gps_attr, previous)
                if not restored:
                    # An external tool deleted or rewrote-as-qualified
                    # our plain occurrence between the initial write and
                    # now, so the restore had nowhere to land. Recreate
                    # a plain target from the backup before dropping
                    # ``vireo:previous*'' -- otherwise the original
                    # coordinate is lost entirely.
                    self.properties.set_plain_property_value(desc, gps_attr, previous)
                self.properties.delete_property(previous_attr)
                removed = True
            elif self.properties.delete_plain_property_copies(gps_attr):
                # Vireo originally created this field, so only its
                # plain occurrence is ours to remove. A qualified
                # duplicate another tool added after the initial write
                # carries data we can't restore per-occurrence -- and
                # there's no backup to restore anyway -- so leave it
                # in place with its metadata AND original value intact.
                removed = True

        removed |= self.properties.delete_property(marker)
        return removed
