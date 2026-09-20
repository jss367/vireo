# Third-party notices

## ExifTool

Vireo desktop builds bundle ExifTool by Phil Harvey.

Copyright 2003-2026, Phil Harvey.

ExifTool is free software; it may be redistributed and/or modified under the
same terms as Perl itself: either the Perl Artistic License or the GNU General
Public License. Vireo uses it under the Perl Artistic License.

The complete upstream README containing the copyright and license statement is
included beside the bundled ExifTool runtime. Source and licensing information
are available at https://exiftool.org/.

## darktable camera noise measurements

Camera-aware denoising includes darktable's camera/ISO noise measurements,
with contributor credits preserved in the JSON (trailing whitespace normalized). The data is
redistributed under GPL-3.0-or-later. Its complete source, upstream revision,
and license are packaged in `vireo/data/denoise/`. Vireo's denoising and
profile-selection implementation is original code using OpenCV.

Source: https://github.com/darktable-org/darktable/blob/636a7471b85ce7a65dcbbc9b6ee2946ef81eecae/data/noiseprofiles.json
