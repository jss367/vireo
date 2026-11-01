# Third-party notices

Vireo's original code is licensed under the [MIT License](LICENSE). This does
not replace or override the licenses of third-party code, data, or model
weights used by Vireo. Those components retain their respective licenses and
notices, including the components documented below.

## ExifTool

Vireo desktop builds bundle ExifTool by Phil Harvey.

Copyright 2003-2026, Phil Harvey.

ExifTool is free software; it may be redistributed and/or modified under the
same terms as Perl itself: either the Perl Artistic License or the GNU General
Public License. Vireo uses it under the Perl Artistic License.

The complete upstream README containing the copyright and license statement is
included beside the bundled ExifTool runtime. Source and licensing information
are available at https://exiftool.org/.

## tifffile

Vireo uses tifffile by Christoph Gohlke for 16-bit RGB TIFF export.
Copyright (c) 2008-2026, Christoph Gohlke. Distributed under the BSD 3-Clause
license. Desktop builds include its license in the bundled package metadata.
Source and license: https://github.com/cgohlke/tifffile.
## darktable camera noise measurements

Camera-aware denoising includes darktable's camera/ISO noise measurements,
with contributor credits preserved in the JSON (trailing whitespace normalized). The data is
redistributed under GPL-3.0-or-later. Its complete source, upstream revision,
and license are packaged in `vireo/data/denoise/`. Vireo's denoising and
profile-selection implementation is original code using OpenCV.

Source: https://github.com/darktable-org/darktable/blob/636a7471b85ce7a65dcbbc9b6ee2946ef81eecae/data/noiseprofiles.json

## Native photo filters

The optional native noise-reduction and sharpening extension uses PyO3,
rust-numpy, Rayon, and their Rust dependencies. Complete notices for the pinned
dependencies are packaged in `vireo/data/native-detail/THIRD_PARTY_LICENSES.txt`.
The source and locked dependency versions are in `native/detail/`.

## LightCraft lens corrections and defringing

`vireo/optics.py` adapts the manual radial distortion, vignetting gain, and
edge-aware purple/green defringing algorithms from LightCraft
(`crates/pipeline/src/optics.rs`). Modified for Vireo: Python/NumPy/OpenCV,
linear sRGB working space, bounded row tiles, mask alignment, and border inset.
Automatic profile matching and interpolation in `vireo/lens_profiles.py` also
follow LightCraft's `crates/lensdb/src/lib.rs` and `model.rs`.

Source: https://github.com/storytold/lightcraft

```text
MIT License

Copyright (c) 2026 ArtCraft Team and the LightCraft contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Lensfun calibration database

The unmodified XML camera/lens calibration database is bundled under the
Creative Commons Attribution-ShareAlike 3.0 Unported license (CC-BY-SA-3.0).
Copyright the Lensfun project and contributing photographers, as credited in
the XML files. Complete license and pinned upstream provenance are packaged in
`vireo/data/lensfun/LICENSE.txt` and `vireo/data/lensfun/NOTICE.txt`.

Source: https://github.com/lensfun/lensfun/tree/052c3ec6380aaa172caa6d7ea3a5eaafe0b052da/data/db

Vireo's profile reader and renderer do not bundle the Lensfun library.
