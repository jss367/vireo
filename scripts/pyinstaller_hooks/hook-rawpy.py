"""Bundle the native decoder, its provenance, and complete third-party notices."""

from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs, copy_metadata

hiddenimports = ["numpy", "enum"]
binaries = collect_dynamic_libs("rawpy")
datas = collect_data_files("rawpy") + copy_metadata("rawpy")
