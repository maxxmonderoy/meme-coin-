"""Decoding: IDL-driven, fail-loud.

Program layout changes produce corrupt output rather than errors
(CLAUDE.md 3.8.8). The defence here is that no field offset is hardcoded --
layouts are read from the vendored IDL, and `idl_check` diffs the vendored
copy against upstream so a layout change is a visible diff, not a silent
reinterpretation of bytes.
"""
