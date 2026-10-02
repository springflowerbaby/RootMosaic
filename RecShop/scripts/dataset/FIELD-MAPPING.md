# Dataset field mapping

The maintained public rules are in [DATA-FORMAT.md](../../docs/DATA-FORMAT.md).

The portable implementation retains established numerical conversions and V1 delivery checks. Source path resolution is explicit; outputs are independent. Original evidence bytes/hashes remain unchanged.

`trace_formats.py` contains the vendored flat-trace function and original source hash. It does not import an old collection or packaging runner.
