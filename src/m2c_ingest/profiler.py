"""Single streaming pass over a SAP flat file: hashes, row counts, trailer, control totals
and structural checks. Memory use is bounded by the read buffer, not the file size
(except for the optional duplicate-key check, which keeps one 16-byte digest per key)."""
from __future__ import annotations

import base64
import csv
import hashlib
import io
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .config import EntityConfig

CHUNK = 4 * 1024 * 1024
MAX_SAMPLES = 5


class _HashingRaw(io.RawIOBase):
    """Raw stream that feeds every byte read through sha256 + md5."""

    def __init__(self, fh):
        self._fh = fh
        self.sha256 = hashlib.sha256()
        self.md5 = hashlib.md5()
        self.size = 0
        self.newlines = 0

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        n = self._fh.readinto(b)
        if n:
            view = memoryview(b)[:n]
            self.sha256.update(view)
            self.md5.update(view)
            self.size += n
            self.newlines += bytes(view).count(b"\n")
        return n


def parse_sap_amount(raw: str) -> Decimal:
    """SAP exports put the sign last ('1234.50-') and may use thousands separators."""
    s = raw.strip().replace(",", "")
    if not s:
        return Decimal("0")
    if s.endswith("-"):
        s = "-" + s[:-1]
    return Decimal(s)


@dataclass
class Issue:
    layer: str      # CONTROL | BALANCE
    check: str
    message: str
    line_no: int | None = None


@dataclass
class Profile:
    size_bytes: int = 0
    newline_count: int = 0
    sha256: str = ""
    md5_b64: str = ""
    header: list[str] = field(default_factory=list)
    row_count: int = 0
    bad_rows: int = 0
    trailer: list[str] | None = None
    trailer_row_count: int | None = None
    totals: dict[str, Decimal] = field(default_factory=dict)
    trailer_totals: dict[str, Decimal] = field(default_factory=dict)
    issues: list[Issue] = field(default_factory=list)

    def add(self, layer: str, check: str, message: str, line_no: int | None = None) -> None:
        if sum(1 for i in self.issues if i.check == check) < MAX_SAMPLES:
            self.issues.append(Issue(layer, check, message, line_no))

    def failed(self, check: str) -> bool:
        return any(i.check == check for i in self.issues)


def profile_file(path: Path, ent: EntityConfig) -> Profile:
    prof = Profile(totals={ct.name: Decimal("0") for ct in ent.control_totals})
    col_idx = {c: i for i, c in enumerate(ent.expected_columns)}
    amount_idx = {ct.name: col_idx[ct.column] for ct in ent.control_totals}
    key_idx = [col_idx[c] for c in ent.key_columns]
    seen_keys: set[bytes] | None = set() if ent.check_duplicate_keys else None
    width = len(ent.expected_columns)

    with open(path, "rb") as fh:
        raw = _HashingRaw(fh)
        text = io.TextIOWrapper(io.BufferedReader(raw, CHUNK), encoding=ent.encoding, newline="")
        reader = csv.reader(text, delimiter=ent.delimiter, quotechar='"')
        line_no = 0
        try:
            for record in reader:
                line_no += 1
                if not record or record == [""]:
                    continue
                if ent.has_header and line_no == 1:
                    prof.header = [h.strip() for h in record]
                    if tuple(prof.header) != ent.expected_columns:
                        prof.add("CONTROL", "header_matches_contract",
                                 f"expected {list(ent.expected_columns)}, got {prof.header}", 1)
                    continue
                if ent.trailer and record[0] == ent.trailer.prefix:
                    if prof.trailer is not None:
                        prof.add("CONTROL", "single_trailer", "more than one trailer record", line_no)
                    prof.trailer = record
                    continue
                if prof.trailer is not None:
                    prof.add("CONTROL", "trailer_is_last", "data found after trailer record", line_no)

                prof.row_count += 1
                if len(record) != width:
                    prof.bad_rows += 1
                    prof.add("CONTROL", "column_count", f"{len(record)} fields, expected {width}", line_no)
                    continue
                for name, idx in amount_idx.items():
                    try:
                        prof.totals[name] += parse_sap_amount(record[idx])
                    except InvalidOperation:
                        prof.bad_rows += 1
                        prof.add("CONTROL", "amount_parseable", f"{name}: '{record[idx]}'", line_no)
                if key_idx:
                    key = tuple(record[i].strip() for i in key_idx)
                    if any(k == "" for k in key):
                        prof.bad_rows += 1
                        prof.add("CONTROL", "key_not_null", f"null key {key}", line_no)
                    elif seen_keys is not None:
                        digest = hashlib.md5("\x1f".join(key).encode()).digest()
                        if digest in seen_keys:
                            prof.bad_rows += 1
                            prof.add("CONTROL", "key_unique", f"duplicate key {key}", line_no)
                        seen_keys.add(digest)
        except UnicodeDecodeError as e:
            prof.add("CONTROL", "encoding_valid", f"not valid {ent.encoding}: {e.reason}", line_no + 1)
            # Drain the remaining bytes so the hashes still describe the whole file.
            while raw.readinto(bytearray(CHUNK)):
                pass

    prof.size_bytes = raw.size
    prof.newline_count = raw.newlines
    prof.sha256 = raw.sha256.hexdigest()
    prof.md5_b64 = base64.b64encode(raw.md5.digest()).decode()

    if ent.has_header and not prof.header:
        prof.add("CONTROL", "header_matches_contract", "file has no header record")
    if ent.trailer:
        if prof.trailer is None:
            prof.add("CONTROL", "trailer_present", "no trailer record found")
        else:
            try:
                prof.trailer_row_count = int(prof.trailer[ent.trailer.row_count_field])
                for ct in ent.control_totals:
                    if ct.trailer_field is not None:
                        prof.trailer_totals[ct.name] = parse_sap_amount(prof.trailer[ct.trailer_field])
            except (IndexError, ValueError, InvalidOperation):
                prof.add("CONTROL", "trailer_parseable", f"cannot parse trailer {prof.trailer}")
    if prof.row_count == 0 and not ent.allow_empty:
        prof.add("CONTROL", "non_empty", "file has no data rows and entity does not allow empty loads")
    return prof
