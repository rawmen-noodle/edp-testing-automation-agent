"""Generate deterministic SAP M2C flat files (pipe-delimited, header + TRL trailer) for
testing the ingestion pipeline and the ABC framework.

    python scripts/generate_m2c_samples.py --out /mnt/test_share --set clean --rows 5000
    python scripts/generate_m2c_samples.py --out /mnt/test_share --set faulty
"""
from __future__ import annotations

import argparse
import random
from decimal import Decimal
from pathlib import Path

STAMP = "20261007"


def _amt(d: Decimal) -> str:
    # SAP style: sign after the number.
    return f"{abs(d):.2f}-" if d < 0 else f"{d:.2f}"


def _write(path: Path, header: list[str], rows: list[list[str]], trailer: list[str],
           encoding: str = "utf-8", raw_suffix: bytes = b"") -> Path:
    lines = ["|".join(header)] + ["|".join(r) for r in rows] + ["|".join(trailer)]
    path.write_bytes(("\n".join(lines) + "\n").encode(encoding) + raw_suffix)
    return path


def kna1(n: int, rnd: random.Random) -> tuple[list[str], list[list[str]]]:
    hdr = ["MANDT", "KUNNR", "NAME1", "LAND1", "ORT01", "KTOKD", "ERDAT"]
    cities = [("DE", "München"), ("FR", "Société Générale Paris"), ("US", "Chicago"), ("IN", "Pune")]
    rows = []
    for i in range(n):
        land, city = rnd.choice(cities)
        rows.append(["100", f"{1000000 + i:010d}", f"Customer {i} {city.split()[0]}", land, city,
                     "0001", "20260901"])
    return hdr, rows


def vbak(n: int, rnd: random.Random):
    hdr = ["MANDT", "VBELN", "ERDAT", "AUART", "VKORG", "VTWEG", "KUNNR", "NETWR", "WAERK"]
    rows, total = [], Decimal(0)
    for i in range(n):
        net = Decimal(rnd.randint(1000, 5_000_000)) / 100
        if rnd.random() < 0.05:  # credit memo requests carry negative values
            net = -net
        total += net
        rows.append(["100", f"{5000000 + i:010d}", "20261006", rnd.choice(["OR", "ZOR", "CR"]),
                     "1000", "10", f"{1000000 + rnd.randint(0, 999):010d}", _amt(net), "EUR"])
    return hdr, rows, total


def vbap(n: int, rnd: random.Random):
    hdr = ["MANDT", "VBELN", "POSNR", "MATNR", "KWMENG", "VRKME", "NETWR", "WAERK"]
    rows, total, qty = [], Decimal(0), Decimal(0)
    for i in range(n):
        q = Decimal(rnd.randint(1, 50000)) / 1000
        net = Decimal(rnd.randint(100, 900_000)) / 100
        total += net
        qty += q
        rows.append(["100", f"{5000000 + i // 3:010d}", f"{(i % 3 + 1) * 10:06d}",
                     f"MAT-{rnd.randint(1, 400):05d}", f"{q:.3f}", "EA", _amt(net), "EUR"])
    return hdr, rows, total, qty


def vbrk(n: int, rnd: random.Random):
    hdr = ["MANDT", "VBELN", "FKART", "FKDAT", "KUNRG", "NETWR", "MWSBK", "WAERK"]
    rows, total = [], Decimal(0)
    for i in range(n):
        net = Decimal(rnd.randint(1000, 2_000_000)) / 100
        total += net
        rows.append(["100", f"{9000000 + i:010d}", "F2", "20261006", f"{1000000 + i % 999:010d}",
                     _amt(net), _amt((net * Decimal("0.19")).quantize(Decimal("0.01"))), "EUR"])
    return hdr, rows, total


def bsid(n: int, rnd: random.Random):
    hdr = ["MANDT", "BUKRS", "KUNNR", "BELNR", "GJAHR", "BUZEI", "BUDAT", "SHKZG", "DMBTR", "WAERS"]
    rows, total = [], Decimal(0)
    for i in range(n):
        amt = Decimal(rnd.randint(100, 1_000_000)) / 100
        credit = rnd.random() < 0.3
        signed = -amt if credit else amt
        total += signed
        rows.append(["100", "1000", f"{1000000 + i % 999:010d}", f"{1400000000 + i}", "2026", "001",
                     "20261006", "H" if credit else "S", _amt(signed), "EUR"])
    return hdr, rows, total


def generate(out: Path, which: str, rows: int = 1000, seed: int = 42) -> list[Path]:
    rnd = random.Random(seed)
    out.mkdir(parents=True, exist_ok=True)
    made: list[Path] = []
    if which == "clean":
        h, r = kna1(rows, rnd)
        made.append(_write(out / f"KNA1_{STAMP}_001.txt", h, r, ["TRL", str(len(r))]))
        h, r, t = vbak(rows, rnd)
        made.append(_write(out / f"VBAK_{STAMP}_001.txt", h, r, ["TRL", str(len(r)), _amt(t)]))
        h, r, t, q = vbap(rows * 3, rnd)
        made.append(_write(out / f"VBAP_{STAMP}_001.txt", h, r, ["TRL", str(len(r)), _amt(t), f"{q:.3f}"]))
        h, r, t = vbrk(rows, rnd)
        made.append(_write(out / f"VBRK_{STAMP}_001.txt", h, r, ["TRL", str(len(r)), _amt(t)],
                           encoding="cp1252"))
        h, r, t = bsid(rows, rnd)
        made.append(_write(out / f"BSID_{STAMP}_001.txt", h, r, ["TRL", str(len(r)), _amt(t)]))
        h, _, _ = bsid(0, rnd)
        made.append(_write(out / f"BSID_{STAMP}_002.txt", h, [], ["TRL", "0", "0.00"]))
    elif which == "faulty":
        h, r, t = vbak(rows, rnd)  # trailer claims one more row than delivered
        made.append(_write(out / f"VBAK_{STAMP}_101.txt", h, r, ["TRL", str(len(r) + 1), _amt(t)]))
        h, r, t, q = vbap(rows, rnd)  # NETWR control total off by 10.00
        made.append(_write(out / f"VBAP_{STAMP}_102.txt", h, r,
                           ["TRL", str(len(r)), _amt(t + 10), f"{q:.3f}"]))
        h, r = kna1(rows, rnd)  # header drifted: column ORT01 dropped from extract
        made.append(_write(out / f"KNA1_{STAMP}_103.txt", [c for c in h if c != "ORT01"],
                           [x[:4] + x[5:] for x in r], ["TRL", str(len(r))]))
        h, r, t = vbak(rows, rnd)  # duplicate business key
        r[5] = list(r[4])
        t = sum((Decimal(x[7][:-1]) * -1 if x[7].endswith("-") else Decimal(x[7])) for x in r)
        made.append(_write(out / f"VBAK_{STAMP}_104.txt", h, r, ["TRL", str(len(r)), _amt(t)]))
        h, r, t = vbrk(rows, rnd)  # byte 0x81 is undefined in cp1252
        made.append(_write(out / f"VBRK_{STAMP}_105.txt", h, r, ["TRL", str(len(r)), _amt(t)],
                           encoding="cp1252", raw_suffix=b"100|\x81BAD\n"))
        h, r, t = bsid(rows, rnd)  # no trailer at all (truncated transfer)
        lines = ["|".join(h)] + ["|".join(x) for x in r]
        p = out / f"BSID_{STAMP}_106.txt"
        p.write_text("\n".join(lines) + "\n")
        made.append(p)
    else:
        raise ValueError(which)
    return made


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--set", choices=("clean", "faulty"), default="clean")
    ap.add_argument("--rows", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    for f in generate(a.out, a.set, a.rows, a.seed):
        print(f)
