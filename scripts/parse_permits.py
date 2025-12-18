#!/usr/bin/env python3
import argparse
import os
import re
import sys
import csv

DEFAULT_INPUTS = ["data/permits_latest.csv", "permits_latest.csv"]

def norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def up(s: str) -> str:
    return norm(s).upper()

def tier(score: int) -> str:
    # Simple tiers for quick scanning
    if score >= 85: return "A"
    if score >= 70: return "B"
    if score >= 55: return "C"
    return "D"

def score_permit_type(pt_raw: str) -> tuple[int, str]:
    """
    Score dumpsters based ONLY on Permit Type (current data reality).
    Returns (0-100 score, reason string).
    """
    pt = up(pt_raw)

    # Strong indicators
    if "DEMOLITION" in pt:
        return 98, "Demolition = high debris"
    if "NEW CONSTRUCTION" in pt:
        return 92, "New construction = high debris"
    if "COMMERCIAL INTERIOR UPFIT" in pt or ("UPFIT" in pt and "COMMERCIAL" in pt):
        return 86, "Commercial upfit = tear-out debris"
    if "ADDITION" in pt:
        return 84, "Addition = construction debris"
    if "ACCESSORY STRUCTURE" in pt or "ACESSORY STRUCTURE" in pt:
        return 76, "Accessory structure = framing/debris"
    if "SWIMMING POOL" in pt:
        return 78, "Pool install = excavation/packaging debris"

    # Medium indicators
    if "EXTERIOR" in pt and "ALTER" in pt:
        return 70, "Exterior alteration often creates debris (varies)"
    if "INTERIOR" in pt and "ALTER" in pt:
        return 66, "Interior alteration often creates debris (varies)"
    if "REROOF" in pt or "RE-ROOF" in pt:
        return 60, "Reroof sometimes uses dumpster (contractor-dependent)"
    if "MANUFACTURED HOME" in pt and ("SET UP" in pt or "SETUP" in pt):
        return 52, "Manufactured setup may create packaging/debris"

    # Low value / usually not dumpster-worthy
    if "FEASIBILITY" in pt:
        return 10, "Feasibility = planning, no debris"
    if "STANDAL" in pt or "STANDALONE" in pt:
        return 25, "Standalone trade permit often no dumpster"

    # Fallback
    return 40, "Unclassified permit type"

def find_input(path_arg: str | None) -> str:
    if path_arg:
        if os.path.exists(path_arg):
            return path_arg
        raise SystemExit(f"Input not found: {path_arg}")

    for p in DEFAULT_INPUTS:
        if os.path.exists(p):
            return p
    raise SystemExit(f"Input not found. Looked for: {DEFAULT_INPUTS}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=None, help="Path to permits_latest.csv")
    ap.add_argument("--out", default="data/permits_scored.csv", help="Output CSV path")
    args = ap.parse_args()

    inp = find_input(args.input)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    with open(inp, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        if not rows:
            raise SystemExit("No rows found in input CSV.")
        headers = reader.fieldnames or list(rows[0].keys())

    # Locate Permit Type column robustly
    pt_col = None
    for h in headers:
        if up(h) in ("PERMIT TYPE", "PERMIT_TYPE"):
            pt_col = h
            break
    if not pt_col:
        raise SystemExit(f"Could not find 'Permit Type' column. Columns: {headers}")

    # Add new columns
    out_rows = []
    type_counts = {}
    tier_counts = {"A":0, "B":0, "C":0, "D":0}

    for r in rows:
        pt = r.get(pt_col, "")
        s, reason = score_permit_type(pt)
        t = tier(s)

        type_key = up(pt)
        type_counts[type_key] = type_counts.get(type_key, 0) + 1
        tier_counts[t] += 1

        rr = dict(r)
        rr["dumpster_score"] = s
        rr["dumpster_tier"] = t
        rr["dumpster_reason"] = reason
        out_rows.append(rr)

    # Sort high-to-low for convenience (still includes all)
    out_rows.sort(key=lambda x: int(x.get("dumpster_score", 0)), reverse=True)

    out_headers = headers + ["dumpster_score", "dumpster_tier", "dumpster_reason"]

    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=out_headers)
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    # Console summary
    print(f"Input: {inp}")
    print(f"Rows: {len(rows)}")
    print("Tier counts:", tier_counts)
    print("\nPermit Type counts (top):")
    for k, v in sorted(type_counts.items(), key=lambda kv: kv[1], reverse=True)[:15]:
        print(f"  {k} -> {v}")
    print(f"\nWrote: {args.out}")

if __name__ == "__main__":
    main()
