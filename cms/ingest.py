#!/usr/bin/env python3
"""Ingest CMS Medicare Physician & Other Practitioners (by Provider and Service)
for 11 surgical specialties, 2013-2024, and write one HCPCS x Specialty x Year parquet.

Pipeline (four methods):
  get_metadata()  -> discover per-year dataset UUIDs + actual specialty names
  make_url()      -> build a dataset /data URL with offset (and optional filter)
  fanout()        -> asyncio fan-out: fetch, post-process, stream to per-unit NDJSON
  write_output()  -> consolidate to one row per (HCPCS_Cd, Specialty, Year) and write parquet
"""
import argparse
import asyncio
import json
import os
import re
import time
import unicodedata
import urllib.parse
from pathlib import Path

import aiofiles
import httpx
import pandas as pd

BASE = "https://data.cms.gov/data-api/v1/dataset"
CATALOG = "https://data.cms.gov/data.json"
TITLE = "Medicare Physician & Other Practitioners - by Provider and Service"
PAGE = 1000          # API currently ignores `limit` and caps at 1000 rows/page
WAVE = 8             # in-flight pages per unit (fan-out within a specialty)
RETRIES = 4
UA = "cms-ingest/1.0"
HERE = Path(__file__).resolve().parent

# canonical -> (exact variants, precise case-insensitive regex, CONTAINS keyword)
SPECIALTIES = {
    "Colorectal Surgery (Proctology)": (
        ["Colorectal Surgery (Proctology)", "Colorectal Surgery (formerly proctology)"],
        "colorectal", "Colorectal"),
    "General Surgery": (["General Surgery"], "general surgery", "General Surgery"),
    "Neurosurgery": (["Neurosurgery"], "neurosurgery|neurological surgery", "Neurosurgery"),
    "Obstetrics/Gynecology": (
        ["Obstetrics & Gynecology", "Obstetrics/Gynecology"], "obstetric|gynecolog", "Gynecology"),
    "Ophthalmology": (["Ophthalmology"], "ophthalmolog", "Ophthalmology"),
    "Orthopedic Surgery": (["Orthopedic Surgery", "Orthopaedic Surgery"], "orthop", "Orthopedic"),
    "Otolaryngology": (["Otolaryngology"], "otolaryngolog", "Otolaryngology"),
    "Plastic and Reconstructive Surgery": (
        ["Plastic and Reconstructive Surgery"], "plastic|reconstructive", "Plastic"),
    "Thoracic Surgery": (["Thoracic Surgery"], "thoracic surgery", "Thoracic Surgery"),
    "Urology": (["Urology"], r"\burology\b", "Urology"),
    "Vascular Surgery": (["Vascular Surgery"], "vascular surgery", "Vascular Surgery"),
}

COUNTS = ["Tot_Benes", "Tot_Srvcs", "Tot_Bene_Day_Srvcs"]
AVGS = ["Avg_Sbmtd_Chrg", "Avg_Mdcr_Alowd_Amt", "Avg_Mdcr_Pymt_Amt", "Avg_Mdcr_Stdzd_Amt"]
KEEP = ["HCPCS_Cd", "HCPCS_Desc", "Place_Of_Srvc", "Rndrng_Prvdr_Type", "Rndrng_NPI"] + COUNTS + AVGS


def _slug(s):
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")


def _clean(s):
    return " ".join(unicodedata.normalize("NFKC", str(s or "")).split())


def _parse_years(s):
    ys = set()
    for part in s.split(","):
        part = part.strip()
        a, _, b = part.partition("-")
        ys.update(range(int(a), int(b) + 1)) if b else ys.add(int(a))
    return sorted(ys)


# ---------------------------------------------------------------- method 2
def make_url(uuid, offset=0, provider_type=None, **extra):
    q = {"limit": PAGE, "offset": offset, **extra}
    if provider_type is not None:
        q["filter[Rndrng_Prvdr_Type]"] = provider_type
    return f"{BASE}/{uuid}/data?" + urllib.parse.urlencode(q)


async def _get_json(client, sem, url):
    last = None
    for attempt in range(RETRIES):
        try:
            async with sem:
                r = await client.get(url)
            if r.status_code in (429, 500, 502, 503, 504):
                ra = r.headers.get("Retry-After")
                await asyncio.sleep(float(ra) if ra and ra.isdigit() else min(2 ** attempt, 20))
                continue
            r.raise_for_status()
            return r.json()
        except (httpx.HTTPError, asyncio.TimeoutError) as e:
            last = e
            await asyncio.sleep(min(2 ** attempt, 20))
    raise RuntimeError(f"GET failed after retries: {url} ({last})")


# ---------------------------------------------------------------- method 1
async def _resolve(client, sem, uuid, canonical):
    variants, regex, kw = SPECIALTIES[canonical]
    rx = re.compile(regex, re.I)
    for v in variants:
        rows = await _get_json(client, sem, make_url(uuid, 0, v))
        if rows:
            return v
    rows = await _get_json(client, sem, make_url(
        uuid, 0,
        **{"filter[x][condition][path]": "Rndrng_Prvdr_Type",
           "filter[x][condition][operator]": "CONTAINS",
           "filter[x][condition][value]": kw}))
    types = sorted({r.get("Rndrng_Prvdr_Type", "") for r in rows
                    if rx.search(r.get("Rndrng_Prvdr_Type", "") or "")})
    if len(types) != 1:
        raise RuntimeError(f"Cannot resolve '{canonical}' for {uuid}: found {types}")
    return types[0]


async def get_metadata(client, sem, data_dir, years, refresh):
    meta_path = data_dir / "meta.json"
    data_dir.mkdir(parents=True, exist_ok=True)
    cached = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    need = {str(y) for y in years}
    if not refresh and need <= set(cached.get("uuids", {})) and need <= set(cached.get("types", {})):
        return cached

    cat = await _get_json(client, sem, CATALOG)
    ds = next((d for d in cat.get("dataset", []) if d.get("title") == TITLE), None) or \
        next((d for d in cat.get("dataset", []) if "by-provider-and-service" in d.get("identifier", "")), None)
    if ds is None:
        raise RuntimeError("Dataset not found in data.json catalog")

    parent = ds.get("identifier", "").split("/dataset/")[-1].split("/")[0]
    by_year = {}
    for dist in ds.get("distribution", []):
        if dist.get("format") != "API" or not dist.get("accessURL"):
            continue
        m = re.match(r"(\d{4})-", dist.get("temporal") or "")
        if not m:
            continue
        uuid = dist["accessURL"].rstrip("/").split("/dataset/")[-1].split("/")[0]
        by_year.setdefault(int(m.group(1)), []).append(uuid)

    meta = {"uuids": dict(cached.get("uuids", {})), "types": dict(cached.get("types", {}))}
    for y in years:
        cands = by_year.get(y, [])
        if not cands:
            raise RuntimeError(f"No API dataset found for year {y}")
        meta["uuids"][str(y)] = parent if parent in cands else cands[0]
        if len(cands) > 1:
            print(f"[meta] year {y}: {len(cands)} datasets -> {meta['uuids'][str(y)]}")

    for y in years:
        if refresh or str(y) not in meta["types"]:
            names = await asyncio.gather(*[_resolve(client, sem, meta["uuids"][str(y)], c) for c in SPECIALTIES])
            meta["types"][str(y)] = dict(zip(SPECIALTIES, names))

    meta_path.write_text(json.dumps(meta, indent=2))
    return meta


# ---------------------------------------------------------------- method 3
def _postprocess(row, year):
    out = {"Year": year}
    out["HCPCS_Cd"] = str(row.get("HCPCS_Cd") or "").strip().upper()
    out["HCPCS_Desc"] = _clean(row.get("HCPCS_Desc"))
    out["Place_Of_Srvc"] = row.get("Place_Of_Srvc")
    out["Rndrng_Prvdr_Type"] = row.get("Rndrng_Prvdr_Type")
    out["Rndrng_NPI"] = row.get("Rndrng_NPI")
    for k in COUNTS:
        v = row.get(k)
        out[k] = int(float(v)) if v not in (None, "") else 0
    for k in AVGS:
        v = row.get(k)
        out[k] = float(v) if v not in (None, "") else None
    return out


async def _page(client, sem, uuid, off, provider_type):
    rows = await _get_json(client, sem, make_url(uuid, off, provider_type))
    if not isinstance(rows, list):
        rows = []
    return off, rows


async def _fetch_unit(client, sem, year, uuid, provider_type, out_path):
    tmp = Path(str(out_path) + ".tmp")
    written = 0
    final = None
    offset = 0
    pending = {}

    async with aiofiles.open(tmp, "w") as f:
        while True:
            while (final is None or offset <= final) and len(pending) < WAVE:
                pending[offset] = asyncio.create_task(_page(client, sem, uuid, offset, provider_type))
                offset += PAGE
            if not pending:
                break
            done, _ = await asyncio.wait(pending.values(), return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                off, rows = t.result()
                if len(rows) < PAGE and (final is None or off < final):
                    final = off
                if final is None or off <= final:
                    rows = [r for r in rows if r.get("Rndrng_Prvdr_Type") == provider_type]
                    buf = [json.dumps(_postprocess(r, year), ensure_ascii=False) for r in rows]
                    if buf:
                        await f.write("\n".join(buf) + "\n")
                    written += len(rows)
                del pending[off]
            if final is not None:
                for off in [o for o in pending if o > final]:
                    pending[off].cancel()
                    del pending[off]

    os.replace(tmp, out_path)
    return written


async def fanout(client, sem, meta, data_dir, years, refresh=False):
    raw = data_dir / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    jobs = []
    for y in years:
        uuid = meta["uuids"][str(y)]
        for canonical in SPECIALTIES:
            out = raw / f"{y}__{_slug(canonical)}.jsonl"
            if out.exists() and not refresh:
                continue  # resume: unit already complete
            jobs.append((y, uuid, meta["types"][str(y)][canonical], out))
    print(f"[fanout] {len(jobs)} units to fetch ({len(years) * len(SPECIALTIES) - len(jobs)} cached)")

    async def safe(job):
        y, uuid, ptype, out = job
        try:
            n = await _fetch_unit(client, sem, y, uuid, ptype, out)
            print(f"[ok] {y} {ptype}: {n} rows")
        except Exception as e:
            print(f"[FAIL] {y} {ptype}: {e!r}")

    tasks = [asyncio.create_task(safe(j)) for j in jobs]
    for t in asyncio.as_completed(tasks):
        await t


# ---------------------------------------------------------------- method 4
def write_output(data_dir, out_path, years, meta):
    raw = Path(data_dir) / "raw"
    files = sorted(raw.glob("*.jsonl"))
    df = pd.concat([pd.read_json(f, lines=True) for f in files], ignore_index=True)
    df = df.drop_duplicates()                       # exact-dup guard (pagination reorder)
    df["HCPCS_Cd"] = df["HCPCS_Cd"].astype(str)
    # map each resolved Rndrng_Prvdr_Type back to its canonical specialty name
    rev = {resolved: canon for t in meta.get("types", {}).values() for canon, resolved in t.items()}
    df["Specialty"] = df["Rndrng_Prvdr_Type"].map(rev).fillna(df["Rndrng_Prvdr_Type"])

    g = df.groupby(["Year", "Specialty", "HCPCS_Cd"])
    den = g["Tot_Srvcs"].sum()
    out = pd.DataFrame({
        "Tot_Benes": g["Tot_Benes"].sum(),
        "Tot_Srvcs": den,
        "Tot_Bene_Day_Srvcs": g["Tot_Bene_Day_Srvcs"].sum(),
        "HCPCS_Desc": g["HCPCS_Desc"].apply(lambda s: s.mode().iloc[0] if not s.mode().empty else ""),
        "n_providers": g["Rndrng_NPI"].nunique(),
    })
    for k in AVGS:
        num = df.assign(v=df[k] * df["Tot_Srvcs"]).groupby(["Year", "Specialty", "HCPCS_Cd"])["v"].sum()
        out[k] = num / den.replace(0, pd.NA)

    out = out.reset_index().sort_values(["Year", "Specialty", "HCPCS_Cd"]).reset_index(drop=True)
    out["Year"] = out["Year"].astype("int16")
    out[COUNTS] = out[COUNTS].astype("int64")
    out["n_providers"] = out["n_providers"].astype("int32")

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, engine="pyarrow", index=False)

    yrs = sorted(out["Year"].unique())
    print(f"[write] {out_path}: {len(out)} rows | {len(files)} units | "
          f"years {yrs[0]}-{yrs[-1]} | dup keys {int(out.duplicated(['Year', 'Specialty', 'HCPCS_Cd']).sum())}")
    if len(files) != len(years) * len(SPECIALTIES):
        print(f"[warn] expected {len(years) * len(SPECIALTIES)} units, found {len(files)}")
    if set(yrs) != set(years):
        print(f"[warn] years {yrs} != expected {years}")
    if out["Specialty"].nunique() != len(SPECIALTIES):
        print(f"[warn] specialties present: {sorted(out['Specialty'].unique())}")
    return out


async def run(args):
    years = _parse_years(args.years)
    data_dir = Path(args.data_dir)
    limits = httpx.Limits(max_connections=2 * args.concurrency, max_keepalive_connections=args.concurrency)
    sem = asyncio.Semaphore(args.concurrency)
    async with httpx.AsyncClient(
            timeout=60, limits=limits, follow_redirects=True,
            headers={"User-Agent": UA, "Accept": "application/json"}) as client:
        meta = await get_metadata(client, sem, data_dir, years, args.refresh)
        await fanout(client, sem, meta, data_dir, years, args.refresh)
    write_output(data_dir, args.out, years, meta)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--years", default="2013-2024", help="e.g. 2013-2024 or 2015,2016")
    ap.add_argument("--concurrency", type=int, default=64)
    ap.add_argument("--refresh", action="store_true", help="ignore cached metadata/temp files")
    ap.add_argument("--data-dir", default=str(HERE / "data"))
    ap.add_argument("--out", default=str(HERE / "cms_surgical_hcpcs_year.parquet"))
    args = ap.parse_args(argv)
    t0 = time.time()
    asyncio.run(run(args))
    print(f"[done] {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
