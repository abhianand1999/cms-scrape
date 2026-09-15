#!/usr/bin/env python3
"""Medicare Physician Fee Schedule (PFS) reimbursement by surgical specialty, 2013-2024.

Pipeline:
  1) get_metadata() -> per-year PSPS CSV URLs + byte sizes (data.cms.gov catalog).
  2) build_urls()   -> offset (HTTP Range) chunk tasks for PSPS + one task per PFS RVU year.
  3) fanout()       -> parallel download -> filter -> temp parquet per task (resumable).
  4) write_output() -> join unique codes to PFS RVUs, compute prices, write merged parquet.
"""
import glob, io, os, re, time, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "data")
OUT = os.path.join(BASE, "pfs_all_specialties.parquet")
os.makedirs(DATA, exist_ok=True)

SPECIALTIES = {
    "Colorectal Surgery (Proctology)": "28", "General Surgery": "02", "Neurosurgery": "14",
    "Obstetrics/Gynecology": "16", "Ophthalmology": "18", "Orthopedic Surgery": "20",
    "Otolaryngology": "04", "Plastic and Reconstructive Surgery": "24", "Thoracic Surgery": "33",
    "Urology": "34", "Vascular Surgery": "77",
}
SPEC_CODES = set(SPECIALTIES.values())
SPEC_BYTES = {c.encode() for c in SPEC_CODES}
YEARS = list(range(2013, 2025))
CHUNK = 32 * 1024 * 1024          # 32 MB byte-range chunks for PSPS CSVs

# PFS "Relative Value File" (RVU A) zip per year; each contains a PPRRVU*.csv.
PFS_ZIPS = {y: f"https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/PhysicianFeeSched/Downloads/RVU{str(y)[2:]}A.ZIP"
            for y in range(2013, 2020)}
PFS_ZIPS.update({
    2020: "https://www.cms.gov/files/zip/rvu20a-updated-01312020.zip",
    2021: "https://www.cms.gov/files/zip/rvu21a-updated-01052021.zip",
    2022: "https://www.cms.gov/files/zip/rvu22a.zip",
    2023: "https://www.cms.gov/files/zip/rvu23a-updated-01/31/2023.zip",
    2024: "https://www.cms.gov/files/zip/rvu24a-updated-04/01/2024.zip",
})

S = requests.Session()
S.headers["User-Agent"] = "cms-pfs-pipeline/1.0"


def get(url, **kw):
    """GET with retries/backoff for transient network failures."""
    for i in range(4):
        try:
            r = S.get(url, timeout=180, **kw)
            if r.status_code in (200, 206):
                return r
        except requests.RequestException:
            pass
        if i < 3:
            print(f"  retry {i + 1}: {url}")
            time.sleep(2 * (i + 1))
    raise RuntimeError(f"GET failed: {url}")


# --- 1) metadata -------------------------------------------------------------
def get_metadata():
    cat = get("https://data.cms.gov/data.json").json()
    psps = next(d for d in cat["dataset"] if d["title"] == "Physician/Supplier Procedure Summary")
    urls = {}
    for dist in psps["distribution"]:
        if dist.get("format") == "CSV" and "downloadURL" in dist:
            m = re.search(r"(\d{4})-01-01", dist.get("temporal", "") or "")
            if m:
                urls[int(m.group(1))] = dist["downloadURL"]
    urls = {y: urls[y] for y in YEARS if y in urls}
    sizes = {y: _size(u) for y, u in urls.items()}
    print(f"metadata: {len(urls)} PSPS years")
    return urls, sizes


def _size(url):
    r = get(url, headers={"Range": "bytes=0-0"})
    return int(r.headers["Content-Range"].rsplit("/", 1)[1])


# --- 2) URL construction -----------------------------------------------------
def build_urls(urls, sizes):
    tasks = []
    for y, u in urls.items():
        n = sizes[y]
        for start in range(0, n, CHUNK):
            tasks.append(("psps", y, u, start, min(start + CHUNK, n) - 1))
    for y in YEARS:
        tasks.append(("pfs", y, None, 0, 0))
    return tasks


# --- 3) fanout ---------------------------------------------------------------
def worker(task):
    kind, y, url, start, end = task
    if kind == "psps":
        dst = os.path.join(DATA, f"codes_{y}_{start}.parquet")
        if not os.path.exists(dst):
            _psps_chunk(y, url, start, end, dst)
    else:
        dst = os.path.join(DATA, f"pfs_{y}.parquet")
        if not os.path.exists(dst):
            load_pfs(y).to_parquet(dst, index=False)


def _psps_chunk(year, url, start, end, dst):
    text = get(url, headers={"Range": f"bytes={start}-{end}"}).content
    if start:                                   # drop partial first line
        nl = text.find(b"\n")
        if nl < 0:
            return
        text = text[nl + 1:]
    nl = text.rfind(b"\n")                      # drop partial trailing line
    if nl >= 0:
        text = text[:nl]
    rows = set()
    for line in text.split(b"\n"):
        f = line.split(b",", 3)
        if len(f) > 2 and f[0] not in (b"HCPCS_CD", b"") and f[2] in SPEC_BYTES:
            rows.add((year, f[2].decode(), f[0].strip().decode().zfill(5)))
    pd.DataFrame(list(rows), columns=["year", "specialty_code", "hcpcs_code"]).to_parquet(dst, index=False)


def load_pfs(year):
    z = zipfile.ZipFile(io.BytesIO(get(PFS_ZIPS[year]).content))
    name = next(n for n in z.namelist() if n.upper().startswith("PPRRVU") and n.endswith(".csv"))
    raw = z.read(name)
    hdr = next(i for i, l in enumerate(raw.split(b"\n")) if l.startswith(b"HCPCS,MOD"))
    df = pd.read_csv(io.BytesIO(raw), skiprows=hdr, dtype=str)
    df = df.iloc[:, [0, 1, 2, 3, 5, 6, 8, 10, 11, 12, 13, 14, 24]]
    df.columns = ["hcpcs_code", "mod", "description", "status", "work_rvu", "nonfac_pe_rvu",
                  "fac_pe_rvu", "mp_rvu", "total_nonfac_rvu", "total_fac_rvu", "pctc_ind", "global_days", "cf"]
    df["hcpcs_code"] = df["hcpcs_code"].astype(str).str.strip().str.zfill(5)
    df["mod"] = df["mod"].fillna("").astype(str).str.strip()
    return df


def fanout(tasks):
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(worker, t): t for t in tasks}
        for fut in as_completed(futs):
            try:
                fut.result()
            except Exception as e:
                print("FAILED", futs[fut], repr(e)[:160])


# --- 4) output ---------------------------------------------------------------
def write_output():
    code2name = {v: k for k, v in SPECIALTIES.items()}
    frames = []
    for y in YEARS:
        codes = pd.concat([pd.read_parquet(f) for f in sorted(_glob(f"codes_{y}_*.parquet"))],
                          ignore_index=True).drop_duplicates()
        pfs = pd.read_parquet(os.path.join(DATA, f"pfs_{y}.parquet"))
        g = pfs[pfs["mod"] == ""]
        extra = pfs[~pfs["hcpcs_code"].isin(g["hcpcs_code"])].drop_duplicates("hcpcs_code")
        pfs = pd.concat([g, extra])
        m = codes.merge(pfs, on="hcpcs_code", how="left")
        m["year"] = y
        m["specialty_name"] = m["specialty_code"].map(code2name)
        m["unmatched_in_pfs"] = m["status"].isna()
        frames.append(m)
    out = pd.concat(frames, ignore_index=True)
    for c in ["work_rvu", "nonfac_pe_rvu", "fac_pe_rvu", "mp_rvu", "total_nonfac_rvu", "total_fac_rvu", "cf"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["nonfacility_price"] = out["cf"] * out["total_nonfac_rvu"]
    out["facility_price"] = out["cf"] * out["total_fac_rvu"]
    out = out.rename(columns={"mod": "mod_used", "status": "status_code", "cf": "conversion_factor",
                              "pctc_ind": "pctc_indicator"})
    cols = ["specialty_name", "specialty_code", "year", "hcpcs_code", "description", "status_code",
            "mod_used", "work_rvu", "nonfac_pe_rvu", "fac_pe_rvu", "mp_rvu", "total_nonfac_rvu",
            "total_fac_rvu", "global_days", "conversion_factor", "nonfacility_price", "facility_price",
            "pctc_indicator", "unmatched_in_pfs"]
    out[cols].to_parquet(OUT, index=False)
    print(f"wrote {OUT}: {len(out)} rows, {out['specialty_code'].nunique()} specialties")


def _glob(pat):
    return glob.glob(os.path.join(DATA, pat))


if __name__ == "__main__":
    urls, sizes = get_metadata()
    tasks = build_urls(urls, sizes)
    print(f"{len(tasks)} tasks")
    fanout(tasks)
    write_output()
