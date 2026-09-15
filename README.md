# CMS Reimbursements

Ingestion pipelines for CMS Medicare surgical-specialty data (HCPCS × specialty ×
year) and Medicare Physician Fee Schedule (PFS) reimbursements.

## Install

```bash
pip install -r requirements.txt
```

## Run the pipelines

```bash
# CMS: HCPCS x Specialty x Year aggregate -> cms/cms_surgical_hcpcs_year.parquet
python cms/ingest.py

# PFS: specialty reimbursement schedule -> pfs/pfs_all_specialties.parquet
python pfs/main.py
```

## Load output into memory

```python
import pandas as pd

cms = pd.read_parquet("cms/cms_surgical_hcpcs_year.parquet")
pfs = pd.read_parquet("pfs/pfs_all_specialties.parquet")
```
