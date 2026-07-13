# ENOE Utilities

Utilities for processing microdata from Mexico's **National Occupation and Employment Survey (ENOE)**, converting raw ZIP/CSV files to Parquet, merging SDEM and COE2, building labor-poverty analytical datasets, computing quarterly labor poverty indicators, and generating auditable JSON metadata and YAML paradata.

## Current status

`enoe-utilities` is an early-stage utility library extracted from a labor poverty pipeline whose core calculations were validated against official INEGI/CONEVAL results. The current package focuses on making that validated workflow reusable as a Python library without changing its statistical or methodological rules.

## Main features

- Convert ENOE ZIP/CSV files into Parquet.
- Build quarterly LPEI tables.
- Validate required columns across historical ENOE schemas.
- Resolve historical column-name equivalences such as `fac`/`fac_tri`, `t_loc`/`t_loc_tri`, and `ent`/`cve_ent`.
- Merge SDEM and COE2 using SDEM as the mother table.
- Build analytical Parquet datasets for labor poverty analysis.
- Compute quarterly labor poverty indicators.
- Build historical concatenated Parquet datasets.
- Generate JSON metadata and YAML paradata.
- Track quality indicators for income, imputation, exclusion, expansion factors, LPEI assignment, joins, final indicators, and historical consistency.

## Installation

Clone the repository and install it in editable mode:

```bash
git clone https://github.com/DevArmando99/enoe-utilities.git
cd enoe-utilities
pip install -e .
```

Python 3.10 or newer is required.

## Basic usage

```python
from pathlib import Path

from enoe_utilities import (
    get_time_series,
    get_pl_series,
    build_lpei_quarterly_long,
    validate_required_columns,
    build_merged_time_series,
    build_analysis_dataset,
    compute_pl_time_series,
    build_historical_analysis_parquet,
)

PATH_ZIPS = Path("../Archivos_Zip")
PATH_PARQUETS = Path("../Datos_Parquet")
PATH_MERGED = Path("../Merge_de_parquet")
PATH_ANALYSIS = Path("../Analisis_PL_parquet")
PATH_RESULTS = Path("../Resultados_PL")
PATH_HISTORICAL = Path("../Serie_Historica_PL")
PATH_PARADATA = Path("../Paradata")
PATH_LPEI_CSV = Path("../LPEI/serie_lpei.csv")

time_series = get_time_series(
    path_in=PATH_ZIPS,
    path_out=PATH_PARQUETS,
    generate_metadata=True,
    path_paradata=PATH_PARADATA,
)

lpei_monthly = get_pl_series(PATH_LPEI_CSV)
lpei_quarterly = build_lpei_quarterly_long(lpei_monthly)

# Define the canonical columns and keys required by your workflow.
validation = validate_required_columns(
    time_series=time_series,
    cols_coe2=COLS_COE2,
    cols_sdem=COLS_SDEM,
)

merged_sources, merge_audit = build_merged_time_series(
    time_series=time_series,
    cols_coe2=COLS_COE2,
    cols_sdem=COLS_SDEM,
    llave_persona=LLAVE_PERSONA,
    path_out=PATH_MERGED,
    generate_metadata=True,
    path_paradata=PATH_PARADATA,
)

analysis_sources = {}

for period, merged_path in merged_sources.items():
    result = build_analysis_dataset(
        periodo=period,
        path_merged=merged_path,
        lpei_quarterly=lpei_quarterly,
        llave_persona=LLAVE_PERSONA,
        llave_hogar=LLAVE_HOGAR,
        path_out=PATH_ANALYSIS,
        generate_metadata=True,
        path_paradata=PATH_PARADATA,
    )
    analysis_sources[period] = result["path_output"]

EXCLUDED_PERIODS = ["2020T2"]

pl_series = compute_pl_time_series(
    analysis_sources=analysis_sources,
    path_out=PATH_RESULTS,
    generate_metadata=True,
    path_paradata=PATH_PARADATA,
    excluded_periods=EXCLUDED_PERIODS,
)

historical = build_historical_analysis_parquet(
    analysis_sources=analysis_sources,
    path_out=PATH_HISTORICAL,
    generate_metadata=True,
    path_paradata=PATH_PARADATA,
    excluded_periods=EXCLUDED_PERIODS,
)
```

The paths, canonical column lists, and key definitions must be adapted to the local ENOE files. The `excluded_periods` argument is documentary for paradata and does not alter the calculation by itself.

## Methodological note

The package preserves the validated labor poverty workflow:

- SDEM is used as the mother table.
- COE2 provides labor and income variables.
- `p6b2` is prioritized as exact monthly labor income.
- `p6c` is used to recover income through wage-range imputation when the required conditions are met.
- Unpaid workers are handled according to the validated occupation and income rules.
- Households with unrecoverable labor income are excluded from the official indicator denominator.
- `fac_tri` is used as the quarterly expansion factor.
- Rural and urban LPEI values are assigned by locality scope.
- The final indicator is computed as the weighted share of eligible people living in households whose per-capita labor income is below the corresponding LPEI.
- JSON metadata and YAML paradata are generated for auditability and reproducibility.

## Quality indicators

Generated JSON metadata may include the following quality dimensions:

- conversion quality;
- schema quality;
- critical-variable quality;
- join quality;
- key quality;
- income-input availability;
- income recovery and imputation quality;
- exclusion quality;
- expansion-factor quality;
- LPEI assignment quality;
- final-indicator quality;
- temporal and historical consistency.

## Public API

The main functions are available directly from the package:

```python
from enoe_utilities import get_time_series
from enoe_utilities import build_analysis_dataset
from enoe_utilities import compute_pl_time_series
```

They can also be imported from their modules:

```python
from enoe_utilities.io import get_time_series
from enoe_utilities.merge import merge_period_parquet
from enoe_utilities.analysis import build_analysis_dataset
from enoe_utilities.indicators import compute_pl_time_series
from enoe_utilities.historical import build_historical_analysis_parquet
```

## Repository structure

```text
enoe-utilities/
├── README.md
├── pyproject.toml
├── LICENSE
├── .gitignore
├── enoe_utilities/
│   ├── __init__.py
│   ├── io.py
│   ├── lpei.py
│   ├── validation.py
│   ├── merge.py
│   ├── analysis.py
│   ├── indicators.py
│   ├── historical.py
│   ├── metadata.py
│   └── utils.py
└── examples/
    └── basic_pipeline.py
```

## Roadmap

- Add unit tests.
- Add sample notebooks.
- Add complete API documentation.
- Add CI checks.
- Publish package documentation.
- Add type hints progressively.

## License

This project is licensed under the BSD 3-Clause License. See [`LICENSE`](LICENSE).
