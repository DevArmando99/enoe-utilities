# Labor Poverty Pipeline Example (2025-2026)

[Leer en español](README.es.md)

This directory contains a complete, bilingual example of the labor poverty workflow implemented with the public API of `enoe-utilities`. The notebooks process quarterly ENOE microdata from raw ZIP/CSV inputs through conversion, schema validation, SDEM-COE2 merging, analytical dataset construction, labor poverty indicators, historical concatenation, metadata, and paradata.

## Notebooks

- `labor_poverty_pipeline_en.ipynb`: English version.
- `labor_poverty_pipeline_es.ipynb`: Spanish version.

Both notebooks implement the same pipeline and are intended to be run independently. Restart the kernel before switching languages so that variables and open Parquet handles from one execution do not affect the other.

## Complete runnable folder

The raw microdata, converted Parquet files, and generated outputs are too large for this Git repository. The complete runnable folder is available in [Google Drive](https://drive.google.com/drive/folders/1WyXtJdbN__LyOo1la5rE7mNs5r3bFE4m?usp=sharing).

You can either run the downloaded folder as a standalone example or copy its `data/` contents into this directory while preserving the structure shown below. Generated files remain local and are excluded by this directory's `.gitignore`.

## Directory structure

```text
labor_poverty_2025_2026/
├── README.md
├── README.es.md
├── labor_poverty_pipeline_en.ipynb
├── labor_poverty_pipeline_es.ipynb
├── docs/
├── data/
│   ├── raw/
│   │   ├── zip/
│   │   └── lpei/
│   └── parquet/
│       ├── converted/
│       ├── merged/
│       ├── analytical/
│       └── historical/
└── outputs/
    ├── indicators/
    ├── metadata/
    ├── paradata/
    └── validation/
```

## Requirements

- Python 3.10 or newer.
- JupyterLab or Jupyter Notebook.
- An editable installation of this repository and its dependencies.

From the repository root:

```bash
python -m pip install -e .
python -m pip install jupyterlab
```

## Run the example

1. Download the complete folder from Google Drive, or place the required ENOE ZIP files and LPEI CSV in the matching `data/raw/` directories.
2. Open a terminal in `examples/labor_poverty_2025_2026/`.
3. Start Jupyter with `jupyter lab` or `jupyter notebook`.
4. Open either language version and run the cells in order from the beginning.

The notebooks must be executed with `examples/labor_poverty_2025_2026/` as the working directory. Do not run them from `data/`, `outputs/`, or the notebook checkpoint directory.

The example covers `2025T1`, `2025T2`, `2025T3`, `2025T4`, and `2026T1`. Existing derived files may be overwritten according to the notebook configuration. Close any other notebook kernel or application that is using a generated Parquet file before rerunning the pipeline.

## Data and generated artifacts

The repository tracks the notebooks, documentation, directory placeholders, and ignore rules only. It intentionally does not track:

- raw ENOE ZIP/CSV microdata;
- converted, merged, analytical, or historical Parquet files;
- generated JSON metadata or YAML paradata;
- validation tables and indicator outputs.

This keeps the Git history lightweight and avoids redistributing large survey files. Consult the source institutions' terms and documentation before redistributing any downloaded data.

## Methodological references

The microdata used by this pipeline come from the **National Survey of Occupation and Employment (ENOE)**. Its technical documentation and methodology are available on the [official INEGI ENOE page](https://www.inegi.org.mx/programas/enoe/15ymas/).

The additional INEGI technical reference supplied with this example is the [Monthly Survey of the Manufacturing Industry (EMIM), 2018 series](https://www.inegi.org.mx/programas/emim/2018/). EMIM and ENOE are different statistical programs; the EMIM link is retained here as an additional reference and is not the methodology for the ENOE microdata processed by these notebooks.

## Reproducibility note

The Google Drive folder is a convenience copy of the worked example. For research or production use, obtain the official source data directly from the responsible institutions, record the download date and source version, and preserve the generated metadata and paradata alongside the results.
