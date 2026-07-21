# API pública

La versión `0.1.0` expone doce funciones desde `enoe_utilities`. Las funciones cuyo nombre empieza con `_` son auxiliares internos y pueden cambiar sin aviso.

```python
from enoe_utilities import (
    get_time_series,
    get_pl_series,
    build_lpei_quarterly_long,
    validate_required_columns,
    validate_merged_period,
    resolve_parquet_columns,
    merge_period_parquet,
    build_merged_time_series,
    build_analysis_dataset,
    compute_pl_period,
    compute_pl_time_series,
    build_historical_analysis_parquet,
)
```

## Índice

| Módulo | Funciones | Documentación |
|---|---|---|
| `io` | `get_time_series` | [API: entrada y LPEI](https://github.com/DevArmando99/enoe-utilities/wiki/API-entrada-y-LPEI) |
| `lpei` | `get_pl_series`, `build_lpei_quarterly_long` | [API: entrada y LPEI](https://github.com/DevArmando99/enoe-utilities/wiki/API-entrada-y-LPEI) |
| `validation` | `validate_required_columns`, `validate_merged_period` | [API: validación](https://github.com/DevArmando99/enoe-utilities/wiki/API-validacion) |
| `merge` | `resolve_parquet_columns`, `merge_period_parquet`, `build_merged_time_series` | [API: merge](https://github.com/DevArmando99/enoe-utilities/wiki/API-merge) |
| `analysis` | `build_analysis_dataset` | [API: análisis e indicadores](https://github.com/DevArmando99/enoe-utilities/wiki/API-analisis-e-indicadores) |
| `indicators` | `compute_pl_period`, `compute_pl_time_series` | [API: análisis e indicadores](https://github.com/DevArmando99/enoe-utilities/wiki/API-analisis-e-indicadores) |
| `historical` | `build_historical_analysis_parquet` | [API: histórico](https://github.com/DevArmando99/enoe-utilities/wiki/API-historico) |

## Convenciones

- `periodo` usa el formato `AAAAT1` a `AAAAT4`.
- Las rutas aceptan `str` o `pathlib.Path`, salvo donde la firma indique algo más específico.
- `overwrite=False` protege archivos existentes.
- `generate_metadata=True` activa persistencia documental adicional.
- `path_paradata` define la raíz de los YAML de ejecución.
- Los callbacks reciben estructuras de eventos, metadatos o paradatos; `strict_hooks=True` convierte sus fallas en errores de la etapa.

## Dependencias

Python 3.10 o posterior, `pandas`, `pyarrow`, `duckdb` y `PyYAML`.

```bash
python -m pip install -e .
```

