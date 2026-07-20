# API: análisis e indicadores

## `build_analysis_dataset`

```python
build_analysis_dataset(
    periodo,
    path_merged,
    lpei_quarterly,
    llave_persona,
    llave_hogar,
    path_out="../Analisis_PL_parquet",
    overwrite=False,
    compression="zstd",
    strict=True,
    event_callback=None,
    metadata_callback=None,
    paradata_callback=None,
    strict_hooks=False,
    *,
    generate_metadata=False,
    path_paradata="../Paradata",
)
```

Construye el Parquet persona-trimestre con ingreso, hogar, LPEI, elegibilidad y pobreza laboral.

| Argumento | Uso |
|---|---|
| `periodo` | trimestre procesado |
| `path_merged` | Parquet unido validado |
| `lpei_quarterly` | LPEI en formato ancho o largo |
| `llave_persona`, `llave_hogar` | claves canónicas |
| `path_out` | raíz de salida analítica |
| `overwrite`, `compression` | escritura Parquet |
| `strict` | detener por inconsistencias críticas |
| callbacks | receptores opcionales de eventos y documentación |
| `strict_hooks` | propagar errores de callbacks |
| `generate_metadata` | persistir JSON y YAML |
| `path_paradata` | raíz de paradatos |

Retorna `path_output`, métricas, metadatos, paradatos y eventos.

## `compute_pl_period`

```python
compute_pl_period(
    periodo,
    path_analysis,
    lpei_quarterly=None,
    round_digits=2,
)
```

| Argumento | Uso |
|---|---|
| `periodo` | trimestre esperado |
| `path_analysis` | Parquet analítico |
| `lpei_quarterly` | control opcional de existencia de LPEI |
| `round_digits` | decimales del porcentaje |

Retorna un DataFrame de una fila con `Año`, `Trimestre` y `PL`.

## `compute_pl_time_series`

```python
compute_pl_time_series(
    analysis_sources,
    periods=None,
    start_period=None,
    end_period=None,
    lpei_quarterly=None,
    path_out="../Resultados_PL",
    output_name=None,
    save_csv=True,
    save_parquet=True,
    overwrite=False,
    round_digits=2,
    stop_on_error=False,
    require_complete=False,
    event_callback=None,
    metadata_callback=None,
    paradata_callback=None,
    strict_hooks=False,
    *,
    generate_metadata=False,
    path_paradata="../Paradata",
    excluded_periods=None,
)
```

| Argumento | Uso |
|---|---|
| `analysis_sources` | carpeta, diccionario, lista de rutas o pares periodo-ruta |
| `periods` | lista explícita |
| `start_period`, `end_period` | rango inclusivo alternativo |
| `lpei_quarterly` | control metodológico opcional |
| `path_out`, `output_name` | destino y nombre base |
| `save_csv`, `save_parquet` | formatos de salida |
| `overwrite`, `round_digits` | escritura y redondeo |
| `stop_on_error` | detener al primer periodo fallido |
| `require_complete` | exigir todos los periodos solicitados |
| callbacks, `strict_hooks` | integración externa |
| `generate_metadata`, `path_paradata` | persistencia documental |
| `excluded_periods` | periodos excluidos documentados |

Retorna `data`, `audit`, rutas, metadatos, paradatos, eventos, `OK` y `completo`.

## Nota estadística

Las métricas de error estándar e intervalo de confianza generadas en metadatos son aproximaciones de proporción simple. No representan la varianza completa del diseño complejo de la ENOE.

