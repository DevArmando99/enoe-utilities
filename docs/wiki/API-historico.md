# API: histórico

## `build_historical_analysis_parquet`

```python
build_historical_analysis_parquet(
    analysis_sources="../Analisis_PL_parquet",
    periods=None,
    start_period=None,
    end_period=None,
    path_out="../Serie_Historica_PL",
    output_name=None,
    overwrite=False,
    compression="zstd",
    row_group_size=100000,
    strict_schema=True,
    required_columns=None,
    validate_content=True,
    require_complete=False,
    stop_on_error=False,
    save_manifest=True,
    order_by_period=True,
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

Concatena Parquet analíticos trimestrales en un archivo persona-trimestre.

| Argumento | Uso |
|---|---|
| `analysis_sources` | carpeta, diccionario, lista de rutas o pares periodo-ruta |
| `periods` | lista explícita de trimestres |
| `start_period`, `end_period` | rango inclusivo alternativo |
| `path_out`, `output_name` | destino y nombre base |
| `overwrite`, `compression` | escritura Parquet |
| `row_group_size` | filas por grupo, por defecto 100000 |
| `strict_schema` | exigir columnas idénticas y en el mismo orden |
| `required_columns` | columnas mínimas; usa un conjunto predeterminado si es `None` |
| `validate_content` | verificar periodo, año y trimestre dentro de cada archivo |
| `require_complete` | exigir todas las fuentes solicitadas |
| `stop_on_error` | detener al primer archivo inválido |
| `save_manifest` | guardar auditoría CSV de fuentes |
| `order_by_period` | ordenar por año y trimestre |
| callbacks, `strict_hooks` | integración externa |
| `generate_metadata`, `path_paradata` | persistencia documental |
| `excluded_periods` | exclusiones conocidas registradas |

Retorna `path_output`, `audit`, `path_manifest`, metadatos, paradatos, eventos, `OK` y `completo`.

## Recomendación

Para una serie auditable utiliza:

```python
historical = build_historical_analysis_parquet(
    analysis_sources=analysis_sources,
    periods=EXPECTED_PERIODS,
    path_out=HISTORICAL_DIR,
    overwrite=True,
    strict_schema=True,
    validate_content=True,
    require_complete=False,
    stop_on_error=True,
    save_manifest=True,
    order_by_period=True,
    generate_metadata=True,
    path_paradata=PARADATA_DIR,
    excluded_periods=["2020T2"],
)
```

Si se ejecutan los notebooks en dos idiomas sobre las mismas rutas, reinicia el kernel y decide explícitamente si se sobrescribirán los archivos derivados.

