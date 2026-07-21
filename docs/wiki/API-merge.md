# API: merge

## `resolve_parquet_columns`

```python
resolve_parquet_columns(
    path_parquet,
    required_columns,
    equivalences=None,
    strict=True,
)
```

Resuelve columnas físicas contra nombres canónicos leyendo únicamente el esquema.

| Argumento | Uso |
|---|---|
| `path_parquet` | archivo que se inspeccionará |
| `required_columns` | nombres canónicos requeridos |
| `equivalences` | alternativas históricas por nombre canónico |
| `strict` | error si falta una columna; con `False` devuelve `OK=False` |

Retorna rutas, dimensiones, columnas disponibles y requeridas, mapas de lectura y renombrado, equivalencias, faltantes y `OK`.

## `merge_period_parquet`

```python
merge_period_parquet(
    periodo,
    path_coe2,
    path_sdem,
    cols_coe2,
    cols_sdem,
    llave_persona,
    path_out="../Merge_de_parquet",
    equivalences=None,
    overwrite=False,
    compression="zstd",
    strict_keys=True,
    *,
    generate_metadata=False,
    path_paradata="../Paradata",
)
```

Ejecuta un `LEFT JOIN` con SDEM como tabla madre y escribe directamente a Parquet.

| Argumento | Uso |
|---|---|
| `periodo` | trimestre `AAAATn` |
| `path_coe2`, `path_sdem` | fuentes Parquet |
| `cols_coe2`, `cols_sdem` | selección canónica por tabla |
| `llave_persona` | columnas del join |
| `path_out` | raíz de salida |
| `equivalences` | nombres históricos alternativos |
| `overwrite` | reemplazar salida existente |
| `compression` | compresión, por defecto `zstd` |
| `strict_keys` | detener por nulos o duplicados en llaves |
| `generate_metadata` | persistir documentación de calidad |
| `path_paradata` | raíz de YAML |

Retorna un diccionario con ruta y métricas del merge.

## `build_merged_time_series`

```python
build_merged_time_series(
    time_series,
    cols_coe2,
    cols_sdem,
    llave_persona,
    path_out="../Merge_de_parquet",
    equivalences=None,
    periods=None,
    overwrite=False,
    reuse_existing=True,
    compression="zstd",
    strict_keys=True,
    stop_on_error=True,
    return_audit=True,
    *,
    generate_metadata=False,
    path_paradata="../Paradata",
)
```

Procesa varios trimestres sin concatenarlos.

| Argumento | Uso |
|---|---|
| `time_series` | fuentes por periodo |
| `periods` | subconjunto; `None` procesa todos |
| `reuse_existing` | reutilizar salida cuando no se sobrescribe |
| `stop_on_error` | detener la serie al primer fallo |
| `return_audit` | retornar también DataFrame de auditoría |
| restantes | se transfieren al merge individual |

Con `return_audit=True` retorna `(merged_sources, audit)`. Con `False`, solo `merged_sources`.

