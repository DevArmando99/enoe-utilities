# API: validación

## `validate_required_columns`

```python
validate_required_columns(time_series, cols_coe2, cols_sdem)
```

Valida archivos y esquemas de SDEM y COE2 sin cargar todas sus filas.

| Argumento | Uso |
|---|---|
| `time_series` | diccionario generado por `get_time_series()` |
| `cols_coe2` | nombres canónicos requeridos de COE2 |
| `cols_sdem` | nombres canónicos requeridos de SDEM |

Retorna un DataFrame con periodo, tabla, existencia del archivo, número de columnas y filas, faltantes, equivalencias aplicadas, mapa de lectura y `OK`.

## `validate_merged_period`

```python
validate_merged_period(
    periodo,
    path_merged,
    llave_persona,
    llave_hogar=None,
    required_columns=None,
    coe2_columns=None,
    consistency_columns=None,
    min_occupied_match_rate=None,
    strict=False,
)
```

Inspecciona un Parquet unido mediante DuckDB y PyArrow sin modificarlo.

| Argumento | Uso |
|---|---|
| `periodo` | trimestre esperado |
| `path_merged` | Parquet SDEM-COE2 |
| `llave_persona` | llave canónica de persona |
| `llave_hogar` | llave de hogar; si es `None`, elimina `n_ren` de la llave personal |
| `required_columns` | columnas obligatorias; usa un mínimo metodológico si es `None` |
| `coe2_columns` | columnas para revisar consistencia de `cruce_coe2` |
| `consistency_columns` | variables constantes dentro del hogar; por defecto entidad, tamaño de localidad y factor |
| `min_occupied_match_rate` | porcentaje mínimo de ocupados con cruce COE2 |
| `strict` | lanzar excepción si existen errores críticos |

Retorna un diccionario con métricas, `errores`, `advertencias` y `OK`.

## Controles críticos

- archivo no vacío y esquema requerido;
- ausencia de nombres históricos residuales o encabezados duplicados;
- coincidencia entre filas de metadatos y filas consultadas;
- llave personal completa y única;
- consistencia de año, trimestre y periodo;
- etiquetas válidas de `cruce_coe2`;
- coherencia de `both` y `left_only`;
- factores de expansión presentes y positivos;
- consistencia intrahogar de variables seleccionadas;
- cobertura de COE2 para personas ocupadas.

## Uso estricto

```python
result = validate_merged_period(
    periodo="2025T1",
    path_merged=merged_path,
    llave_persona=PERSON_KEY,
    llave_hogar=HOUSEHOLD_KEY,
    coe2_columns=COE2_COLUMNS,
    strict=True,
)
```

La función considera los factores inválidos como error estructural. Si una fuente oficial contiene faltantes conocidos, deben conservarse en la evidencia y evaluarse explícitamente; no es recomendable ocultarlos mediante filtrado previo.

