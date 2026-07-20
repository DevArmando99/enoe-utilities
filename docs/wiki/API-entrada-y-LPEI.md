# API: entrada y LPEI

## `get_time_series`

```python
get_time_series(
    path_in,
    path_out,
    chunk_size=100000,
    compression="snappy",
    overwrite=False,
    *,
    generate_metadata=False,
    path_paradata="../Paradata",
)
```

Convierte las tablas SDEM y COE2 contenidas en ZIP/CSV a Parquet por bloques y devuelve las rutas organizadas por trimestre.

| Argumento | Uso |
|---|---|
| `path_in` | carpeta con archivos ZIP ENOE |
| `path_out` | raíz de Parquet convertidos |
| `chunk_size` | filas leídas por bloque |
| `compression` | compresión PyArrow, por defecto `snappy` |
| `overwrite` | reemplazar Parquet existentes |
| `generate_metadata` | generar JSON y acumular paradatos |
| `path_paradata` | raíz de los YAML |

Retorna un diccionario ordenado por periodo, normalmente con `[ruta_coe2, ruta_sdem]` como valor.

## `get_pl_series`

```python
get_pl_series(filepath: str) -> pandas.DataFrame
```

Lee el CSV de LPEI descargado del BIE en el formato utilizado por el proyecto.

| Argumento | Uso |
|---|---|
| `filepath` | archivo CSV UTF-16-LE con filas fechadas `AAAA/MM` |

Retorna `Año`, `Mes`, `Trimestre`, `Rural` y `Urbano`. Omite encabezados, notas y líneas que no comienzan con una fecha válida.

## `build_lpei_quarterly_long`

```python
build_lpei_quarterly_long(serie_lp)
```

| Argumento | Uso |
|---|---|
| `serie_lp` | DataFrame mensual devuelto por `get_pl_series()` |

Agrupa por año y trimestre, promedia `Rural` y `Urbano`, redondea a dos decimales y transforma a formato largo.

Retorna `anio`, `trimestre`, `ambito` y `lpei`.

## Ejemplo

```python
from enoe_utilities import get_pl_series, build_lpei_quarterly_long

lpei_monthly = get_pl_series("data/raw/lpei/indicadores_linea_pobreza.csv")
lpei_quarterly = build_lpei_quarterly_long(lpei_monthly)
```

## Errores comunes

- El CSV no está en UTF-16-LE.
- Las columnas rural y urbana no ocupan las posiciones esperadas.
- El archivo no contiene filas `AAAA/MM`.
- Falta alguno de los tres meses del trimestre; la función promedia las filas disponibles y no exige por sí sola completitud mensual.

