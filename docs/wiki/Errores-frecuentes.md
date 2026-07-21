# Errores frecuentes

## `NameError: COLS_COE2 is not defined`

La celda usa un nombre de configuración que no existe en el kernel. Verifica si el notebook define `COE2_COLUMNS`, `SDEM_COLUMNS`, `PERSON_KEY` y `HOUSEHOLD_KEY`, y utiliza esos mismos nombres en todas las llamadas.

Reinicia el kernel y ejecuta las celdas desde el inicio después de corregir nombres.

## `RuntimeError: La validación del merge falló`

La excepción resume el resultado. Revisa la tabla anterior o el CSV `merged_period_validation.csv` para identificar la causa: llaves, duplicados, periodo, cruce, factor de expansión o columnas.

No elimines el `raise` sin clasificar antes cada hallazgo. Una condición oficialmente conocida puede documentarse como excepción operativa, pero el conteo debe permanecer en metadatos y auditoría.

## `The 'person-quarter history' stage failed`

El wrapper del notebook oculta la excepción interna para presentar un mensaje uniforme. Consulta la última entrada de `LIBRARY_LOG`:

```python
LIBRARY_LOG[-1]
```

Las causas habituales son:

- archivo histórico existente con `overwrite=False`;
- Parquet abierto por otro kernel o aplicación;
- diferencias de esquema con `strict_schema=True`;
- periodo o contenido inconsistente;
- fuente faltante con configuración estricta.

## Archivo existente

Decide si el producto debe reutilizarse o regenerarse. Para derivados usa la opción de sobrescritura diseñada por el notebook. Nunca borres las fuentes originales para resolver este error.

## Archivo Parquet bloqueado

Cierra vistas previas, DuckDB, otros notebooks y procesos que mantengan el archivo abierto. Reinicia el kernel antes de ejecutar la versión en otro idioma.

## LPEI ausente

Comprueba que el CSV incluya los tres meses del trimestre y que `build_lpei_quarterly_long()` produzca filas rural y urbana para el periodo.

## Columnas históricas

Ejecuta `validate_required_columns()` y revisa `Equivalencias_aplicadas`. La biblioteca reconoce `ent/cve_ent`, `t_loc/t_loc_tri` y `fac/fac_tri`; otras diferencias requieren una revisión metodológica antes de ampliar equivalencias.

## `OK=False` sin excepción

Varias funciones permiten inspección no estricta. Revisa siempre `errores`, `advertencias`, `audit`, periodos faltantes y `completo`; que una función retorne un objeto no implica que el producto sea válido.

