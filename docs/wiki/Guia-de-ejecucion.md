# Guía de ejecución

## Requisitos

- Python 3.10 o posterior.
- JupyterLab o Jupyter Notebook.
- Espacio suficiente para ZIP, CSV y Parquet.
- Dependencias: `pandas`, `pyarrow`, `duckdb` y `PyYAML`.

Desde la raíz del repositorio:

```bash
python -m pip install -e .
python -m pip install jupyterlab
```

## Ejemplo 2025-2026

El repositorio incluye:

- `labor_poverty_pipeline_es.ipynb`
- `labor_poverty_pipeline_en.ipynb`

Los datos y resultados pesados están en la [carpeta ejecutable de Google Drive](https://drive.google.com/drive/folders/1WyXtJdbN__LyOo1la5rE7mNs5r3bFE4m?usp=sharing).

## Directorio correcto

Abre la terminal en:

```text
examples/labor_poverty_2025_2026/
```

Inicia Jupyter desde esa carpeta:

```bash
jupyter lab
```

Los notebooks deben tener `examples/labor_poverty_2025_2026/` como directorio de trabajo. No deben ejecutarse desde `data/`, `outputs/` ni `.ipynb_checkpoints/`.

## Orden del pipeline

1. Importaciones, configuración y mensajes.
2. Definición de rutas, periodos, llaves y columnas.
3. Conversión ZIP/CSV a Parquet.
4. Lectura de la LPEI mensual y construcción trimestral.
5. Validación de columnas requeridas.
6. Merge SDEM-COE2.
7. Validación de cada merge.
8. Construcción de Parquet analíticos.
9. Cálculo del indicador trimestral.
10. Construcción de la serie temporal.
11. Concatenación histórica persona-trimestre.
12. Revisión de metadatos, paradatos y auditorías.

Los títulos y números de bloque pueden variar entre versiones. El orden lógico debe conservarse.

## Ejecutar ambos idiomas

Los notebooks en español e inglés implementan el mismo flujo. Deben ejecutarse de manera independiente:

1. Ejecuta uno desde el inicio.
2. Cierra o reinicia su kernel.
3. Verifica `overwrite` para archivos derivados.
4. Ejecuta el segundo.

Esto evita reutilizar variables del kernel o mantener archivos Parquet abiertos.

## Configuraciones importantes

| Opción | Recomendación |
|---|---|
| `OVERWRITE` | `False` para proteger conversiones costosas |
| `OVERWRITE_DERIVED` | `True` solo cuando se desea regenerar merge, análisis o histórico |
| `GENERATE_METADATA` | `True` para ejecuciones auditables |
| `PARQUET_COMPRESSION` | `zstd` para derivados |
| `EXPECTED_PERIODS` | declarar de forma explícita |
| `DOCUMENTED_EXCLUDED_PERIODS` | incluir `2020T2` cuando el rango lo atraviesa |

## Antes de aceptar resultados

- Todas las columnas requeridas deben estar disponibles o resueltas por equivalencia.
- La llave de persona debe ser única.
- Las filas del merge deben ser iguales a las filas de SDEM.
- Las tasas de cruce y los factores inválidos deben revisarse.
- La LPEI debe existir para ambos ámbitos del periodo.
- Los hogares incompletos y la población excluida deben quedar cuantificados.
- El manifiesto histórico debe coincidir con las fuentes procesadas.

