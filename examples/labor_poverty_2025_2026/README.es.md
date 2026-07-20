# Ejemplo del pipeline de pobreza laboral (2025-2026)

[Read in English](README.md)

Este directorio contiene un ejemplo completo y bilingüe del flujo de pobreza laboral implementado con la API pública de `enoe-utilities`. Los notebooks procesan microdatos trimestrales de la ENOE desde los archivos ZIP/CSV originales hasta la conversión, validación de esquemas, unión SDEM-COE2, construcción del conjunto analítico, indicadores de pobreza laboral, concatenación histórica, metadatos y paradatos.

## Notebooks

- `labor_poverty_pipeline_es.ipynb`: versión en español.
- `labor_poverty_pipeline_en.ipynb`: versión en inglés.

Ambos notebooks implementan el mismo pipeline y deben ejecutarse de manera independiente. Reinicia el kernel antes de cambiar de idioma para evitar que variables o archivos Parquet abiertos durante una ejecución afecten a la otra.

## Carpeta completa y ejecutable

Los microdatos originales, archivos Parquet convertidos y resultados generados son demasiado pesados para este repositorio. La carpeta completa y ejecutable está disponible en [Google Drive](https://drive.google.com/drive/folders/1WyXtJdbN__LyOo1la5rE7mNs5r3bFE4m?usp=sharing).

Puedes ejecutar la carpeta descargada como un ejemplo independiente o copiar su contenido de `data/` a este directorio, respetando la estructura mostrada abajo. Los archivos generados permanecen de forma local y están excluidos mediante el `.gitignore` de este ejemplo.

## Estructura de directorios

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

## Requisitos

- Python 3.10 o una versión posterior.
- JupyterLab o Jupyter Notebook.
- Una instalación editable de este repositorio y sus dependencias.

Desde la raíz del repositorio:

```bash
python -m pip install -e .
python -m pip install jupyterlab
```

## Ejecutar el ejemplo

1. Descarga la carpeta completa de Google Drive, o coloca los archivos ZIP de la ENOE y el CSV de la LPEI en los directorios correspondientes dentro de `data/raw/`.
2. Abre una terminal en `examples/labor_poverty_2025_2026/`.
3. Inicia Jupyter con `jupyter lab` o `jupyter notebook`.
4. Abre cualquiera de las versiones y ejecuta las celdas en orden desde el inicio.

Los notebooks deben ejecutarse usando `examples/labor_poverty_2025_2026/` como directorio de trabajo. No los ejecutes desde `data/`, `outputs/` ni desde el directorio de checkpoints de Jupyter.

El ejemplo cubre `2025T1`, `2025T2`, `2025T3`, `2025T4` y `2026T1`. Los archivos derivados existentes pueden sobrescribirse según la configuración del notebook. Antes de volver a ejecutar el pipeline, cierra cualquier otro kernel o aplicación que esté utilizando un archivo Parquet generado.

## Datos y archivos generados

El repositorio solo conserva los notebooks, la documentación, los marcadores de directorios y las reglas de exclusión. De forma intencional, no conserva:

- microdatos ZIP/CSV originales de la ENOE;
- archivos Parquet convertidos, unidos, analíticos o históricos;
- metadatos JSON o paradatos YAML generados;
- tablas de validación y resultados de indicadores.

Esto mantiene ligero el historial de Git y evita redistribuir archivos grandes de la encuesta. Consulta los términos y la documentación de las instituciones fuente antes de redistribuir los datos descargados.

## Referencias metodológicas

Los microdatos procesados por este pipeline corresponden a la **Encuesta Nacional de Ocupación y Empleo (ENOE)**. Su documentación técnica y metodología se encuentran en la [página oficial de la ENOE en INEGI](https://www.inegi.org.mx/programas/enoe/15ymas/).

## Nota de reproducibilidad

La carpeta de Google Drive es una copia de conveniencia del ejemplo trabajado. Para investigación o producción, descarga los datos oficiales directamente de las instituciones responsables, registra la fecha y la versión de la fuente, y conserva los metadatos y paradatos generados junto con los resultados.
