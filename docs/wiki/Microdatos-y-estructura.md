# Microdatos y estructura de archivos

## Tablas principales

Los microdatos trimestrales se distribuyen en cinco familias de tablas:

| Tabla | Contenido general |
|---|---|
| `VIV` | características y control de la vivienda |
| `HOG` | características y control del hogar |
| `SDEM` | información sociodemográfica, residencia, clasificación laboral y factores |
| `COE1` | primera parte del Cuestionario de Ocupación y Empleo |
| `COE2` | segunda parte del cuestionario, incluidas variables laborales e ingreso |

El flujo de pobreza laboral implementado por esta biblioteca utiliza principalmente `SDEM` y `COE2`. Las demás tablas continúan siendo relevantes para análisis distintos y para comprender el diseño completo.

## Organización del proyecto

```text
data/
├── raw/
│   ├── zip/
│   └── lpei/
└── parquet/
    ├── converted/<periodo>/
    ├── merged/<periodo>/
    ├── analytical/<periodo>/
    └── historical/

outputs/
├── indicators/
├── metadata/
├── paradata/<periodo>/
└── validation/
```

## Capas de datos

| Capa | Grano | Descripción |
|---|---|---|
| Convertida | registro de la tabla fuente | SDEM y COE2 en Parquet |
| Unida | persona-periodo de SDEM | LEFT JOIN con variables de COE2 |
| Analítica | residente válido persona-trimestre | ingresos, hogar, LPEI, elegibilidad y pobreza |
| Histórica analítica | persona-trimestre | concatenación ordenada de periodos analíticos |
| Indicador | trimestre | columnas `Año`, `Trimestre` y `PL` |

## Conversión

`get_time_series()` inspecciona los ZIP de entrada, localiza las tablas SDEM y COE2 y convierte sus CSV a Parquet por bloques. Esto evita cargar en memoria el trimestre completo.

La salida esperada es un diccionario:

```python
{
    "2025T1": [ruta_coe2, ruta_sdem],
    "2025T2": [ruta_coe2, ruta_sdem],
}
```

## Nombres históricos

La biblioteca trabaja con nombres canónicos y resuelve estas equivalencias físicas:

| Nombre canónico | Alternativas admitidas |
|---|---|
| `cve_ent` | `cve_ent`, `ent` |
| `t_loc_tri` | `t_loc_tri`, `t_loc` |
| `fac_tri` | `fac_tri`, `fac` |

La normalización no autoriza a mezclar diccionarios de distintos periodos sin revisión. Antes del merge se debe ejecutar `validate_required_columns()`.

## Cuestionarios básico y ampliado

COE2 puede contener preguntas adicionales cuando se utiliza el cuestionario ampliado. El proyecto selecciona únicamente las columnas necesarias para su método; por eso puede procesar ambos diseños siempre que las variables críticas y sus equivalencias estén presentes.

## Documentación

- [Conociendo la base de datos](https://www.inegi.org.mx/contenidos/programas/enoe/15ymas/doc/con_basedatos_proy2010.pdf)
- [Estructura de la base de datos 2022](https://www.inegi.org.mx/contenidos/programas/enoe/15ymas/doc/fd_c_bas_amp_15ymas.pdf)
- [Estructura de la base de datos 2025](https://www.inegi.org.mx/contenidos/programas/enoe/15ymas/doc/enoe_325_fd_c_bas_amp.pdf)

