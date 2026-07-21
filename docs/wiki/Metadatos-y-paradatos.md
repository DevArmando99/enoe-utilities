# Metadatos y paradatos

## Diferencia

| Artefacto | Pregunta que responde | Formato |
|---|---|---|
| Metadatos | ¿Qué contiene el archivo y cuál es su calidad? | JSON |
| Paradatos | ¿Cómo, cuándo y con qué parámetros se produjo? | YAML |

## Activación

Las funciones compatibles reciben:

```python
generate_metadata=True
path_paradata=PARADATA_DIR
```

Los JSON se guardan junto al Parquet o resultado que describen. Los YAML se acumulan bajo la raíz de paradatos, normalmente por periodo.

## Estructura de metadatos JSON

La estructura general incluye:

```text
general_information
source_data
construction_method
dataset_size
schema
critical_variables
quality_indicator_dictionary
quality_indicators
methodological_observations
```

### Secciones

| Sección | Contenido |
|---|---|
| `general_information` | nombre, periodo, nivel, tipo, fecha, función y versión |
| `source_data` | archivos o conjuntos de entrada |
| `construction_method` | transformaciones y parámetros |
| `dataset_size` | filas, columnas, grupos y tamaño |
| `schema` | tipo, descripción, fuente, unidad, rol y uso por columna |
| `critical_variables` | variables que condicionan el cálculo |
| `quality_indicator_dictionary` | definición e interpretación de métricas |
| `quality_indicators` | valores observados |
| `methodological_observations` | advertencias necesarias para interpretar el archivo |

## Indicadores de calidad

Según la etapa pueden incluir calidad de:

- conversión y esquema;
- llaves y merge;
- residencia;
- ingreso directo `p6b2`;
- imputación `p6c`;
- hogares y personas excluidas;
- factores de expansión;
- asignación de LPEI;
- indicador final;
- cobertura y consistencia temporal;
- variables sociodemográficas.

## Tres métricas de imputación

Estas métricas no son intercambiables:

| Métrica | Fórmula simplificada | Lectura |
|---|---|---|
| Cobertura de imputación | imputados / candidatos a imputación | capacidad de recuperación |
| Proporción de registros imputados | imputados elegibles / personas elegibles | presencia de imputación en filas analíticas |
| Proporción poblacional imputada | suma de `fac_tri` de imputados elegibles / población elegible expandida | peso poblacional de la imputación |

## Paradatos YAML

Un YAML por periodo acumula pasos sucesivos, por ejemplo:

```yaml
periodo: 2025T1
steps:
  - step_number: 1
    step_name: get_time_series
    function_name: get_time_series
    status: ok
    started_at_utc: "..."
    finished_at_utc: "..."
    input: {}
    parameters: {}
    output: {}
    execution_summary: {}
    warnings: []
    errors: []
```

La serie y el histórico pueden utilizar YAML específicos que documentan cobertura de periodos, exclusiones y fuentes concatenadas.

## Callbacks

`event_callback`, `metadata_callback` y `paradata_callback` permiten enviar la evidencia a otro sistema sin cambiar el cálculo. Con `strict_hooks=False`, una falla del callback no debe invalidar el producto principal; con `True`, sí detiene la etapa.

## Reglas de conservación

- No separes el JSON del archivo que describe.
- Conserva el YAML completo del periodo, no solo el último paso.
- No reemplaces advertencias por valores vacíos.
- Versiona los esquemas documentales cuando cambien nombres o fórmulas.
- Registra la versión de la función que produjo cada artefacto.

