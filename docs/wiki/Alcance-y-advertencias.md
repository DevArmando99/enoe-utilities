# Alcance y advertencias

## Propósito

Esta biblioteca hace reutilizable un flujo de cálculo de pobreza laboral construido con microdatos trimestrales de la ENOE. Su prioridad es conservar las reglas metodológicas validadas, registrar cada transformación y facilitar la reproducción de resultados.

## Qué representa cada registro

La ENOE observa hogares y residentes habituales. El Parquet analítico de esta biblioteca utiliza una fila **persona-trimestre**. Sin embargo, la clasificación de pobreza laboral no es puramente individual: el ingreso laboral se suma por hogar, se divide entre sus integrantes válidos y la condición resultante se asigna a cada persona elegible del hogar.

La llave canónica identifica a una persona dentro de un trimestre. No debe interpretarse como un identificador personal permanente ni como una llave longitudinal universal.

## Resultado oficial y resultado reproducido

La Wiki distingue tres niveles:

| Nivel | Contenido |
|---|---|
| Concepto oficial | Definiciones, diseño y metodología publicados por INEGI y CONEVAL |
| Implementación | Reglas codificadas en `enoe-utilities` |
| Evidencia de ejecución | Parquet, validaciones, metadatos JSON y paradatos YAML generados |

Una coincidencia con cifras publicadas es una validación empírica importante, pero no convierte a la biblioteca en una publicación oficial.

## Periodos y comparabilidad

- El proyecto histórico se ha trabajado para `2006T1` a `2026T1`.
- `2020T2` se documenta como periodo excluido por la interrupción de la ENOE presencial y la operación extraordinaria de la ETOE.
- La ENOEN de `2020T3` a `2022T4` mantuvo el diseño conceptual y estadístico con una estrategia de levantamiento adaptada.
- Desde `2021T1` cambiaron las estimaciones de población empleadas en los factores de expansión. Esto requiere una advertencia explícita al comparar tramos de la serie.
- Desde `2023T1` volvió la denominación ENOE con cambios operativos, tecnológicos y de clasificadores documentados por INEGI.

## Inferencia estadística

El indicador principal utiliza `fac_tri` para estimar proporciones expandidas. La versión actual también puede registrar un error estándar, coeficiente de variación e intervalo de confianza **aproximados**, calculados como si se tratara de una proporción simple.

Estas aproximaciones no incorporan de manera completa estratos, unidades primarias de muestreo, conglomeración ni el diseño rotatorio de la ENOE. No deben presentarse como errores estándar oficiales del diseño complejo.

## Límites técnicos actuales

- Las equivalencias históricas se restringen a las declaradas por la biblioteca.
- La lectura de la LPEI espera el formato CSV del BIE utilizado por el proyecto.
- Las columnas y categorías deben verificarse contra el diccionario correspondiente a cada periodo.
- Los archivos de Google Drive son una copia de conveniencia, no la fuente institucional primaria.
- `excluded_periods` documenta exclusiones en metadatos y paradatos; no borra datos ni modifica por sí mismo una lista explícita de fuentes.
- Los auxiliares cuyo nombre empieza con `_` no forman parte de la API pública estable.

## Uso responsable

Para resultados publicables conserva la fuente original, fecha de descarga, versión de los documentos metodológicos, parámetros ejecutados, versión de la biblioteca, commit de Git y todos los metadatos y paradatos asociados.

