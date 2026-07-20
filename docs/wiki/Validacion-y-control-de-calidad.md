# Validación y control de calidad

La validación es una secuencia de puertas. Un resultado final correcto no compensa silenciosamente un esquema, llave o fuente incorrectos.

## Puertas de control

| Etapa | Controles principales | Evidencia |
|---|---|---|
| Conversión | ZIP reconocido, tablas localizadas, filas y esquema | JSON y YAML de conversión |
| Columnas | requeridas, equivalencias y mapa de lectura | DataFrame/CSV de validación |
| Merge | llaves, filas, cruces, factores y consistencia del hogar | auditoría y JSON del merge |
| Análisis | filtros, ingreso, imputación, exclusión, ámbito y LPEI | métricas analíticas |
| Indicador | población elegible y pobre expandida | serie, auditoría y metadatos |
| Histórico | esquema, contenido, cobertura y orden | manifiesto CSV y JSON/YAML |

## Validación de merge

Los errores críticos incluyen:

- archivo vacío o columnas ausentes;
- columnas históricas sin normalizar o nombres duplicados;
- llave personal nula o duplicada;
- periodo inconsistente;
- etiquetas inválidas de `cruce_coe2`;
- conteos incompatibles entre `both`, `left_only` y total;
- registros `left_only` con datos de COE2;
- factores de expansión nulos o no positivos;
- inconsistencias intrahogar;
- cobertura de ocupados inferior al umbral configurado.

`strict=False` devuelve toda la evidencia con `OK=False`. `strict=True` lanza una excepción después de construir el diagnóstico.

## Calidad de ingreso

Se deben revisar, como mínimo:

- ocupados y ocupados remunerados;
- trabajadores sin pago;
- ingresos directos `p6b2`;
- candidatos e ingresos imputados con `p6c`;
- ingresos no recuperables;
- hogares incompletos;
- personas y población expandida excluidas.

Una imputación alta no es automáticamente un error, pero sí requiere explicación por periodo y contraste histórico.

## Calidad del indicador

Debe cumplirse:

```text
población pobre expandida <= población elegible expandida
0 <= PL <= 100
```

También se revisan duplicados de periodo, cobertura del rango solicitado y diferencias contra valores oficiales de referencia.

## Tratamiento de advertencias

Una advertencia documenta una condición que permite continuar. Un error indica incumplimiento de una regla del producto. La política del notebook no debe borrar la clasificación original de la biblioteca: puede justificar una excepción conocida, pero debe conservar el conteo, periodo y fuente.

## Comparación temporal

Las tasas de cruce, imputación, exclusión, factores inválidos, población expandida y PL deben compararse con periodos vecinos. Los saltos coincidentes con ETOE, ENOEN, cambios de cuestionario o nuevas estimaciones poblacionales requieren interpretación metodológica antes de atribuirlos al fenómeno económico.

