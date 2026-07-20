# Metodología de cálculo

Esta página describe lo que ejecuta `enoe-utilities` 0.1.0. Las decisiones institucionales se documentan por separado en [LPEI y pobreza laboral](https://github.com/DevArmando99/enoe-utilities/wiki/LPEI-y-pobreza-laboral).

## 1. Conversión

Los ZIP/CSV de cada trimestre se convierten por bloques a `COE2.parquet` y `SDEM.parquet`. El periodo se normaliza como `AAAATn`.

## 2. Validación de columnas

Se verifica la existencia de archivos, filas y columnas requeridas. Los nombres históricos `ent`, `t_loc` y `fac` pueden resolverse como `cve_ent`, `t_loc_tri` y `fac_tri`.

## 3. Unión

SDEM se usa como tabla madre en un `LEFT JOIN` por la llave canónica de persona. La salida debe conservar el número de filas de SDEM y registra `cruce_coe2`.

## 4. Residentes válidos

Solo pasan al dataset analítico los registros que cumplen:

```text
r_def = 0
c_res ∈ {1, 3}
```

## 5. Condición laboral e ingreso individual

```text
es_ocupado  := clase2 = 1
es_sin_pago := es_ocupado y (pos_ocu = 4 o p6_9 = 9)
```

La prioridad de ingreso es:

1. No ocupado: ingreso igual a `0`.
2. Trabajador sin pago: ingreso igual a `0`.
3. `p6b2` entre `1` y `999998`: se usa el ingreso declarado.
4. `p6c` entre `1` y `7`, con `salario > 0`: se usa `multiplicador_p6c × salario`.
5. Otro ocupado remunerado: ingreso no recuperable.

## 6. Agregación del hogar

Para cada llave de hogar se calcula:

```text
integrantes_hogar          = número de residentes válidos
ingreso_hogar_observado    = suma de ingresos individuales disponibles
hogar_ingreso_incompleto   = existe ingreso no recuperable
```

Si el hogar está incompleto, `ingreso_hogar` e `ingreso_pc` se asignan como nulos. Sus integrantes permanecen en el Parquet, pero no entran al indicador.

## 7. Ingreso per cápita y ámbito

Para hogares completos:

$$
ingreso\_pc_h=\frac{ingreso\_hogar_h}{integrantes\_hogar_h}
$$

`t_loc_tri = 4` se clasifica como rural; `1`, `2` o `3` como urbano.

## 8. LPEI y elegibilidad

La LPEI se une por año, trimestre y ámbito. Una persona entra al cálculo cuando:

- su hogar no tiene ingreso incompleto;
- `ingreso_pc` está disponible;
- la LPEI está disponible;
- `fac_tri` es positivo.

`pobreza_laboral_persona` es verdadero cuando `ingreso_pc < lpei`.

## 9. Indicador

`compute_pl_period()` suma `fac_tri` de las personas pobres elegibles y divide entre el total expandido elegible. El resultado se multiplica por 100 y se redondea, por defecto, a dos decimales.

## 10. Serie e histórico

`compute_pl_time_series()` repite el cálculo por trimestre, crea una auditoría y guarda CSV y/o Parquet. `build_historical_analysis_parquet()` concatena los Parquet persona-trimestre y puede guardar un manifiesto de fuentes.

## 11. Evidencia de calidad

Cada etapa puede generar:

- metadatos JSON del archivo y sus indicadores de calidad;
- paradatos YAML con pasos, parámetros, tiempos, advertencias y errores;
- tablas de auditoría por periodo;
- manifiestos de la concatenación histórica.

## Regla de revisión

Cualquier modificación de filtros, códigos, multiplicadores, llaves, ámbito, denominador o ponderación cambia la metodología. Debe validarse contra cifras oficiales y registrarse como una nueva versión.

