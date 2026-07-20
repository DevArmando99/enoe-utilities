# Variables del proyecto

Esta página documenta el conjunto mínimo utilizado por el flujo actual. No sustituye el diccionario completo de microdatos de INEGI.

## Columnas de COE2

```python
COE2_COLUMNS = [
    *PERSON_KEY,
    "p6_9", "p6b1", "p6b2", "p6c",
]
```

| Variable | Función en el proyecto |
|---|---|
| `p6_9` | apoyo para identificar trabajadores sin pago |
| `p6b1` | variable conservada del bloque de ingreso |
| `p6b2` | ingreso laboral mensual exacto; fuente prioritaria |
| `p6c` | rango de salarios mínimos para recuperación del ingreso |

## Columnas de SDEM

```python
SDEM_COLUMNS = [
    *PERSON_KEY,
    "r_def", "c_res", "par_c", "sex", "eda", "n_hij", "e_con",
    "t_loc_tri", "clase2", "pos_ocu", "salario", "fac_tri",
]
```

| Variable | Función en el proyecto |
|---|---|
| `r_def` | resultado definitivo de entrevista; se conserva `0` |
| `c_res` | condición de residencia; se conservan `1` y `3` |
| `par_c` | parentesco dentro del hogar |
| `sex` | sexo registrado |
| `eda` | edad registrada |
| `n_hij` | número de hijos declarado |
| `e_con` | estado conyugal |
| `t_loc_tri` | tamaño de localidad para asignar ámbito rural/urbano |
| `clase2` | clasificación laboral; `1` identifica ocupación en la implementación |
| `pos_ocu` | posición en la ocupación; `4` identifica trabajo sin pago |
| `salario` | salario mínimo mensual del periodo usado con `p6c` |
| `fac_tri` | factor trimestral de expansión |

## Variables derivadas principales

| Variable | Descripción |
|---|---|
| `es_ocupado` | `clase2 == 1` |
| `es_sin_pago` | persona ocupada con `pos_ocu == 4` o `p6_9 == 9` |
| `p6b2_valido` | `p6b2` entre 1 y 999998 |
| `multiplicador_p6c` | punto representativo del rango `p6c` |
| `ingreso_imputado_p6c` | multiplicador de `p6c` por `salario` |
| `ingreso_laboral_ind` | ingreso individual recuperado o cero según condición |
| `fuente_ingreso` | directo, imputado, sin pago, no ocupado o no recuperable |
| `hogar_ingreso_incompleto` | al menos un ocupado remunerado sin ingreso recuperable |
| `ingreso_hogar` | suma de ingresos cuando el hogar es completo |
| `ingreso_pc` | ingreso del hogar entre integrantes válidos |
| `ambito` | rural o urbano según `t_loc_tri` |
| `lpei` | línea trimestral asignada por ámbito |
| `entra_calculo_pl` | bandera de elegibilidad para el indicador |
| `pobreza_laboral_persona` | `ingreso_pc < lpei` para personas elegibles |

## Multiplicadores de `p6c`

| Código `p6c` | Multiplicador |
|---:|---:|
| 1 | 0.5 |
| 2 | 1.0 |
| 3 | 1.5 |
| 4 | 2.5 |
| 5 | 4.0 |
| 6 | 7.5 |
| 7 | 10.0 |

## Control histórico

Los significados, universos y categorías deben verificarse en el cuestionario y descriptor correspondientes al periodo. La [reconstrucción de variables](https://www.inegi.org.mx/contenidos/programas/enoe/15ymas/doc/recons_var_15ymas.pdf) y la [nota sobre salarios equivalentes](https://www.inegi.org.mx/contenidos/programas/enoe/15ymas/doc/enoe_salarios_equiv_nota_tecnica.pdf) son referencias centrales.

