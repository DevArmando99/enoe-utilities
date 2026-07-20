# LPEI y pobreza laboral

## Línea de Pobreza Extrema por Ingresos

La **LPEI** representa el valor monetario mensual por persona de la canasta alimentaria. Se publica de forma diferenciada para ámbito rural y urbano.

No debe confundirse con la Línea de Pobreza por Ingresos, que incorpora también la canasta no alimentaria.

- [Líneas de pobreza por ingresos](https://www.coneval.org.mx/Medicion/MP/Paginas/Lineas-de-Pobreza-por-Ingresos.aspx)
- [Metodología de construcción de las líneas](https://www.coneval.org.mx/InformesPublicaciones/InformesPublicaciones/Documents/Lineas_pobreza.pdf)
- [Nota de actualización](https://www.coneval.org.mx/Medicion/Documents/Lineas_de_Pobreza_por_Ingresos/Nota_actualizacion_Lineas_Pobreza_por_Ingresos.pdf)

## Transformación mensual a trimestral

`get_pl_series()` lee el CSV mensual del BIE utilizado por el proyecto. Espera codificación UTF-16-LE y registros con fecha `AAAA/MM`. Produce:

```text
Año, Mes, Trimestre, Rural, Urbano
```

`build_lpei_quarterly_long()` calcula el promedio simple de los tres meses, redondeado a dos decimales:

$$
LPEI_{a,q,s}=\frac{1}{3}\sum_{m\in q}LPEI_{a,m,s}
$$

donde $s$ es rural o urbano. La salida larga contiene `anio`, `trimestre`, `ambito` y `lpei`.

## Definición de pobreza laboral

Una persona se clasifica en pobreza laboral cuando vive en un hogar cuyo ingreso laboral mensual per cápita es menor que la LPEI correspondiente al periodo y ámbito:

$$
PobrezaLaboral_i = \mathbb{1}\left(\frac{IngresoLaboralHogar_h}{Integrantes_h}<LPEI_{t,s}\right)
$$

La clasificación se asigna a cada persona elegible del hogar, no únicamente a quienes están ocupados.

## Índice trimestral

La implementación calcula:

$$
PL_t = 100\times
\frac{\sum_{i\in E_t} fac\_tri_i\,\mathbb{1}(PobrezaLaboral_i)}
{\sum_{i\in E_t} fac\_tri_i}
$$

$E_t$ contiene personas con ingreso del hogar completo, ingreso per cápita disponible, LPEI asignada y factor positivo.

## Imputación de ingreso

El ingreso exacto `p6b2` tiene prioridad. Cuando no está disponible y `p6c` y `salario` son válidos, se recupera un ingreso mediante el punto representativo del rango. Un ocupado remunerado sin ingreso recuperable provoca que el hogar se marque como incompleto.

La [nota de imputación de ingresos laborales del CONEVAL](https://www.coneval.org.mx/Medicion/Documents/ITLP_IS/Nota_imputacion_ingresos_laborales_CONEVAL.pdf) debe consultarse junto con la implementación para distinguir la regla institucional de su traducción computacional.

## Interpretación

El indicador mide insuficiencia del ingreso laboral frente a la canasta alimentaria. No equivale a la medición multidimensional de pobreza y no incorpora directamente todas las fuentes de ingreso, carencias sociales o derechos considerados por esa metodología.

