# Diseño conceptual y estadístico

## Unidades estadísticas

Es importante separar tres conceptos:

| Concepto | Aplicación en la ENOE y el proyecto |
|---|---|
| Unidad de muestreo | Vivienda particular seleccionada por el diseño |
| Unidades de observación | Hogares y residentes habituales de las viviendas seleccionadas |
| Unidad analítica del proyecto | Persona dentro de un trimestre, con atributos del hogar agregados |

El cálculo de pobreza laboral combina los dos niveles de observación: suma ingresos y cuenta integrantes por hogar, y después clasifica a cada persona elegible.

## Características del diseño

La documentación de INEGI describe un diseño:

- probabilístico;
- estratificado;
- por conglomerados;
- en varias etapas;
- con panel rotatorio de viviendas;
- con factores de expansión para obtener estimaciones poblacionales.

La selección y los factores dependen del marco de muestreo y de las estimaciones de población vigentes. La [nota sobre cambios en la estimación de población](https://www.inegi.org.mx/contenidos/programas/enoe/15ymas/doc/nota_sobre_cambios_estimacion_poblacion_enoe_n.pdf) es indispensable para interpretar la serie alrededor de 2021.

## Cobertura geográfica

Los productos de la ENOE pueden ofrecer resultados nacionales, por entidad federativa, por tamaños de localidad y para ciudades autorrepresentadas, según el producto y periodo. La precisión no es idéntica para todos los dominios.

En esta biblioteca, el ámbito usado para la LPEI se deriva de `t_loc_tri`:

| Valor | Ámbito del proyecto |
|---:|---|
| `1`, `2`, `3` | urbano |
| `4` | rural |
| otro o faltante | ámbito no asignado |

## Factores de expansión

`fac_tri` representa el factor trimestral. Cada persona elegible aporta su factor al numerador o denominador del indicador. Un factor nulo o no positivo se considera inválido.

Las equivalencias históricas que reconoce la biblioteca incluyen `fac` como nombre alternativo de `fac_tri`.

## Precisión

Los factores de expansión permiten obtener estimaciones, pero no bastan para reproducir varianzas de diseño. Para estimar errores estándar oficiales se requieren variables y procedimientos de diseño muestral. Las métricas aproximadas generadas por la biblioteca deben etiquetarse como tales.

## Referencias

- [Diseño muestral ENOE-N](https://www.inegi.org.mx/contenidos/programas/enoe/15ymas/doc/enoe_n_diseno_muestral.pdf)
- [Diseño conceptual ENOE-N](https://www.inegi.org.mx/contenidos/programas/enoe/15ymas/doc/enoe_n_diseno_conceptual.pdf)
- [Estrategia operativa ENOE-N](https://www.inegi.org.mx/contenidos/programas/enoe/15ymas/doc/enoe_n_estrategia_operativa.pdf)

