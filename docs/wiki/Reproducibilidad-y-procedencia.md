# Reproducibilidad y procedencia

## Objetivo

Una ejecución reproducible permite saber qué archivos, código, parámetros y decisiones produjeron cada resultado.

## Registro mínimo

| Elemento | Ejemplo |
|---|---|
| Fuente | URL oficial del ZIP o CSV |
| Fecha de descarga | fecha y hora UTC |
| Periodo | `2025T1` |
| Archivo | nombre original sin modificar |
| Integridad | SHA-256 recomendado |
| Código | versión del paquete y commit Git |
| Ambiente | Python y versiones de dependencias |
| Parámetros | columnas, llaves, compresión, sobrescritura y modo estricto |
| Resultado | rutas de Parquet, CSV, JSON y YAML |
| Exclusiones | `2020T2` u otras con justificación |

## Fuentes de datos

Para investigación o producción se deben descargar los microdatos y líneas monetarias desde INEGI o CONEVAL. La [carpeta de Google Drive](https://drive.google.com/drive/folders/1WyXtJdbN__LyOo1la5rE7mNs5r3bFE4m?usp=sharing) facilita ejecutar el ejemplo, pero no sustituye la procedencia institucional.

## Niveles de trazabilidad

1. **Archivo:** JSON junto a cada Parquet.
2. **Periodo:** YAML acumulativo con los pasos del trimestre.
3. **Serie:** auditoría de periodos calculados, faltantes y fallidos.
4. **Histórico:** manifiesto de los Parquet concatenados.
5. **Código:** versión del paquete y commit del repositorio.

## Convención de periodos

Usa `AAAATn` en rutas, nombres y columnas documentales. Los resultados tabulares pueden separar `Año` y `Trimestre`, pero deben conservar una correspondencia única con `periodo`.

## Exclusiones

`excluded_periods` sirve para documentar ausencias conocidas. La lista no debe utilizarse para ocultar fallas inesperadas. Registra para cada exclusión:

- periodo;
- motivo;
- fuente metodológica;
- efecto sobre la cobertura;
- decisión sobre comparabilidad.

## Lista antes de publicar

- Ejecutar desde un kernel limpio.
- Validar esquemas y llaves antes del merge.
- Conservar auditorías aunque `OK=True`.
- Comparar el indicador con una referencia oficial.
- Revisar JSON y YAML de una muestra y del histórico.
- Confirmar que los periodos esperados, procesados y excluidos cierran.
- Registrar cualquier adaptación hecha en el notebook.

