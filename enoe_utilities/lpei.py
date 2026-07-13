"""Lectura y transformación de la Línea de Pobreza Extrema por Ingresos."""

import re
from pandas import DataFrame


def get_pl_series(filepath: str) -> DataFrame:
    """
    Lee un CSV de Línea de Pobreza Extrema por Ingresos descargado del BIE de INEGI
    y retorna una serie temporal lista para análisis.

    El archivo debe seguir el formato estándar del BIE (UTF-16-LE, sin encabezado
    tabular explícito, con notas al final). La función ignora automáticamente las
    líneas de encabezado y de notas/fuente.

    Parámetros
    ----------
    filepath : str
        Ruta al archivo .csv descargado del BIE de INEGI.
        Ejemplo: "ruta/a/Indicadores20260614181629.csv"

    Retorna
    -------
    pd.DataFrame con columnas:
        - Año       : int    (ej. 1992)
        - Mes       : int    (1–12)
        - Trimestre : int    (1–4, derivado del mes)
        - Rural     : float  (LP extrema rural, pesos por persona al mes)
        - Urbano    : float  (LP extrema urbana, pesos por persona al mes)

    Uso
    ---
        serie_lp = get_pl_series("ruta/a/archivo.csv")
        print(serie_lp.head())
    """
    # Sólo se procesan líneas que empiezan con el patrón de fecha AAAA/MM
    DATE_PATTERN = re.compile(r"^\d{4}/\d{2},")

    # Los CSVs del BIE de INEGI usan codificación UTF-16-LE (con BOM implícito)
    with open(filepath, encoding="utf-16-le") as f:
        lines = f.readlines()

    records = []
    for line in lines:
        line = line.strip()
        if not DATE_PATTERN.match(line):
            continue  # Omite encabezados, líneas vacías y notas al pie

        # Estructura de cada línea de datos:
        # AAAA/MM , área geográfica , rural , urbano , (vacío)
        parts = line.split(",")
        fecha_str  = parts[0]        # "1992/01"
        rural_str  = parts[2].strip()
        urbano_str = parts[3].strip()

        año      = int(fecha_str[:4])
        mes      = int(fecha_str[5:7])
        trimestre = (mes - 1) // 3 + 1

        records.append(
            {
                "Año"       : año,
                "Mes"       : mes,
                "Trimestre" : trimestre,
                "Rural"     : float(rural_str),
                "Urbano"    : float(urbano_str),
            }
        )

    df = DataFrame(records, columns=["Año", "Mes", "Trimestre", "Rural", "Urbano"])
    return df


def build_lpei_quarterly_long(serie_lp):
    """
    Convierte la serie mensual de LPEI rural/urbana en una
    tabla trimestral larga lista para merge.

    Retorna columnas:
        anio, trimestre, ambito, lpei
    """

    lpei_trimestre = (
        serie_lp
        .groupby(["Año", "Trimestre"], as_index=False)[["Rural", "Urbano"]]
        .mean()
        .round(2)
    )

    lpei_long = (
        lpei_trimestre
        .melt(
            id_vars=["Año", "Trimestre"],
            value_vars=["Rural", "Urbano"],
            var_name="ambito",
            value_name="lpei"
        )
    )

    lpei_long = lpei_long.rename(
        columns={
            "Año": "anio",
            "Trimestre": "trimestre"
        }
    )

    lpei_long["ambito"] = (
        lpei_long["ambito"]
        .str.lower()
    )

    return lpei_long
