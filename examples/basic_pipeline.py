"""Minimal ENOE labor poverty pipeline example.

Adapt every path, canonical column list, and key definition to your local files.
This script is illustrative and requires real ENOE and LPEI inputs.
"""

from pathlib import Path

from enoe_utilities import (
    get_time_series,
    get_pl_series,
    build_lpei_quarterly_long,
    validate_required_columns,
    build_merged_time_series,
    validate_merged_period,
    build_analysis_dataset,
    compute_pl_time_series,
    build_historical_analysis_parquet,
)
from enoe_utilities.metadata import (
    _append_required_columns_validation_paradata,
    _append_merged_validation_paradata,
)


PATH_ZIPS = Path("../Archivos_Zip")
PATH_PARQUETS = Path("../Datos_Parquet")
PATH_MERGED = Path("../Merge_de_parquet")
PATH_ANALYSIS = Path("../Analisis_PL_parquet")
PATH_RESULTS = Path("../Resultados_PL")
PATH_HISTORICAL = Path("../Serie_Historica_PL")
PATH_PARADATA = Path("../Paradata")
PATH_LPEI_CSV = Path("../LPEI/serie_lpei.csv")

GENERATE_METADATA = True
EXCLUDED_PERIODS = ["2020T2"]

LLAVE_PERSONA = [
    "cd_a",
    "cve_ent",
    "con",
    "upm",
    "d_sem",
    "n_pro_viv",
    "v_sel",
    "n_hog",
    "h_mud",
    "n_ent",
    "per",
    "n_ren",
]
LLAVE_HOGAR = [column for column in LLAVE_PERSONA if column != "n_ren"]

# Replace these lists with the canonical columns required by your pipeline.
COLS_COE2 = [*LLAVE_PERSONA, "p6_9", "p6b1", "p6b2", "p6c"]
COLS_SDEM = [
    *LLAVE_PERSONA,
    "r_def",
    "c_res",
    "par_c",
    "sex",
    "eda",
    "n_hij",
    "e_con",
    "t_loc_tri",
    "clase2",
    "pos_ocu",
    "salario",
    "fac_tri",
]


def main() -> None:
    """Run the complete example pipeline."""

    time_series = get_time_series(
        path_in=PATH_ZIPS,
        path_out=PATH_PARQUETS,
        generate_metadata=GENERATE_METADATA,
        path_paradata=PATH_PARADATA,
    )

    required_columns_validation = validate_required_columns(
        time_series=time_series,
        cols_coe2=COLS_COE2,
        cols_sdem=COLS_SDEM,
    )

    _append_required_columns_validation_paradata(
        validation_result=required_columns_validation,
        path_paradata=PATH_PARADATA,
    )

    lpei_monthly = get_pl_series(PATH_LPEI_CSV)
    lpei_quarterly = build_lpei_quarterly_long(lpei_monthly)

    merged_sources, merge_audit = build_merged_time_series(
        time_series=time_series,
        cols_coe2=COLS_COE2,
        cols_sdem=COLS_SDEM,
        llave_persona=LLAVE_PERSONA,
        path_out=PATH_MERGED,
        generate_metadata=GENERATE_METADATA,
        path_paradata=PATH_PARADATA,
    )

    analysis_sources = {}

    for period, merged_path in merged_sources.items():
        merged_validation = validate_merged_period(
            periodo=period,
            path_merged=merged_path,
            llave_persona=LLAVE_PERSONA,
        )

        _append_merged_validation_paradata(
            periodo=period,
            validation_result=merged_validation,
            path_paradata=PATH_PARADATA,
        )

        analysis_result = build_analysis_dataset(
            periodo=period,
            path_merged=merged_path,
            lpei_quarterly=lpei_quarterly,
            llave_persona=LLAVE_PERSONA,
            llave_hogar=LLAVE_HOGAR,
            path_out=PATH_ANALYSIS,
            generate_metadata=GENERATE_METADATA,
            path_paradata=PATH_PARADATA,
        )

        analysis_sources[period] = analysis_result["path_output"]

    pl_result = compute_pl_time_series(
        analysis_sources=analysis_sources,
        path_out=PATH_RESULTS,
        generate_metadata=GENERATE_METADATA,
        path_paradata=PATH_PARADATA,
        excluded_periods=EXCLUDED_PERIODS,
    )

    historical_result = build_historical_analysis_parquet(
        analysis_sources=analysis_sources,
        path_out=PATH_HISTORICAL,
        generate_metadata=GENERATE_METADATA,
        path_paradata=PATH_PARADATA,
        excluded_periods=EXCLUDED_PERIODS,
    )

    print(merge_audit)
    print(pl_result)
    print(historical_result)


if __name__ == "__main__":
    main()
