"""Construcción del dataset analítico trimestral de pobreza laboral."""
from .metadata import _utc_now_iso, _safe_file_size_mb, _safe_rate_pct, _read_parquet_schema_info, _write_json_metadata, _append_period_paradata_step, _build_column_dictionary, _build_quality_indicator_dictionary

def build_analysis_dataset(periodo, path_merged, lpei_quarterly, llave_persona, llave_hogar, path_out='../Analisis_PL_parquet', overwrite=False, compression='zstd', strict=True, event_callback=None, metadata_callback=None, paradata_callback=None, strict_hooks=False, *, generate_metadata=False, path_paradata='../Paradata'):
    """
    Construye el Parquet analítico de pobreza laboral para un
    trimestre de la ENOE.

    La función parte del Parquet unido SDEM-COE2 y aplica:

        1. Selección de residentes con entrevista válida.
        2. Identificación de ocupados y trabajadores sin pago.
        3. Uso prioritario de ingreso directo P6B2.
        4. Imputación mediante P6C y salario mínimo.
        5. Identificación de ingresos no recuperables.
        6. Marcación de hogares con ingreso incompleto.
        7. Cálculo del ingreso laboral del hogar.
        8. Cálculo del ingreso laboral per cápita.
        9. Asignación de ámbito rural o urbano.
       10. Incorporación de la LPEI trimestral.
       11. Construcción de la bandera entra_calculo_pl.

    El archivo de salida conserva a los residentes válidos incluso
    cuando su hogar tiene ingreso incompleto. Estos casos quedan
    identificados mediante hogar_ingreso_incompleto y
    entra_calculo_pl=False.

    Parámetros
    ----------
    periodo : str
        Periodo con formato AAAATX, por ejemplo "2026T1".

    path_merged : str o pathlib.Path
        Ruta del Parquet unido generado por merge_period_parquet().

    lpei_quarterly : pandas.DataFrame
        Serie trimestral de la LPEI.

        Admite formato ancho:
            Año, Trimestre, Rural, Urbano

        También admite formato largo:
            anio, trimestre, ambito, lpei

        Puede contener Año y Trimestre como índice.

    llave_persona : list
        Columnas canónicas que identifican a una persona.

    llave_hogar : list
        Columnas canónicas que identifican al hogar.

    path_out : str o pathlib.Path
        Carpeta raíz de los Parquet analíticos.

    overwrite : bool, default=False
        Reemplaza el archivo de salida si ya existe.

    compression : str, default="zstd"
        Compresión del Parquet de salida.

    strict : bool, default=True
        Detiene el proceso ante inconsistencias críticas.

    event_callback : callable, opcional
        Función que recibirá mensajes de avance.

    metadata_callback : callable, opcional
        Función que recibirá el diccionario de metadatos.

    paradata_callback : callable, opcional
        Función que recibirá el diccionario de paradatos.

    strict_hooks : bool, default=False
        Si es True, un error en los callbacks detiene la función.

    Retorna
    -------
    dict
        Ruta del Parquet analítico, métricas, metadatos,
        paradatos y eventos de ejecución.
    """
    from pathlib import Path
    from datetime import datetime, timezone
    import gc
    import re
    import unicodedata
    import warnings
    import duckdb
    import pandas as pd
    import pyarrow.parquet as pq

    def quote_identifier(identifier):
        """
        Protege un identificador para utilizarlo en SQL.
        """
        identifier = str(identifier).replace('"', '""')
        return f'"{identifier}"'

    def sql_path(path):
        """
        Convierte una ruta a un literal SQL compatible con DuckDB.
        """
        path = Path(path).resolve().as_posix()
        path = path.replace("'", "''")
        return f"'{path}'"

    def unique_ordered(values):
        """
        Elimina elementos repetidos conservando el orden.
        """
        return list(dict.fromkeys(values))

    def normalize_name(value):
        """
        Normaliza texto, elimina acentos y convierte a minúsculas.
        """
        value = str(value).strip()
        value = unicodedata.normalize('NFKD', value)
        value = ''.join((character for character in value if not unicodedata.combining(character)))
        return value.lower()

    def safe_remove(path):
        """
        Elimina un archivo después de liberar manejadores.
        """
        path = Path(path)
        if not path.exists():
            return
        gc.collect()
        try:
            path.unlink()
        except PermissionError as error:
            raise PermissionError(f'No se pudo eliminar el archivo porque está siendo utilizado por otro proceso:\n{path}') from error
    events = []
    analysis_step_start_time = datetime.now(timezone.utc)

    def send_callback(callback, payload, callback_name):
        """
        Envía información a una función externa sin acoplar esta
        función a un formato específico de metadatos o paradatos.
        """
        if callback is None:
            return
        try:
            callback(payload)
        except Exception as error:
            message = f'El callback {callback_name} produjo un error: {type(error).__name__}: {error}'
            if strict_hooks:
                raise RuntimeError(message) from error
            warnings.warn(message)

    def emit_event(stage, status, message, metrics=None):
        """
        Registra un evento y lo envía al callback correspondiente.
        """
        payload = {'timestamp_utc': datetime.now(timezone.utc).isoformat(), 'periodo': periodo, 'etapa': stage, 'estado': status, 'mensaje': message, 'metricas': metrics or {}}
        events.append(payload)
        send_callback(event_callback, payload, 'event_callback')
    match = re.fullmatch('(\\d{4})T([1-4])', str(periodo).upper())
    if not match:
        raise ValueError(f'Periodo inválido: {periodo}. Se esperaba el formato AAAATX.')
    anio, trimestre = match.groups()
    anio = int(anio)
    trimestre = int(trimestre)
    periodo = f'{anio}T{trimestre}'
    llave_persona = unique_ordered([normalize_name(column) for column in llave_persona])
    llave_hogar = unique_ordered([normalize_name(column) for column in llave_hogar])
    if not set(llave_hogar).issubset(set(llave_persona)):
        raise ValueError('La llave de hogar debe ser un subconjunto de la llave de persona.')

    def prepare_lpei_period(lpei_data, year, quarter):
        """
        Convierte la serie trimestral de LPEI en una tabla larga
        con las columnas ambito y lpei para el periodo solicitado.
        """
        if not isinstance(lpei_data, pd.DataFrame):
            raise TypeError('lpei_quarterly debe ser un pandas.DataFrame.')
        lpei_df = lpei_data.copy()
        if isinstance(lpei_df.index, pd.MultiIndex) or lpei_df.index.name is not None:
            lpei_df = lpei_df.reset_index()
        rename_columns = {column: normalize_name(column) for column in lpei_df.columns}
        lpei_df = lpei_df.rename(columns=rename_columns)
        lpei_df = lpei_df.rename(columns={'ano': 'anio', 'año': 'anio'})
        wide_columns = {'anio', 'trimestre', 'rural', 'urbano'}
        if wide_columns.issubset(set(lpei_df.columns)):
            lpei_df['anio'] = pd.to_numeric(lpei_df['anio'], errors='coerce')
            lpei_df['trimestre'] = pd.to_numeric(lpei_df['trimestre'], errors='coerce')
            period_df = lpei_df.loc[lpei_df['anio'].eq(year) & lpei_df['trimestre'].eq(quarter), ['rural', 'urbano']]
            if len(period_df) != 1:
                raise ValueError(f'No se encontró exactamente una fila de LPEI para {year}T{quarter}.')
            period_df = period_df.iloc[0]
            result = pd.DataFrame({'ambito': ['rural', 'urbano'], 'lpei': [period_df['rural'], period_df['urbano']]})
        elif {'anio', 'trimestre', 'ambito', 'lpei'}.issubset(set(lpei_df.columns)):
            lpei_df['anio'] = pd.to_numeric(lpei_df['anio'], errors='coerce')
            lpei_df['trimestre'] = pd.to_numeric(lpei_df['trimestre'], errors='coerce')
            lpei_df['ambito'] = lpei_df['ambito'].map(normalize_name)
            result = lpei_df.loc[lpei_df['anio'].eq(year) & lpei_df['trimestre'].eq(quarter), ['ambito', 'lpei']].copy()
        else:
            raise KeyError('La tabla LPEI debe contener:\n- Año, Trimestre, Rural y Urbano; o\n- anio, trimestre, ambito y lpei.')
        result['lpei'] = pd.to_numeric(result['lpei'], errors='coerce')
        result = result.loc[result['ambito'].isin(['rural', 'urbano'])].drop_duplicates(subset=['ambito']).sort_values('ambito').reset_index(drop=True)
        if set(result['ambito']) != {'rural', 'urbano'}:
            raise ValueError(f'La LPEI de {year}T{quarter} debe contener los ámbitos rural y urbano.')
        if result['lpei'].isna().any() or result['lpei'].le(0).any():
            raise ValueError(f'La LPEI de {year}T{quarter} contiene valores faltantes o no positivos.')
        return result
    lpei_period = prepare_lpei_period(lpei_data=lpei_quarterly, year=anio, quarter=trimestre)
    lpei_values = {row['ambito']: float(row['lpei']) for _, row in lpei_period.iterrows()}
    emit_event(stage='preparacion_lpei', status='ok', message='LPEI trimestral preparada.', metrics=lpei_values)
    path_merged = Path(path_merged)
    path_out = Path(path_out)
    if not path_merged.exists():
        raise FileNotFoundError(f'No existe el Parquet unido:\n{path_merged}')
    required_columns = unique_ordered([*llave_persona, 'r_def', 'c_res', 'par_c', 'sex', 'eda', 'n_hij', 'e_con', 't_loc_tri', 'clase2', 'pos_ocu', 'salario', 'fac_tri', 'emp_ppal', 'tue_ppal', 'hrsocup', 'ing_x_hrs', 'scian', 'p6_9', 'p6b1', 'p6b2', 'p6c', 'p7', 'p7c', 'p7f', 'p7f_dias', 'p7f_horas', 'p7g1', 'p7g2', 'p7g9', 'p7gcan', 'periodo', 'anio', 'trimestre', 'cruce_coe2'])
    parquet_file = pq.ParquetFile(path_merged)
    input_columns = list(parquet_file.schema.names)
    input_rows_metadata = int(parquet_file.metadata.num_rows)
    del parquet_file
    gc.collect()
    normalized_input_columns = {normalize_name(column) for column in input_columns}
    missing_columns = [column for column in required_columns if column not in normalized_input_columns]
    if missing_columns:
        raise KeyError(f'El Parquet unido de {periodo} no contiene las columnas requeridas:\n{missing_columns}')
    emit_event(stage='validacion_entrada', status='ok', message='Esquema del Parquet unido validado.', metrics={'filas_metadata': input_rows_metadata, 'columnas_entrada': len(input_columns)})
    output_folder = path_out / periodo
    output_folder.mkdir(parents=True, exist_ok=True)
    output_path = output_folder / f'ENOE_{periodo}_analisis_pl.parquet'
    temp_path = output_folder / f'ENOE_{periodo}_analisis_pl.tmp.parquet'
    if output_path.exists() and (not overwrite):
        raise FileExistsError(f'El archivo ya existe:\n{output_path}\nUtiliza overwrite=True para reemplazarlo.')
    if temp_path.exists():
        safe_remove(temp_path)
    household_keys_sql = ', '.join([quote_identifier(column) for column in llave_hogar])
    household_join_condition = '\n                AND '.join([f'p.{quote_identifier(column)} = h.{quote_identifier(column)}' for column in llave_hogar])
    household_id_expression = "concat_ws('|', " + ', '.join([f'CAST(p.{quote_identifier(column)} AS VARCHAR)' for column in llave_hogar]) + ')'
    person_id_expression = "concat_ws('|', " + ', '.join([f'CAST(p.{quote_identifier(column)} AS VARCHAR)' for column in llave_persona]) + ')'
    con = duckdb.connect(database=':memory:')
    try:
        con.register('lpei_periodo', lpei_period)
        con.execute(f'\n            CREATE TEMP VIEW merged_source AS\n\n            SELECT *\n            FROM read_parquet(\n                {sql_path(path_merged)}\n            )\n            ')
        key_select_sql = ',\n                '.join([f"NULLIF(TRIM(CAST({quote_identifier(column)} AS VARCHAR)), '') AS {quote_identifier(column)}" for column in llave_persona])
        con.execute(f'\n            CREATE TEMP VIEW base_residentes AS\n\n            SELECT\n                {key_select_sql},\n\n                TRY_CAST(r_def AS INTEGER) AS r_def,\n                TRY_CAST(c_res AS INTEGER) AS c_res,\n\n                TRY_CAST(par_c AS INTEGER)\n                    AS par_c,\n                \n                TRY_CAST(sex AS INTEGER)\n                    AS sex,\n                \n                TRY_CAST(eda AS INTEGER)\n                    AS eda,\n                \n                TRY_CAST(n_hij AS INTEGER)\n                    AS n_hij,\n                \n                TRY_CAST(e_con AS INTEGER)\n                    AS e_con,\n                \n                TRY_CAST(t_loc_tri AS INTEGER)\n                    AS t_loc_tri,\n\n                TRY_CAST(clase2 AS INTEGER)\n                    AS clase2,\n\n                TRY_CAST(pos_ocu AS INTEGER)\n                    AS pos_ocu,\n\n                TRY_CAST(salario AS DOUBLE)\n                    AS salario,\n\n                TRY_CAST(fac_tri AS DOUBLE)\n                    AS fac_tri,\n\n                TRY_CAST(p6_9 AS INTEGER)\n                    AS p6_9,\n\n                TRY_CAST(p6b1 AS INTEGER)\n                    AS p6b1,\n\n                TRY_CAST(p6b2 AS DOUBLE)\n                    AS p6b2,\n\n                TRY_CAST(p6c AS INTEGER)\n                    AS p6c,\n\n                TRY_CAST(emp_ppal AS INTEGER)\n                    AS emp_ppal,\n\n                TRY_CAST(tue_ppal AS INTEGER)\n                    AS tue_ppal,\n\n                TRY_CAST(hrsocup AS DOUBLE)\n                    AS hrsocup,\n\n                TRY_CAST(ing_x_hrs AS DOUBLE)\n                    AS ing_x_hrs,\n\n                TRY_CAST(scian AS INTEGER)\n                    AS scian,\n\n                TRY_CAST(p7 AS INTEGER)\n                    AS p7,\n\n                NULLIF(TRIM(CAST(p7c AS VARCHAR)), '')\n                    AS p7c,\n\n                TRY_CAST(p7f AS INTEGER)\n                    AS p7f,\n\n                TRY_CAST(p7f_dias AS INTEGER)\n                    AS p7f_dias,\n\n                TRY_CAST(p7f_horas AS DOUBLE)\n                    AS p7f_horas,\n\n                TRY_CAST(p7g1 AS INTEGER)\n                    AS p7g1,\n\n                TRY_CAST(p7g2 AS INTEGER)\n                    AS p7g2,\n\n                TRY_CAST(p7g9 AS INTEGER)\n                    AS p7g9,\n\n                TRY_CAST(p7gcan AS DOUBLE)\n                    AS p7gcan,\n\n                CAST(periodo AS VARCHAR)\n                    AS periodo,\n\n                TRY_CAST(anio AS INTEGER)\n                    AS anio,\n\n                TRY_CAST(trimestre AS INTEGER)\n                    AS trimestre,\n\n                CAST(cruce_coe2 AS VARCHAR)\n                    AS cruce_coe2\n\n            FROM merged_source\n\n            WHERE TRY_CAST(r_def AS INTEGER) = 0\n\n              AND TRY_CAST(c_res AS INTEGER)\n                  IN (1, 3)\n            ')
        con.execute("\n            CREATE TEMP VIEW personas_ingreso AS\n\n            WITH flags AS (\n\n                SELECT\n                    *,\n\n                    COALESCE(\n                        clase2 = 1,\n                        FALSE\n                    ) AS es_ocupado,\n\n                    COALESCE(\n                        clase2 = 1\n                        AND (\n                            pos_ocu = 4\n                            OR p6_9 = 9\n                        ),\n                        FALSE\n                    ) AS es_sin_pago,\n\n                    COALESCE(\n                        p6b2 BETWEEN 1 AND 999998,\n                        FALSE\n                    ) AS p6b2_valido,\n\n                    CASE p6c\n                        WHEN 1 THEN 0.5\n                        WHEN 2 THEN 1.0\n                        WHEN 3 THEN 1.5\n                        WHEN 4 THEN 2.5\n                        WHEN 5 THEN 4.0\n                        WHEN 6 THEN 7.5\n                        WHEN 7 THEN 10.0\n                        ELSE NULL\n                    END AS multiplicador_p6c\n\n                FROM base_residentes\n            ),\n\n            recovery AS (\n\n                SELECT\n                    *,\n\n                    (\n                        multiplicador_p6c IS NOT NULL\n                        AND salario IS NOT NULL\n                        AND salario > 0\n                    ) AS p6c_valido,\n\n                    multiplicador_p6c * salario\n                        AS ingreso_imputado_p6c\n\n                FROM flags\n            ),\n\n            income AS (\n\n                SELECT\n                    *,\n\n                    CASE\n                        WHEN NOT es_ocupado\n                            THEN 0.0\n\n                        WHEN es_sin_pago\n                            THEN 0.0\n\n                        WHEN p6b2_valido\n                            THEN p6b2\n\n                        WHEN p6c_valido\n                            THEN ingreso_imputado_p6c\n\n                        ELSE NULL\n                    END AS ingreso_laboral_ind,\n\n                    CASE\n                        WHEN NOT es_ocupado\n                            THEN 'no_ocupado'\n\n                        WHEN es_sin_pago\n                            THEN 'sin_pago'\n\n                        WHEN p6b2_valido\n                            THEN 'directo_p6b2'\n\n                        WHEN p6c_valido\n                            THEN 'imputado_p6c'\n\n                        ELSE 'no_recuperable'\n                    END AS fuente_ingreso\n\n                FROM recovery\n            )\n\n            SELECT\n                *,\n\n                (\n                    es_ocupado\n                    AND NOT es_sin_pago\n                    AND ingreso_laboral_ind IS NULL\n                ) AS ingreso_no_recuperable\n\n            FROM income\n            ")
        con.execute(f"\n            CREATE TEMP VIEW hogares AS\n\n            SELECT\n                {household_keys_sql},\n\n                COUNT(*) AS integrantes_hogar,\n\n                SUM(\n                    COALESCE(\n                        ingreso_laboral_ind,\n                        0.0\n                    )\n                ) AS ingreso_hogar_observado,\n\n                BOOL_OR(\n                    ingreso_no_recuperable\n                ) AS hogar_ingreso_incompleto,\n\n                SUM(\n                    CASE\n                        WHEN fuente_ingreso\n                             = 'directo_p6b2'\n                        THEN 1\n                        ELSE 0\n                    END\n                ) AS ingresos_directos_hogar,\n\n                SUM(\n                    CASE\n                        WHEN fuente_ingreso\n                             = 'imputado_p6c'\n                        THEN 1\n                        ELSE 0\n                    END\n                ) AS ingresos_imputados_hogar,\n\n                SUM(\n                    CASE\n                        WHEN ingreso_no_recuperable\n                        THEN 1\n                        ELSE 0\n                    END\n                ) AS ingresos_no_recuperables_hogar\n\n            FROM personas_ingreso\n\n            GROUP BY\n                {household_keys_sql}\n            ")
        con.execute(f"\n            CREATE TEMP VIEW analisis_pre AS\n\n            SELECT\n                p.*,\n\n                p.par_c AS parentesco_codigo,\n\n                p.e_con AS estado_conyugal_codigo,\n                \n                p.n_hij AS numero_hijos_declarado,\n                \n                CASE\n                    WHEN p.n_hij IS NULL\n                        THEN NULL\n                \n                    WHEN p.n_hij = 0\n                        THEN FALSE\n                \n                    WHEN p.n_hij > 0\n                        THEN TRUE\n                \n                    ELSE NULL\n                END AS tiene_hijos_declarados,\n\n                {household_id_expression}\n                    AS id_hogar,\n\n                {person_id_expression}\n                    AS id_persona,\n\n                '{periodo}' || '|'\n                    || {household_id_expression}\n                    AS id_hogar_periodo,\n\n                '{periodo}' || '|'\n                    || {person_id_expression}\n                    AS id_persona_periodo,\n\n                h.integrantes_hogar,\n\n                h.ingreso_hogar_observado,\n\n                h.hogar_ingreso_incompleto,\n\n                h.ingresos_directos_hogar,\n\n                h.ingresos_imputados_hogar,\n\n                h.ingresos_no_recuperables_hogar,\n\n                CASE\n                    WHEN NOT h.hogar_ingreso_incompleto\n                    THEN h.ingreso_hogar_observado\n                    ELSE NULL\n                END AS ingreso_hogar,\n\n                CASE\n                    WHEN NOT h.hogar_ingreso_incompleto\n                    THEN (\n                        h.ingreso_hogar_observado\n                        / NULLIF(\n                            h.integrantes_hogar,\n                            0\n                        )\n                    )\n                    ELSE NULL\n                END AS ingreso_pc,\n\n                CASE\n                    WHEN p.t_loc_tri = 4\n                    THEN 'rural'\n\n                    WHEN p.t_loc_tri IN (1, 2, 3)\n                    THEN 'urbano'\n\n                    ELSE NULL\n                END AS ambito,\n\n                (\n                    p.fac_tri IS NOT NULL\n                    AND p.fac_tri > 0\n                ) AS factor_valido\n\n            FROM personas_ingreso AS p\n\n            LEFT JOIN hogares AS h\n                ON {household_join_condition}\n            ")
        con.execute("\n            CREATE TEMP VIEW analisis_final AS\n        \n            SELECT\n                a.*,\n        \n                l.lpei,\n        \n                (\n                    NOT a.hogar_ingreso_incompleto\n                    AND a.ingreso_pc IS NOT NULL\n                    AND l.lpei IS NOT NULL\n                    AND a.factor_valido\n                ) AS entra_calculo_pl,\n        \n                CASE\n                    WHEN a.hogar_ingreso_incompleto\n                        THEN 'hogar_ingreso_incompleto'\n        \n                    WHEN a.ingreso_pc IS NULL\n                        THEN 'ingreso_pc_no_disponible'\n        \n                    WHEN l.lpei IS NULL\n                        THEN 'lpei_no_asignada'\n        \n                    WHEN NOT a.factor_valido\n                        THEN 'factor_expansion_invalido'\n        \n                    ELSE 'elegible'\n                END AS motivo_elegibilidad_pl,\n        \n                CASE\n                    WHEN NOT a.hogar_ingreso_incompleto\n                     AND a.ingreso_pc IS NOT NULL\n                     AND l.lpei IS NOT NULL\n                     AND l.lpei > 0\n                     AND a.factor_valido\n        \n                    THEN a.ingreso_pc / l.lpei\n        \n                    ELSE NULL\n                END AS ingreso_pc_lpei_ratio,\n        \n                CASE\n                    WHEN NOT a.hogar_ingreso_incompleto\n                     AND a.ingreso_pc IS NOT NULL\n                     AND l.lpei IS NOT NULL\n                     AND a.factor_valido\n        \n                    THEN a.ingreso_pc < l.lpei\n        \n                    ELSE NULL\n                END AS pobreza_laboral_persona\n        \n            FROM analisis_pre AS a\n        \n            LEFT JOIN lpei_periodo AS l\n                ON a.ambito = l.ambito\n            ")
        filas_entrada = int(con.execute('\n                SELECT COUNT(*)\n                FROM merged_source\n                ').fetchone()[0])
        residentes_validos = int(con.execute('\n                SELECT COUNT(*)\n                FROM base_residentes\n                ').fetchone()[0])
        descartados_residencia = filas_entrada - residentes_validos
        income_metrics = con.execute("\n            SELECT\n                SUM(\n                    CASE\n                        WHEN es_ocupado\n                        THEN 1 ELSE 0\n                    END\n                ) AS ocupados,\n\n                SUM(\n                    CASE\n                        WHEN es_ocupado\n                         AND NOT es_sin_pago\n                        THEN 1 ELSE 0\n                    END\n                ) AS ocupados_remunerados,\n\n                SUM(\n                    CASE\n                        WHEN es_sin_pago\n                        THEN 1 ELSE 0\n                    END\n                ) AS trabajadores_sin_pago,\n\n                SUM(\n                    CASE\n                        WHEN fuente_ingreso\n                             = 'directo_p6b2'\n                        THEN 1 ELSE 0\n                    END\n                ) AS ingresos_directos,\n\n                SUM(\n                    CASE\n                        WHEN fuente_ingreso\n                             = 'imputado_p6c'\n                        THEN 1 ELSE 0\n                    END\n                ) AS ingresos_imputados,\n\n                SUM(\n                    CASE\n                        WHEN ingreso_no_recuperable\n                        THEN 1 ELSE 0\n                    END\n                ) AS ingresos_no_recuperables,\n\n                SUM(\n                    CASE\n                        WHEN fuente_ingreso IN (\n                            'imputado_p6c',\n                            'no_recuperable'\n                        )\n                        THEN 1 ELSE 0\n                    END\n                ) AS candidatos_imputacion\n\n            FROM personas_ingreso\n            ").fetchone()
        ocupados = int(income_metrics[0] or 0)
        ocupados_remunerados = int(income_metrics[1] or 0)
        trabajadores_sin_pago = int(income_metrics[2] or 0)
        ingresos_directos = int(income_metrics[3] or 0)
        ingresos_imputados = int(income_metrics[4] or 0)
        ingresos_no_recuperables = int(income_metrics[5] or 0)
        candidatos_imputacion = int(income_metrics[6] or 0)
        household_metrics = con.execute('\n            SELECT\n                COUNT(*) AS hogares_totales,\n\n                SUM(\n                    CASE\n                        WHEN hogar_ingreso_incompleto\n                        THEN 1 ELSE 0\n                    END\n                ) AS hogares_incompletos\n\n            FROM hogares\n            ').fetchone()
        hogares_totales = int(household_metrics[0] or 0)
        hogares_incompletos = int(household_metrics[1] or 0)
        eligibility_metrics = con.execute('\n            SELECT\n                SUM(\n                    CASE\n                        WHEN hogar_ingreso_incompleto\n                        THEN 1 ELSE 0\n                    END\n                ) AS personas_hogares_incompletos,\n\n                SUM(\n                    CASE\n                        WHEN entra_calculo_pl\n                        THEN 1 ELSE 0\n                    END\n                ) AS personas_elegibles,\n\n                SUM(\n                    CASE\n                        WHEN NOT factor_valido\n                        THEN 1 ELSE 0\n                    END\n                ) AS factores_invalidos,\n\n                SUM(\n                    CASE\n                        WHEN hogar_ingreso_incompleto\n                         AND factor_valido\n                        THEN fac_tri\n                        ELSE 0\n                    END\n                ) AS poblacion_excluida_expandida,\n\n                SUM(\n                    CASE\n                        WHEN factor_valido\n                        THEN fac_tri\n                        ELSE 0\n                    END\n                ) AS poblacion_residente_expandida\n\n            FROM analisis_final\n            ').fetchone()
        personas_hogares_incompletos = int(eligibility_metrics[0] or 0)
        personas_elegibles = int(eligibility_metrics[1] or 0)
        factores_invalidos = int(eligibility_metrics[2] or 0)
        poblacion_excluida_expandida = float(eligibility_metrics[3] or 0)
        poblacion_residente_expandida = float(eligibility_metrics[4] or 0)
        demographic_metrics = con.execute('\n            SELECT\n                SUM(\n                    CASE\n                        WHEN e_con IS NULL\n                        THEN 1 ELSE 0\n                    END\n                ) AS estado_conyugal_no_disponible,\n\n                SUM(\n                    CASE\n                        WHEN e_con IS NOT NULL\n                        THEN 1 ELSE 0\n                    END\n                ) AS estado_conyugal_disponible,\n\n                SUM(\n                    CASE\n                        WHEN n_hij IS NULL\n                        THEN 1 ELSE 0\n                    END\n                ) AS numero_hijos_no_disponible,\n\n                SUM(\n                    CASE\n                        WHEN n_hij IS NOT NULL\n                        THEN 1 ELSE 0\n                    END\n                ) AS numero_hijos_disponible,\n\n                SUM(\n                    CASE\n                        WHEN par_c IS NULL\n                        THEN 1 ELSE 0\n                    END\n                ) AS parentesco_no_disponible,\n\n                SUM(\n                    CASE\n                        WHEN par_c IS NOT NULL\n                        THEN 1 ELSE 0\n                    END\n                ) AS parentesco_disponible,\n\n                SUM(\n                    CASE\n                        WHEN tiene_hijos_declarados = TRUE\n                        THEN 1 ELSE 0\n                    END\n                ) AS personas_con_hijos_declarados,\n\n                SUM(\n                    CASE\n                        WHEN tiene_hijos_declarados = FALSE\n                        THEN 1 ELSE 0\n                    END\n                ) AS personas_sin_hijos_declarados\n\n            FROM analisis_final\n            ').fetchone()
        estado_conyugal_no_disponible = int(demographic_metrics[0] or 0)
        estado_conyugal_disponible = int(demographic_metrics[1] or 0)
        numero_hijos_no_disponible = int(demographic_metrics[2] or 0)
        numero_hijos_disponible = int(demographic_metrics[3] or 0)
        parentesco_no_disponible = int(demographic_metrics[4] or 0)
        parentesco_disponible = int(demographic_metrics[5] or 0)
        personas_con_hijos_declarados = int(demographic_metrics[6] or 0)
        personas_sin_hijos_declarados = int(demographic_metrics[7] or 0)
        weighted_imputation_metrics = con.execute("\n            SELECT\n                SUM(\n                    CASE\n                        WHEN entra_calculo_pl\n                        THEN fac_tri\n                        ELSE 0\n                    END\n                ) AS poblacion_elegible_expandida,\n\n                SUM(\n                    CASE\n                        WHEN entra_calculo_pl\n                         AND fuente_ingreso = 'imputado_p6c'\n                        THEN fac_tri\n                        ELSE 0\n                    END\n                ) AS poblacion_imputada_expandida,\n\n                SUM(\n                    CASE\n                        WHEN entra_calculo_pl\n                         AND fuente_ingreso = 'imputado_p6c'\n                        THEN 1\n                        ELSE 0\n                    END\n                ) AS registros_imputados_elegibles\n\n            FROM analisis_final\n            ").fetchone()
        poblacion_elegible_expandida = float(weighted_imputation_metrics[0] or 0)
        poblacion_imputada_expandida = float(weighted_imputation_metrics[1] or 0)
        registros_imputados_elegibles = int(weighted_imputation_metrics[2] or 0)
        try:
            poverty_metrics = con.execute('\n                SELECT\n                    SUM(\n                        CASE\n                            WHEN entra_calculo_pl\n                             AND factor_valido\n                            THEN 1 ELSE 0\n                        END\n                    ) AS sample_eligible_records,\n\n                    SUM(\n                        CASE\n                            WHEN entra_calculo_pl\n                             AND factor_valido\n                             AND pobreza_laboral_persona\n                            THEN 1 ELSE 0\n                        END\n                    ) AS sample_poverty_records,\n\n                    SUM(\n                        CASE\n                            WHEN entra_calculo_pl\n                             AND factor_valido\n                            THEN fac_tri\n                            ELSE 0\n                        END\n                    ) AS expanded_eligible_population_check,\n\n                    SUM(\n                        CASE\n                            WHEN entra_calculo_pl\n                             AND factor_valido\n                             AND pobreza_laboral_persona\n                            THEN fac_tri\n                            ELSE 0\n                        END\n                    ) AS expanded_poverty_population\n                FROM analisis_final\n                ').fetchone()
            sample_eligible_records = int(poverty_metrics[0] or 0)
            sample_poverty_records = int(poverty_metrics[1] or 0)
            expanded_poverty_population = float(poverty_metrics[3] or 0)
        except Exception:
            sample_eligible_records = 0
            sample_poverty_records = 0
            expanded_poverty_population = 0.0
        tasa_imputacion_candidatos = ingresos_imputados / candidatos_imputacion * 100 if candidatos_imputacion > 0 else 0.0
        tasa_imputacion_remunerados = ingresos_imputados / ocupados_remunerados * 100 if ocupados_remunerados > 0 else 0.0
        tasa_no_recuperable = ingresos_no_recuperables / ocupados_remunerados * 100 if ocupados_remunerados > 0 else 0.0
        tasa_hogares_incompletos = hogares_incompletos / hogares_totales * 100 if hogares_totales > 0 else 0.0
        tasa_personas_excluidas = personas_hogares_incompletos / residentes_validos * 100 if residentes_validos > 0 else 0.0
        tasa_perdida_expandida = poblacion_excluida_expandida / poblacion_residente_expandida * 100 if poblacion_residente_expandida > 0 else 0.0
        tasa_cobertura_imputacion = tasa_imputacion_candidatos
        proporcion_registros_imputados = registros_imputados_elegibles / personas_elegibles * 100 if personas_elegibles > 0 else 0.0
        proporcion_poblacional_imputada = poblacion_imputada_expandida / poblacion_elegible_expandida * 100 if poblacion_elegible_expandida > 0 else 0.0
        tasa_estado_conyugal_no_disponible = estado_conyugal_no_disponible / residentes_validos * 100 if residentes_validos > 0 else 0.0
        tasa_numero_hijos_no_disponible = numero_hijos_no_disponible / residentes_validos * 100 if residentes_validos > 0 else 0.0
        tasa_parentesco_no_disponible = parentesco_no_disponible / residentes_validos * 100 if residentes_validos > 0 else 0.0
        tasa_con_hijos_entre_declarantes = personas_con_hijos_declarados / numero_hijos_disponible * 100 if numero_hijos_disponible > 0 else 0.0
        emit_event(stage='metricas_sociodemograficas', status='ok', message='Métricas de estado conyugal, número de hijos y parentesco calculadas.', metrics={'estado_conyugal_disponible': estado_conyugal_disponible, 'estado_conyugal_no_disponible': estado_conyugal_no_disponible, 'numero_hijos_disponible': numero_hijos_disponible, 'numero_hijos_no_disponible': numero_hijos_no_disponible, 'parentesco_disponible': parentesco_disponible, 'parentesco_no_disponible': parentesco_no_disponible, 'personas_con_hijos_declarados': personas_con_hijos_declarados, 'personas_sin_hijos_declarados': personas_sin_hijos_declarados})
        duplicate_person_ids = int(con.execute('\n                SELECT COALESCE(\n                    SUM(n - 1),\n                    0\n                )\n\n                FROM (\n                    SELECT\n                        id_persona_periodo,\n                        COUNT(*) AS n\n\n                    FROM analisis_final\n\n                    GROUP BY\n                        id_persona_periodo\n\n                    HAVING COUNT(*) > 1\n                )\n                ').fetchone()[0])
        unknown_scope = int(con.execute('\n                SELECT COUNT(*)\n                FROM analisis_final\n                WHERE ambito IS NULL\n                ').fetchone()[0])
        if strict:
            validation_errors = []
            if residentes_validos == 0:
                validation_errors.append('No existen residentes válidos.')
            if duplicate_person_ids > 0:
                validation_errors.append(f'Hay {duplicate_person_ids:,} duplicados en id_persona_periodo.')
            if unknown_scope > 0:
                validation_errors.append(f'Hay {unknown_scope:,} registros sin ámbito rural o urbano.')
            if validation_errors:
                raise ValueError(f'[{periodo}] Falló la construcción del dataset analítico:\n' + '\n'.join((f'- {error}' for error in validation_errors)))
        compression_sql = str(compression).strip().upper()
        con.execute(f"\n            COPY (\n\n                SELECT *\n                FROM analisis_final\n\n            )\n\n            TO {sql_path(temp_path)}\n\n            (\n                FORMAT PARQUET,\n                COMPRESSION '{compression_sql}',\n                ROW_GROUP_SIZE 100000\n            )\n            ")
        metrics = {'filas_entrada': filas_entrada, 'residentes_validos': residentes_validos, 'descartados_por_residencia': descartados_residencia, 'ocupados': ocupados, 'ocupados_remunerados': ocupados_remunerados, 'trabajadores_sin_pago': trabajadores_sin_pago, 'ingresos_directos_p6b2': ingresos_directos, 'candidatos_imputacion': candidatos_imputacion, 'ingresos_imputados_p6c': ingresos_imputados, 'ingresos_no_recuperables': ingresos_no_recuperables, 'hogares_totales': hogares_totales, 'hogares_ingreso_incompleto': hogares_incompletos, 'personas_hogares_incompletos': personas_hogares_incompletos, 'personas_elegibles_calculo': personas_elegibles, 'factores_expansion_invalidos': factores_invalidos, 'duplicados_id_persona_periodo': duplicate_person_ids, 'registros_ambito_desconocido': unknown_scope, 'tasa_imputacion_candidatos_pct': tasa_imputacion_candidatos, 'tasa_imputacion_remunerados_pct': tasa_imputacion_remunerados, 'tasa_ingreso_no_recuperable_pct': tasa_no_recuperable, 'tasa_hogares_incompletos_pct': tasa_hogares_incompletos, 'tasa_personas_excluidas_pct': tasa_personas_excluidas, 'tasa_perdida_expandida_pct': tasa_perdida_expandida, 'tasa_cobertura_imputacion_pct': tasa_cobertura_imputacion, 'registros_imputados_elegibles': registros_imputados_elegibles, 'proporcion_registros_imputados_pct': proporcion_registros_imputados, 'poblacion_elegible_expandida': poblacion_elegible_expandida, 'poblacion_imputada_expandida': poblacion_imputada_expandida, 'proporcion_poblacional_imputada_pct': proporcion_poblacional_imputada, 'estado_conyugal_disponible': estado_conyugal_disponible, 'estado_conyugal_no_disponible': estado_conyugal_no_disponible, 'numero_hijos_disponible': numero_hijos_disponible, 'numero_hijos_no_disponible': numero_hijos_no_disponible, 'parentesco_disponible': parentesco_disponible, 'parentesco_no_disponible': parentesco_no_disponible, 'personas_con_hijos_declarados': personas_con_hijos_declarados, 'personas_sin_hijos_declarados': personas_sin_hijos_declarados, 'tasa_estado_conyugal_no_disponible_pct': tasa_estado_conyugal_no_disponible, 'tasa_numero_hijos_no_disponible_pct': tasa_numero_hijos_no_disponible, 'tasa_parentesco_no_disponible_pct': tasa_parentesco_no_disponible, 'tasa_con_hijos_entre_declarantes_pct': tasa_con_hijos_entre_declarantes}
    except Exception:
        con.close()
        con = None
        gc.collect()
        if temp_path.exists():
            try:
                temp_path.unlink()
            except PermissionError:
                pass
        emit_event(stage='construccion_dataset', status='error', message='Falló la construcción del dataset analítico.')
        raise
    finally:
        if con is not None:
            con.close()
        gc.collect()
    if not temp_path.exists():
        raise RuntimeError(f'No se generó el archivo temporal esperado:\n{temp_path}')
    parquet_result = pq.ParquetFile(temp_path)
    output_rows = int(parquet_result.metadata.num_rows)
    output_columns = list(parquet_result.schema.names)
    del parquet_result
    gc.collect()
    if output_rows != residentes_validos:
        safe_remove(temp_path)
        raise RuntimeError(f'[{periodo}] El número de filas del Parquet analítico no coincide con los residentes válidos:\nResidentes válidos: {residentes_validos:,}\nFilas de salida: {output_rows:,}')
    if output_path.exists():
        safe_remove(output_path)
    temp_path.replace(output_path)
    emit_event(stage='construccion_dataset', status='ok', message='Dataset analítico construido correctamente.', metrics={'filas_salida': output_rows, 'columnas_salida': len(output_columns), 'path_output': str(output_path)})
    metadata = {'function': 'build_analysis_dataset', 'function_version': '1.1.0', 'timestamp_utc': datetime.now(timezone.utc).isoformat(), 'periodo': periodo, 'anio': anio, 'trimestre': trimestre, 'archivo_entrada': str(path_merged), 'archivo_salida': str(output_path), 'formato_salida': 'parquet', 'compresion': compression, 'filas_entrada': input_rows_metadata, 'filas_salida': output_rows, 'columnas_entrada': input_columns, 'columnas_salida': output_columns, 'llave_persona': llave_persona, 'llave_hogar': llave_hogar, 'lpei': lpei_values, 'metodologia_ingreso': {'prioridad_1': 'p6b2', 'prioridad_2': 'p6c_por_salario', 'sin_pago': 0, 'no_ocupado': 0, 'no_recuperable': 'hogar marcado como ingreso incompleto'}, 'metricas_imputacion': {'cobertura_imputacion': 'ingresos imputados / candidatos a imputacion', 'proporcion_registros_imputados': 'registros imputados elegibles / personas elegibles', 'proporcion_poblacional_imputada': 'suma fac_tri imputados elegibles / suma fac_tri personas elegibles'}, 'variables_sociodemograficas': {'parentesco_original': 'par_c', 'parentesco_derivada': 'parentesco_codigo', 'estado_conyugal_original': 'e_con', 'estado_conyugal_derivada': 'estado_conyugal_codigo', 'numero_hijos_original': 'n_hij', 'numero_hijos_derivada': 'numero_hijos_declarado', 'indicador_hijos': 'tiene_hijos_declarados', 'uso_en_elegibilidad_pl': False, 'observacion': 'La ausencia de estas variables no elimina registros del cálculo de pobreza laboral.'}}
    paradata = {'periodo': periodo, 'etapa': 'construccion_dataset_analitico_pl', 'transformaciones': ['filtrado_r_def_igual_0', 'filtrado_c_res_en_1_3', 'normalizacion_variables_numericas', 'identificacion_ocupados', 'identificacion_trabajadores_sin_pago', 'uso_ingreso_directo_p6b2', 'imputacion_ingreso_p6c_por_salario', 'identificacion_ingresos_no_recuperables', 'agregacion_ingreso_hogar', 'conteo_integrantes_hogar', 'calculo_ingreso_per_capita', 'asignacion_ambito', 'incorporacion_lpei', 'marcacion_elegibilidad_calculo', 'conservacion_parentesco_hogar', 'conservacion_estado_conyugal', 'conservacion_numero_hijos', 'creacion_indicador_hijos_declarados', 'calculo_metricas_sociodemograficas', 'calculo_ratio_ingreso_pc_lpei', 'clasificacion_pobreza_laboral_persona', 'calculo_tres_metricas_imputacion'], 'metricas': metrics, 'archivo_entrada': str(path_merged), 'archivo_salida': str(output_path)}
    send_callback(metadata_callback, metadata, 'metadata_callback')
    send_callback(paradata_callback, paradata, 'paradata_callback')
    if generate_metadata:
        try:
            metadata_gen_start = datetime.now(timezone.utc)
            schema_info = _read_parquet_schema_info(str(output_path))
            column_dict = {}
            if schema_info is not None:
                column_names = schema_info.get('column_names', [])
                column_types = schema_info.get('column_types', [])
                column_dict = _build_column_dictionary(column_names, dataset_type='analysis')
                for idx, col in enumerate(column_names):
                    if col in column_dict:
                        column_dict[col]['type'] = column_types[idx]
                    else:
                        column_dict[col] = {'label': col, 'description': '', 'source': None, 'type': column_types[idx] if idx < len(column_types) else None, 'unit': None, 'role': None, 'used_in': []}
            valid_resident_rate = residentes_validos / filas_entrada * 100 if filas_entrada > 0 else None
            direct_p6b2_rate = ingresos_directos / ocupados_remunerados * 100 if ocupados_remunerados > 0 else None
            unrecoverable_rate = tasa_no_recuperable
            invalid_fac_rate = factores_invalidos / residentes_validos * 100 if residentes_validos > 0 else None
            lpei_assignment_rate = (residentes_validos - unknown_scope) / residentes_validos * 100 if residentes_validos > 0 else None
            unknown_scope_rate = unknown_scope / residentes_validos * 100 if residentes_validos > 0 else None
            poverty_rate_pct = expanded_poverty_population / poblacion_elegible_expandida * 100 if poblacion_elegible_expandida > 0 else None
            discarded_by_residence_rate = _safe_rate_pct(descartados_residencia, filas_entrada)
            p6c_imputed_rate = _safe_rate_pct(ingresos_imputados, ocupados_remunerados)
            recovered_income_records = ingresos_directos + ingresos_imputados
            recovered_income_rate = _safe_rate_pct(recovered_income_records, ocupados_remunerados)
            non_imputed_candidates_records = candidatos_imputacion - ingresos_imputados
            non_imputed_candidates_rate = _safe_rate_pct(non_imputed_candidates_records, candidatos_imputacion)
            households_income_incomplete_rate = _safe_rate_pct(hogares_incompletos, hogares_totales)
            valid_fac_tri_records = residentes_validos - factores_invalidos
            valid_fac_tri_rate = _safe_rate_pct(valid_fac_tri_records, residentes_validos)
            non_eligible_records = residentes_validos - personas_elegibles
            eligibility_rate = _safe_rate_pct(personas_elegibles, residentes_validos)
            poverty_unweighted_rate = _safe_rate_pct(sample_poverty_records, sample_eligible_records)
            rural_records = None
            urban_records = None
            rural_expanded_population = None
            urban_expanded_population = None
            metadata_warnings = []
            try:
                import duckdb
                con_meta = duckdb.connect(database=':memory:')
                tbl_sql_meta = f"SELECT * FROM read_parquet('{output_path.resolve().as_posix()}')"
                if 'ambito' in (column_names if schema_info is not None else output_columns):
                    rural_records = con_meta.execute(f"SELECT COUNT(*) FROM ({tbl_sql_meta}) WHERE ambito = 'rural'").fetchone()[0]
                    urban_records = con_meta.execute(f"SELECT COUNT(*) FROM ({tbl_sql_meta}) WHERE ambito = 'urbano'").fetchone()[0]
                    if 'fac_tri' in (column_names if schema_info is not None else output_columns):
                        rural_expanded_population = con_meta.execute(f"\n                            SELECT SUM(CASE WHEN ambito = 'rural' AND factor_valido THEN fac_tri ELSE 0 END)\n                            FROM ({tbl_sql_meta})\n                            ").fetchone()[0]
                        urban_expanded_population = con_meta.execute(f"\n                            SELECT SUM(CASE WHEN ambito = 'urbano' AND factor_valido THEN fac_tri ELSE 0 END)\n                            FROM ({tbl_sql_meta})\n                            ").fetchone()[0]
                        rural_expanded_population = float(rural_expanded_population or 0)
                        urban_expanded_population = float(urban_expanded_population or 0)
                con_meta.close()
            except Exception as metadata_error:
                metadata_warnings.append(f'No fue posible calcular todos los indicadores documentales de LPEI/ámbito: {metadata_error}')
            quality_dict = _build_quality_indicator_dictionary(dataset_type='analysis')
            quality_indicators = {'residence_quality': {'input_rows': filas_entrada, 'valid_residents': residentes_validos, 'discarded_by_residence': descartados_residencia, 'valid_resident_rate_pct': valid_resident_rate, 'discarded_by_residence_rate_pct': discarded_by_residence_rate}, 'income_quality': {'occupied_persons': ocupados, 'remunerated_occupied_persons': ocupados_remunerados, 'unpaid_workers': trabajadores_sin_pago, 'direct_p6b2_income_records': ingresos_directos, 'p6c_imputed_income_records': ingresos_imputados, 'unrecoverable_income_records': ingresos_no_recuperables, 'direct_p6b2_rate_pct': direct_p6b2_rate, 'p6c_imputed_rate_pct': p6c_imputed_rate, 'unrecoverable_income_rate_pct': unrecoverable_rate, 'recovered_income_records': recovered_income_records, 'recovered_income_rate_pct': recovered_income_rate}, 'imputation_quality': {'candidates_for_imputation': candidatos_imputacion, 'imputation_rate_candidates_pct': tasa_imputacion_candidatos, 'imputation_rate_remunerated_pct': tasa_imputacion_remunerados, 'imputed_eligible_records': registros_imputados_elegibles, 'imputed_eligible_record_share_pct': proporcion_registros_imputados, 'expanded_imputed_population': poblacion_imputada_expandida, 'expanded_imputed_population_share_pct': proporcion_poblacional_imputada, 'non_imputed_candidates_records': non_imputed_candidates_records, 'non_imputed_candidates_rate_pct': non_imputed_candidates_rate}, 'exclusion_quality': {'households_total': hogares_totales, 'households_income_incomplete': hogares_incompletos, 'households_income_incomplete_rate_pct': households_income_incomplete_rate, 'persons_in_incomplete_income_households': personas_hogares_incompletos, 'exclusion_rate_records_pct': tasa_personas_excluidas, 'exclusion_rate_expanded_pct': tasa_perdida_expandida, 'expanded_excluded_population': poblacion_excluida_expandida}, 'expansion_factor_quality': {'invalid_fac_tri_records': factores_invalidos, 'invalid_fac_tri_rate_pct': invalid_fac_rate, 'valid_fac_tri_records': valid_fac_tri_records, 'valid_fac_tri_rate_pct': valid_fac_tri_rate, 'expanded_valid_population': poblacion_residente_expandida, 'expanded_resident_population': poblacion_residente_expandida}, 'lpei_quality': {'lpei_rural': lpei_values.get('rural'), 'lpei_urban': lpei_values.get('urbano'), 'records_without_lpei': unknown_scope, 'lpei_assignment_rate_pct': lpei_assignment_rate, 'unknown_scope_records': unknown_scope, 'unknown_scope_rate_pct': unknown_scope_rate, 'rural_records': int(rural_records) if rural_records is not None else None, 'urban_records': int(urban_records) if urban_records is not None else None, 'rural_expanded_population': rural_expanded_population, 'urban_expanded_population': urban_expanded_population}, 'indicator_quality': {'eligible_records': personas_elegibles, 'non_eligible_records': non_eligible_records, 'eligibility_rate_pct': eligibility_rate, 'expanded_eligible_population': poblacion_elegible_expandida, 'expanded_poverty_population': expanded_poverty_population, 'sample_eligible_records': sample_eligible_records, 'sample_poverty_records': sample_poverty_records, 'poverty_labor_rate_pct': poverty_rate_pct, 'poverty_labor_rate_unweighted_pct': poverty_unweighted_rate}, 'sociodemographic_quality': {'missing_marital_status_records': estado_conyugal_no_disponible, 'missing_children_number_records': numero_hijos_no_disponible, 'missing_household_relationship_records': parentesco_no_disponible, 'missing_marital_status_rate_pct': tasa_estado_conyugal_no_disponible, 'missing_children_number_rate_pct': tasa_numero_hijos_no_disponible, 'missing_household_relationship_rate_pct': tasa_parentesco_no_disponible, 'available_marital_status_records': estado_conyugal_disponible, 'available_children_number_records': numero_hijos_disponible, 'available_household_relationship_records': parentesco_disponible}}
            metadata_json = {'general_information': {'dataset_name': f'ENOE_{periodo}_analisis_PL', 'dataset_period': periodo, 'dataset_level': 'persona', 'dataset_type': 'analysis', 'created_at_utc': _utc_now_iso(), 'created_by_function': 'build_analysis_dataset', 'function_version': '2.2.0', 'project_name': 'Calculo de pobreza laboral'}, 'source_data': {'merged_dataset': str(path_merged), 'lpei_source': 'lpei_quarterly_provided'}, 'construction_method': {'function': 'build_analysis_dataset', 'method_description': 'Construcción del dataset analítico de pobreza laboral: filtrado de residentes válidos, determinación de ocupados y trabajadores sin pago, imputación de ingresos, agregación por hogar, cálculo de ingreso per cápita, asignación de LPEI y determinación de pobreza laboral.', 'compression': compression, 'overwrite': overwrite, 'strict': strict}, 'dataset_size': {'input_rows': filas_entrada, 'output_rows': output_rows, 'columns_output': len(output_columns), 'row_groups': schema_info.get('row_groups') if schema_info else None, 'file_size_mb': _safe_file_size_mb(str(output_path))}, 'schema': {'column_count': len(output_columns), 'columns': column_dict}, 'critical_variables': ['r_def', 'c_res', 'clase2', 'pos_ocu', 'fac_tri', 'p6b2', 'p6c', 'ingreso_pc', 'lpei', 'pobreza_laboral_persona'], 'quality_indicator_dictionary': quality_dict, 'quality_indicators': quality_indicators, 'methodological_observations': ['Se filtran residentes con entrevista completa (r_def=0) y condición de residencia en el hogar (c_res=1 o 3).', 'Se identifica a las personas ocupadas y se distingue a trabajadores sin pago.', 'Se prioriza el ingreso exacto reportado en p6b2; en su ausencia se imputa con p6c y el salario mínimo del periodo.', 'Los hogares con ingresos no recuperables se marcan y sus integrantes no participan en el cálculo del indicador.', 'Se calcula el ingreso laboral del hogar y el ingreso per cápita utilizando el número de integrantes del hogar.', 'Se asigna ámbito (rural/urbano) y la LPEI correspondiente, y se determina la elegibilidad y la pobreza laboral de cada persona.'] + metadata_warnings}
            metadata_file_path = output_path.parent / f'ENOE_{periodo}_analisis_pl_metadata_quality.json'
            _write_json_metadata(metadata_json, metadata_file_path)
            metadata_gen_end = datetime.now(timezone.utc)
            step_info = {'step_number': None, 'step_name': 'build_analysis_dataset', 'function_name': 'build_analysis_dataset', 'transformation_description': 'Construcción del dataset analítico de pobreza laboral con filtrado de residentes, imputación de ingresos y cálculo de pobreza.', 'status': 'ok', 'started_at_utc': analysis_step_start_time.isoformat(), 'finished_at_utc': metadata_gen_end.isoformat(), 'duration_seconds': (metadata_gen_end - analysis_step_start_time).total_seconds(), 'input': {'objects': [{'object_name': Path(path_merged).name, 'object_type': 'parquet', 'path': str(path_merged), 'role': 'dataset_unido_sdem_coe2'}]}, 'parameters': {'compression': compression, 'overwrite': overwrite, 'strict': strict}, 'output': {'object_name': output_path.name, 'object_type': 'parquet', 'path': str(output_path), 'role': 'dataset_analitico_pl'}, 'execution_summary': metrics, 'warnings': [], 'errors': []}
            try:
                _append_period_paradata_step(periodo, step_info, path_paradata)
            except Exception as paradata_error:
                warnings.warn(f'No fue posible registrar el paradato analítico de {periodo}: {paradata_error}', RuntimeWarning, stacklevel=2)
            metadata = metadata_json
            paradata = step_info
        except Exception:
            pass
    result = {'periodo': periodo, 'path_output': output_path, 'filas_salida': output_rows, 'columnas_salida': len(output_columns), 'nombres_columnas_salida': output_columns, 'metricas': metrics, 'metadata': metadata, 'paradata': paradata, 'events': events, 'OK': True}
    return result
