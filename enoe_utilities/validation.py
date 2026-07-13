"""Validaciones estructurales para Parquet ENOE y datasets unidos."""

def validate_required_columns(time_series, cols_coe2, cols_sdem):
    """
    Valida la disponibilidad de columnas en los Parquet de COE2
    y SDEM considerando equivalencias históricas de la ENOE.

    No carga los datos en memoria. Solo consulta el esquema y los
    metadatos de cada archivo Parquet.

    Retorna un DataFrame con:
        - Periodo
        - Tabla
        - Existe_archivo
        - Num_columnas
        - Num_filas
        - Columnas_faltantes
        - Equivalencias_aplicadas
        - Mapa_lectura
        - OK
    """
    from pandas import DataFrame
    import pyarrow.parquet as pq
    equivalencias = {'cve_ent': ['cve_ent', 'ent'], 't_loc_tri': ['t_loc_tri', 't_loc'], 'fac_tri': ['fac_tri', 'fac']}

    def normalize_column_name(column):
        return str(column).replace('\ufeff', '').replace('ï»¿', '').strip().lower()

    def resolve_column(canonical_column, normalized_to_raw):
        candidates = equivalencias.get(canonical_column, [canonical_column])
        if canonical_column not in candidates:
            candidates = [canonical_column, *candidates]
        for candidate in candidates:
            candidate_norm = normalize_column_name(candidate)
            if candidate_norm in normalized_to_raw:
                return normalized_to_raw[candidate_norm]
        return None
    records = []
    for periodo, paths in sorted(time_series.items()):
        path_coe2, path_sdem = paths
        checks = [('COE2', path_coe2, cols_coe2), ('SDEM', path_sdem, cols_sdem)]
        for table_name, path_table, required_columns in checks:
            if path_table is None:
                records.append({'Periodo': periodo, 'Tabla': table_name, 'Existe_archivo': False, 'Num_columnas': None, 'Num_filas': None, 'Columnas_faltantes': required_columns.copy(), 'Equivalencias_aplicadas': {}, 'Mapa_lectura': {}, 'OK': False})
                continue
            parquet_file = pq.ParquetFile(path_table)
            raw_columns = parquet_file.schema.names
            normalized_to_raw = {}
            for raw_column in raw_columns:
                normalized_column = normalize_column_name(raw_column)
                if normalized_column in normalized_to_raw:
                    raise ValueError(f'[{periodo} - {table_name}] Dos columnas producen el mismo nombre normalizado: {raw_column}')
                normalized_to_raw[normalized_column] = raw_column
            missing_columns = []
            applied_equivalences = {}
            read_map = {}
            for column in required_columns:
                canonical_column = normalize_column_name(column)
                raw_column = resolve_column(canonical_column, normalized_to_raw)
                if raw_column is None:
                    missing_columns.append(canonical_column)
                    continue
                read_map[canonical_column] = raw_column
                raw_normalized = normalize_column_name(raw_column)
                if raw_normalized != canonical_column or raw_column != canonical_column:
                    applied_equivalences[canonical_column] = raw_column
            records.append({'Periodo': periodo, 'Tabla': table_name, 'Existe_archivo': True, 'Num_columnas': len(raw_columns), 'Num_filas': parquet_file.metadata.num_rows, 'Columnas_faltantes': missing_columns, 'Equivalencias_aplicadas': applied_equivalences, 'Mapa_lectura': read_map, 'OK': len(missing_columns) == 0})
    return DataFrame(records)

def validate_merged_period(periodo, path_merged, llave_persona, llave_hogar=None, required_columns=None, coe2_columns=None, consistency_columns=None, min_occupied_match_rate=None, strict=False):
    """
    Valida la calidad estructural y analítica de un Parquet unido
    SDEM-COE2 correspondiente a un solo trimestre de la ENOE.

    La función consulta directamente el archivo Parquet mediante
    DuckDB y PyArrow. No carga el conjunto completo en memoria y
    no modifica el archivo.

    Parámetros
    ----------
    periodo : str
        Periodo esperado con formato AAAATX, por ejemplo "2007T4".

    path_merged : str o pathlib.Path
        Ruta del Parquet generado por merge_period_parquet().

    llave_persona : list
        Columnas canónicas que identifican de forma única a una
        persona dentro del trimestre.

    llave_hogar : list, opcional
        Columnas que identifican al hogar.

        Si es None, se deriva de llave_persona eliminando n_ren.

    required_columns : list, opcional
        Columnas que obligatoriamente debe contener el Parquet.

        Si es None, se construye una lista mínima usando:
            - llave de persona;
            - variables de periodo;
            - variables laborales;
            - variables de ingreso;
            - factor de expansión.

    coe2_columns : list, opcional
        Columnas procedentes de COE2 que se usarán para revisar
        la consistencia de cruce_coe2.

    consistency_columns : list, opcional
        Variables que deben ser constantes dentro del hogar.

        Por defecto:
            cve_ent, t_loc_tri y fac_tri.

    min_occupied_match_rate : float, opcional
        Tasa mínima aceptable de cruce de personas ocupadas con
        COE2. Si es None, solo se informa la tasa.

    strict : bool, default=False
        Si es True, lanza una excepción cuando encuentra errores
        críticos. Si es False, retorna todas las métricas para
        inspección.

    Retorna
    -------
    dict
        Métricas, errores, advertencias y resultado general.
    """
    from pathlib import Path
    import gc
    import re
    import duckdb
    import pyarrow.parquet as pq

    def quote_identifier(identifier):
        """
        Protege nombres de columnas para utilizarlos en SQL.
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

    def normalize_name(column):
        """
        Normaliza nombres canónicos para realizar comparaciones.
        """
        return str(column).strip().lower()

    def unique_ordered(values):
        """
        Elimina valores repetidos conservando el orden.
        """
        return list(dict.fromkeys(values))
    match = re.fullmatch('(\\d{4})T([1-4])', str(periodo).upper())
    if not match:
        raise ValueError(f'Periodo inválido: {periodo}. Se esperaba el formato AAAATX.')
    anio, trimestre = match.groups()
    anio = int(anio)
    trimestre = int(trimestre)
    periodo = f'{anio}T{trimestre}'
    path_merged = Path(path_merged)
    if not path_merged.exists():
        raise FileNotFoundError(f'No existe el Parquet unido:\n{path_merged}')
    if path_merged.suffix.lower() != '.parquet':
        raise ValueError(f'El archivo no tiene extensión .parquet:\n{path_merged}')
    llave_persona = unique_ordered([normalize_name(column) for column in llave_persona])
    if llave_hogar is None:
        llave_hogar = [column for column in llave_persona if column != 'n_ren']
    else:
        llave_hogar = unique_ordered([normalize_name(column) for column in llave_hogar])
    if coe2_columns is None:
        coe2_columns = ['p6_9', 'p6b1', 'p6b2', 'p6c']
    coe2_columns = unique_ordered([normalize_name(column) for column in coe2_columns])
    if consistency_columns is None:
        consistency_columns = ['cve_ent', 't_loc_tri', 'fac_tri']
    consistency_columns = unique_ordered([normalize_name(column) for column in consistency_columns])
    if required_columns is None:
        required_columns = unique_ordered([*llave_persona, 'periodo', 'anio', 'trimestre', 'cruce_coe2', 'r_def', 'c_res', 'clase2', 'pos_ocu', 'cve_ent', 't_loc_tri', 'fac_tri', *coe2_columns])
    else:
        required_columns = unique_ordered([normalize_name(column) for column in required_columns])
    parquet_file = pq.ParquetFile(path_merged)
    raw_columns = list(parquet_file.schema.names)
    metadata_rows = int(parquet_file.metadata.num_rows)
    num_row_groups = int(parquet_file.metadata.num_row_groups)
    del parquet_file
    gc.collect()
    normalized_columns = [normalize_name(column) for column in raw_columns]
    available_columns = set(normalized_columns)
    missing_columns = [column for column in required_columns if column not in available_columns]
    historical_names = {'ent', 't_loc', 'fac', 'ï»¿r_def', 'ï»¿cd_a', '\ufeffr_def', '\ufeffcd_a'}
    historical_columns_present = [raw_column for raw_column in raw_columns if normalize_name(raw_column) in historical_names]
    duplicate_column_names = [column for index, column in enumerate(normalized_columns) if column in normalized_columns[:index]]
    structural_errors = []
    warnings_list = []
    if metadata_rows == 0:
        structural_errors.append('El Parquet no contiene registros.')
    if missing_columns:
        structural_errors.append(f'Columnas requeridas ausentes: {missing_columns}')
    if historical_columns_present:
        structural_errors.append(f'El Parquet conserva nombres históricos o encabezados con BOM: {historical_columns_present}')
    if duplicate_column_names:
        structural_errors.append(f'Existen columnas duplicadas después de normalizar nombres: {duplicate_column_names}')
    if structural_errors:
        result = {'periodo': periodo, 'path_merged': path_merged, 'filas_metadata': metadata_rows, 'num_row_groups': num_row_groups, 'num_columns': len(raw_columns), 'columnas_disponibles': raw_columns, 'columnas_faltantes': missing_columns, 'columnas_historicas_presentes': historical_columns_present, 'columnas_duplicadas': duplicate_column_names, 'errores': structural_errors, 'advertencias': warnings_list, 'OK': False}
        if strict:
            raise ValueError(f'[{periodo}] La validación estructural falló:\n' + '\n'.join((f'- {error}' for error in structural_errors)))
        return result
    con = duckdb.connect(database=':memory:')
    try:
        con.execute(f'\n            CREATE TEMP VIEW merged_data AS\n\n            SELECT *\n            FROM read_parquet(\n                {sql_path(path_merged)}\n            )\n            ')
        query_rows = int(con.execute('\n                SELECT COUNT(*)\n                FROM merged_data\n                ').fetchone()[0])
        if query_rows != metadata_rows:
            structural_errors.append(f'Las filas de metadatos ({metadata_rows:,}) no coinciden con las filas consultadas ({query_rows:,}).')
        null_key_condition = ' OR '.join([f"{quote_identifier(column)} IS NULL OR NULLIF(TRIM(CAST({quote_identifier(column)} AS VARCHAR)), '') IS NULL" for column in llave_persona])
        null_person_keys = int(con.execute(f'\n                SELECT COUNT(*)\n                FROM merged_data\n                WHERE {null_key_condition}\n                ').fetchone()[0])
        person_group_keys = ', '.join([quote_identifier(column) for column in llave_persona])
        duplicate_person_keys = int(con.execute(f'\n                SELECT COALESCE(\n                    SUM(n - 1),\n                    0\n                )\n\n                FROM (\n                    SELECT\n                        COUNT(*) AS n\n\n                    FROM merged_data\n\n                    GROUP BY\n                        {person_group_keys}\n\n                    HAVING COUNT(*) > 1\n                )\n                ').fetchone()[0])
        unique_persons = int(con.execute(f'\n                SELECT COUNT(*)\n\n                FROM (\n                    SELECT\n                        1\n\n                    FROM merged_data\n\n                    GROUP BY\n                        {person_group_keys}\n                )\n                ').fetchone()[0])
        inconsistent_period_rows = int(con.execute(f"\n                SELECT COUNT(*)\n\n                FROM merged_data\n\n                WHERE\n                    CAST(periodo AS VARCHAR) <> '{periodo}'\n\n                    OR TRY_CAST(anio AS INTEGER) <> {anio}\n\n                    OR TRY_CAST(trimestre AS INTEGER)\n                       <> {trimestre}\n\n                    OR periodo IS NULL\n                    OR anio IS NULL\n                    OR trimestre IS NULL\n                ").fetchone()[0])
        invalid_match_labels = int(con.execute("\n                SELECT COUNT(*)\n\n                FROM merged_data\n\n                WHERE cruce_coe2 IS NULL\n                   OR CAST(cruce_coe2 AS VARCHAR)\n                      NOT IN ('both', 'left_only')\n                ").fetchone()[0])
        match_counts = con.execute("\n            SELECT\n                SUM(\n                    CASE\n                        WHEN CAST(cruce_coe2 AS VARCHAR)\n                             = 'both'\n                        THEN 1\n                        ELSE 0\n                    END\n                ) AS registros_cruzados,\n\n                SUM(\n                    CASE\n                        WHEN CAST(cruce_coe2 AS VARCHAR)\n                             = 'left_only'\n                        THEN 1\n                        ELSE 0\n                    END\n                ) AS registros_sin_coe2\n\n            FROM merged_data\n            ").fetchone()
        matched_records = int(match_counts[0] or 0)
        unmatched_records = int(match_counts[1] or 0)
        global_match_rate = matched_records / query_rows * 100 if query_rows > 0 else None
        occupied_metrics = con.execute("\n            SELECT\n                SUM(\n                    CASE\n                        WHEN TRY_CAST(clase2 AS DOUBLE) = 1\n                        THEN 1\n                        ELSE 0\n                    END\n                ) AS ocupados,\n\n                SUM(\n                    CASE\n                        WHEN TRY_CAST(clase2 AS DOUBLE) = 1\n                         AND CAST(cruce_coe2 AS VARCHAR) = 'both'\n                        THEN 1\n                        ELSE 0\n                    END\n                ) AS ocupados_con_coe2,\n\n                SUM(\n                    CASE\n                        WHEN TRY_CAST(clase2 AS DOUBLE) = 1\n                         AND CAST(cruce_coe2 AS VARCHAR)\n                             = 'left_only'\n                        THEN 1\n                        ELSE 0\n                    END\n                ) AS ocupados_sin_coe2,\n\n                SUM(\n                    CASE\n                        WHEN TRY_CAST(clase2 AS DOUBLE) = 1\n                         AND TRY_CAST(pos_ocu AS DOUBLE) = 4\n                        THEN 1\n                        ELSE 0\n                    END\n                ) AS ocupados_sin_pago_sdem\n\n            FROM merged_data\n            ").fetchone()
        occupied = int(occupied_metrics[0] or 0)
        occupied_with_coe2 = int(occupied_metrics[1] or 0)
        occupied_without_coe2 = int(occupied_metrics[2] or 0)
        unpaid_occupied_sdem = int(occupied_metrics[3] or 0)
        occupied_match_rate = occupied_with_coe2 / occupied * 100 if occupied > 0 else None
        if occupied_without_coe2 > 0:
            warnings_list.append(f'Se encontraron {occupied_without_coe2:,} personas ocupadas sin correspondencia en COE2.')
        if min_occupied_match_rate is not None and occupied_match_rate is not None and (occupied_match_rate < min_occupied_match_rate):
            structural_errors.append(f'La tasa de cruce de ocupados ({occupied_match_rate:.2f}%) es menor al mínimo requerido ({min_occupied_match_rate:.2f}%).')
        available_coe2_columns = [column for column in coe2_columns if column in available_columns]
        left_only_with_coe2_data = 0
        if available_coe2_columns:
            coe2_data_condition = ' OR '.join([f"NULLIF(TRIM(CAST({quote_identifier(column)} AS VARCHAR)), '') IS NOT NULL" for column in available_coe2_columns])
            left_only_with_coe2_data = int(con.execute(f"\n                    SELECT COUNT(*)\n\n                    FROM merged_data\n\n                    WHERE CAST(cruce_coe2 AS VARCHAR)\n                          = 'left_only'\n\n                      AND (\n                          {coe2_data_condition}\n                      )\n                    ").fetchone()[0])
        invalid_expansion_factors = int(con.execute('\n                SELECT COUNT(*)\n\n                FROM merged_data\n\n                WHERE TRY_CAST(fac_tri AS DOUBLE) IS NULL\n                   OR TRY_CAST(fac_tri AS DOUBLE) <= 0\n                ').fetchone()[0])
        household_group_keys = ', '.join([quote_identifier(column) for column in llave_hogar])
        unique_households = int(con.execute(f'\n                SELECT COUNT(*)\n\n                FROM (\n                    SELECT\n                        1\n\n                    FROM merged_data\n\n                    GROUP BY\n                        {household_group_keys}\n                )\n                ').fetchone()[0])
        available_consistency_columns = [column for column in consistency_columns if column in available_columns]
        household_inconsistency_conditions = []
        for column in available_consistency_columns:
            household_inconsistency_conditions.append(f"COUNT(DISTINCT COALESCE(CAST({quote_identifier(column)} AS VARCHAR), '__NULL__')) > 1")
        inconsistent_households = 0
        if household_inconsistency_conditions:
            inconsistency_condition = ' OR '.join(household_inconsistency_conditions)
            inconsistent_households = int(con.execute(f'\n                    SELECT COUNT(*)\n\n                    FROM (\n                        SELECT\n                            {household_group_keys}\n\n                        FROM merged_data\n\n                        GROUP BY\n                            {household_group_keys}\n\n                        HAVING\n                            {inconsistency_condition}\n                    )\n                    ').fetchone()[0])
        if null_person_keys > 0:
            structural_errors.append(f'Hay {null_person_keys:,} registros con llave de persona incompleta.')
        if duplicate_person_keys > 0:
            structural_errors.append(f'Hay {duplicate_person_keys:,} duplicados excedentes en la llave de persona.')
        if unique_persons != query_rows:
            structural_errors.append(f'El número de personas únicas ({unique_persons:,}) no coincide con el número de filas ({query_rows:,}).')
        if inconsistent_period_rows > 0:
            structural_errors.append(f'Hay {inconsistent_period_rows:,} registros con año, trimestre o periodo inconsistente.')
        if invalid_match_labels > 0:
            structural_errors.append(f'Hay {invalid_match_labels:,} valores inválidos en cruce_coe2.')
        if matched_records + unmatched_records != query_rows:
            structural_errors.append('Los conteos both y left_only no suman el total de filas.')
        if left_only_with_coe2_data > 0:
            structural_errors.append(f'Hay {left_only_with_coe2_data:,} registros left_only que contienen datos de COE2.')
        if invalid_expansion_factors > 0:
            structural_errors.append(f'Hay {invalid_expansion_factors:,} factores de expansión faltantes o no positivos.')
        if inconsistent_households > 0:
            structural_errors.append(f'Hay {inconsistent_households:,} hogares con inconsistencias en {available_consistency_columns}.')
    finally:
        con.close()
        gc.collect()
    ok = len(structural_errors) == 0
    result = {'periodo': periodo, 'anio': anio, 'trimestre': trimestre, 'path_merged': path_merged, 'filas_metadata': metadata_rows, 'filas_consultadas': query_rows, 'num_row_groups': num_row_groups, 'num_columns': len(raw_columns), 'columnas_disponibles': raw_columns, 'columnas_faltantes': missing_columns, 'columnas_historicas_presentes': historical_columns_present, 'llave_persona': llave_persona, 'llave_hogar': llave_hogar, 'personas_unicas': unique_persons, 'nulos_llave_persona': null_person_keys, 'duplicados_llave_persona': duplicate_person_keys, 'hogares_unicos': unique_households, 'hogares_inconsistentes': inconsistent_households, 'registros_periodo_inconsistente': inconsistent_period_rows, 'registros_cruzados': matched_records, 'registros_sin_coe2': unmatched_records, 'tasa_cruce_global': global_match_rate, 'ocupados': occupied, 'ocupados_con_coe2': occupied_with_coe2, 'ocupados_sin_coe2': occupied_without_coe2, 'ocupados_sin_pago_sdem': unpaid_occupied_sdem, 'tasa_cruce_ocupados': occupied_match_rate, 'left_only_con_datos_coe2': left_only_with_coe2_data, 'factores_expansion_invalidos': invalid_expansion_factors, 'errores': structural_errors, 'advertencias': warnings_list, 'OK': ok}
    if strict and (not ok):
        raise ValueError(f'[{periodo}] La validación del Parquet unido falló:\n' + '\n'.join((f'- {error}' for error in structural_errors)))
    return result
