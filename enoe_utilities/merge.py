"""Resolución de esquemas históricos y unión trimestral SDEM–COE2."""
from datetime import datetime, timezone
from .metadata import _utc_now_iso, _safe_file_size_mb, _safe_rate_pct, _sql_identifier, _read_parquet_schema_info, _write_json_metadata, _append_period_paradata_step, _build_column_dictionary, _build_quality_indicator_dictionary

def resolve_parquet_columns(path_parquet, required_columns, equivalences=None, strict=True):
    """
    Resuelve los nombres físicos de las columnas de un archivo
    Parquet a partir de un conjunto de nombres canónicos.

    La función consulta únicamente el esquema y los metadatos del
    Parquet. No carga las filas en memoria y no modifica el archivo.

    Parámetros
    ----------
    path_parquet : str o pathlib.Path
        Ruta del archivo Parquet que se desea inspeccionar.

    required_columns : list
        Lista de nombres canónicos requeridos por el pipeline.
        Ejemplo:
            [
                "cve_ent",
                "t_loc_tri",
                "fac_tri",
                "n_ren"
            ]

    equivalences : dict, opcional
        Diccionario con las equivalencias históricas.

        La clave representa el nombre canónico y los valores
        contienen los nombres alternativos que pueden aparecer
        físicamente en los Parquet.

        Ejemplo:
            {
                "cve_ent": ["cve_ent", "ent"],
                "t_loc_tri": ["t_loc_tri", "t_loc"],
                "fac_tri": ["fac_tri", "fac"]
            }

    strict : bool, default=True
        Si es True, lanza un error cuando falta al menos una
        columna requerida.

        Si es False, retorna el resultado con OK=False y registra
        las columnas faltantes.

    Retorna
    -------
    dict con:
        - path
        - num_rows
        - num_columns
        - available_columns
        - required_columns
        - read_columns
        - read_map
        - rename_map
        - applied_equivalences
        - missing_columns
        - OK
    """
    from pathlib import Path
    import pyarrow.parquet as pq
    if equivalences is None:
        equivalences = {'cve_ent': ['cve_ent', 'ent'], 't_loc_tri': ['t_loc_tri', 't_loc'], 'fac_tri': ['fac_tri', 'fac']}

    def normalize_column_name(column):
        """
        Normaliza un nombre de columna para facilitar la
        comparación entre diferentes periodos de la ENOE.
        """
        return str(column).replace('\ufeff', '').replace('ï»¿', '').strip().lower()
    path_parquet = Path(path_parquet)
    if not path_parquet.exists():
        raise FileNotFoundError(f'No existe el archivo Parquet: {path_parquet}')
    if path_parquet.suffix.lower() != '.parquet':
        raise ValueError(f'El archivo no tiene extensión .parquet: {path_parquet}')
    parquet_file = pq.ParquetFile(path_parquet)
    raw_columns = list(parquet_file.schema.names)
    num_rows = parquet_file.metadata.num_rows
    normalized_to_raw = {}
    for raw_column in raw_columns:
        normalized_column = normalize_column_name(raw_column)
        if normalized_column in normalized_to_raw:
            previous_column = normalized_to_raw[normalized_column]
            raise ValueError(f'Dos columnas del Parquet generan el mismo nombre normalizado:\n  - {previous_column}\n  - {raw_column}\nNombre normalizado: {normalized_column}')
        normalized_to_raw[normalized_column] = raw_column
    normalized_equivalences = {}
    for canonical_column, alternatives in equivalences.items():
        canonical_norm = normalize_column_name(canonical_column)
        alternatives_norm = [normalize_column_name(alternative) for alternative in alternatives]
        candidates = [canonical_norm, *alternatives_norm]
        normalized_equivalences[canonical_norm] = list(dict.fromkeys(candidates))
    read_map = {}
    rename_map = {}
    applied_equivalences = {}
    missing_columns = []
    required_columns_norm = [normalize_column_name(column) for column in required_columns]
    required_columns_norm = list(dict.fromkeys(required_columns_norm))
    for canonical_column in required_columns_norm:
        candidates = normalized_equivalences.get(canonical_column, [canonical_column])
        raw_column_found = None
        normalized_column_found = None
        for candidate in candidates:
            if candidate in normalized_to_raw:
                raw_column_found = normalized_to_raw[candidate]
                normalized_column_found = candidate
                break
        if raw_column_found is None:
            missing_columns.append(canonical_column)
            continue
        read_map[canonical_column] = raw_column_found
        rename_map[raw_column_found] = canonical_column
        if normalized_column_found != canonical_column or raw_column_found != canonical_column:
            applied_equivalences[canonical_column] = raw_column_found
    read_columns = [read_map[column] for column in required_columns_norm if column in read_map]
    ok = len(missing_columns) == 0
    if strict and (not ok):
        raise KeyError(f'El archivo {path_parquet.name} no contiene todas las columnas requeridas.\nColumnas faltantes: {missing_columns}\nColumnas disponibles: {raw_columns}')
    result = {'path': path_parquet, 'num_rows': num_rows, 'num_columns': len(raw_columns), 'available_columns': raw_columns, 'required_columns': required_columns_norm, 'read_columns': read_columns, 'read_map': read_map, 'rename_map': rename_map, 'applied_equivalences': applied_equivalences, 'missing_columns': missing_columns, 'OK': ok}
    return result

def merge_period_parquet(periodo, path_coe2, path_sdem, cols_coe2, cols_sdem, llave_persona, path_out='../Merge_de_parquet', equivalences=None, overwrite=False, compression='zstd', strict_keys=True, *, optional_cols_coe2=None, optional_cols_sdem=None, generate_metadata=False, path_paradata='../Paradata'):
    """
    Realiza el LEFT JOIN entre SDEM y COE2 para un trimestre
    de la ENOE y guarda el resultado directamente en Parquet.

    SDEM se utiliza como tabla madre. Por lo tanto, el número
    de filas del resultado debe ser igual al número de filas
    de SDEM.

    Parámetros
    ----------
    periodo : str
        Periodo con formato AAAATX, por ejemplo "2007T4".

    path_coe2 : str o pathlib.Path
        Ruta del archivo Parquet de COE2.

    path_sdem : str o pathlib.Path
        Ruta del archivo Parquet de SDEM.

    cols_coe2 : list
        Columnas canónicas requeridas de COE2.

    cols_sdem : list
        Columnas canónicas requeridas de SDEM.

    llave_persona : list
        Columnas que identifican a la persona dentro del trimestre.

    path_out : str o pathlib.Path
        Carpeta raíz para guardar los Parquet unidos.

    equivalences : dict, opcional
        Equivalencias históricas entre nombres de columnas.

    overwrite : bool, default=False
        Si es True, reemplaza el archivo existente.

    compression : str, default="zstd"
        Método de compresión del Parquet.

    strict_keys : bool, default=True
        Si es True, detiene el proceso cuando las llaves contienen
        nulos o duplicados.

    Retorna
    -------
    dict
        Ruta del archivo generado y métricas del merge.
    """
    metadata_start_time = datetime.now(timezone.utc)
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
        Convierte una ruta en un literal SQL compatible con DuckDB.
        """
        path = Path(path).resolve().as_posix()
        path = path.replace("'", "''")
        return f"'{path}'"

    def unique_ordered(values):
        """
        Elimina valores duplicados conservando el orden.
        """
        return list(dict.fromkeys(values))

    def build_select_expression(canonical_column, physical_column, key_columns):
        """Construye una expresión SQL para leer una columna física."""
        physical_sql = quote_identifier(physical_column)
        canonical_sql = quote_identifier(canonical_column)
        if canonical_column in key_columns:
            return f"NULLIF(TRIM(CAST({physical_sql} AS VARCHAR)), '') AS {canonical_sql}"
        return f'{physical_sql} AS {canonical_sql}'

    def build_missing_optional_expression(canonical_column):
        """Conserva una ausencia estructural como columna nula."""
        return f'CAST(NULL AS VARCHAR) AS {quote_identifier(canonical_column)}'

    def safe_remove(path):
        """
        Intenta eliminar un archivo sin ocultar errores distintos
        de un bloqueo temporal de Windows.
        """
        path = Path(path)
        if not path.exists():
            return
        gc.collect()
        try:
            path.unlink()
        except PermissionError as error:
            raise PermissionError(f'No se pudo eliminar el archivo porque está siendo utilizado por otro proceso:\n{path}') from error
    match = re.fullmatch('(\\d{4})T([1-4])', str(periodo).upper())
    if not match:
        raise ValueError(f'Periodo inválido: {periodo}. El formato esperado es AAAATX, por ejemplo 2026T1.')
    anio, trimestre = match.groups()
    anio = int(anio)
    trimestre = int(trimestre)
    periodo = f'{anio}T{trimestre}'
    path_coe2 = Path(path_coe2)
    path_sdem = Path(path_sdem)
    path_out = Path(path_out)
    if not path_coe2.exists():
        raise FileNotFoundError(f'No existe el Parquet COE2: {path_coe2}')
    if not path_sdem.exists():
        raise FileNotFoundError(f'No existe el Parquet SDEM: {path_sdem}')
    llave_persona = unique_ordered([str(column).strip().lower() for column in llave_persona])
    cols_sdem = unique_ordered([str(column).strip().lower() for column in cols_sdem])
    cols_coe2 = unique_ordered([str(column).strip().lower() for column in cols_coe2])
    optional_cols_sdem = unique_ordered([str(column).strip().lower() for column in (optional_cols_sdem or [])])
    optional_cols_coe2 = unique_ordered([str(column).strip().lower() for column in (optional_cols_coe2 or [])])
    no_solicitadas_sdem = [column for column in optional_cols_sdem if column not in cols_sdem]
    no_solicitadas_coe2 = [column for column in optional_cols_coe2 if column not in cols_coe2]
    if no_solicitadas_sdem or no_solicitadas_coe2:
        raise KeyError(f'Las columnas opcionales deben estar incluidas en sus listas de selección. SDEM: {no_solicitadas_sdem}; COE2: {no_solicitadas_coe2}')
    faltantes_llave_sdem = [column for column in llave_persona if column not in cols_sdem]
    faltantes_llave_coe2 = [column for column in llave_persona if column not in cols_coe2]
    if faltantes_llave_sdem:
        raise KeyError(f'La llave no está completa en cols_sdem: {faltantes_llave_sdem}')
    if faltantes_llave_coe2:
        raise KeyError(f'La llave no está completa en cols_coe2: {faltantes_llave_coe2}')
    required_sdem = [column for column in cols_sdem if column not in optional_cols_sdem]
    required_coe2 = [column for column in cols_coe2 if column not in optional_cols_coe2]
    schema_sdem = resolve_parquet_columns(path_parquet=path_sdem, required_columns=required_sdem, equivalences=equivalences, strict=True)
    schema_coe2 = resolve_parquet_columns(path_parquet=path_coe2, required_columns=required_coe2, equivalences=equivalences, strict=True)
    optional_schema_sdem = resolve_parquet_columns(path_parquet=path_sdem, required_columns=optional_cols_sdem, equivalences=equivalences, strict=False)
    optional_schema_coe2 = resolve_parquet_columns(path_parquet=path_coe2, required_columns=optional_cols_coe2, equivalences=equivalences, strict=False)
    for schema, optional_schema in ((schema_sdem, optional_schema_sdem), (schema_coe2, optional_schema_coe2)):
        schema['read_map'].update(optional_schema['read_map'])
        schema['rename_map'].update(optional_schema['rename_map'])
        schema['applied_equivalences'].update(optional_schema['applied_equivalences'])
    select_sdem = []
    for canonical_column in cols_sdem:
        physical_column = schema_sdem['read_map'].get(canonical_column)
        expression = build_missing_optional_expression(canonical_column) if physical_column is None else build_select_expression(canonical_column=canonical_column, physical_column=physical_column, key_columns=llave_persona)
        select_sdem.append(expression)
    select_coe2 = []
    for canonical_column in cols_coe2:
        physical_column = schema_coe2['read_map'].get(canonical_column)
        expression = build_missing_optional_expression(canonical_column) if physical_column is None else build_select_expression(canonical_column=canonical_column, physical_column=physical_column, key_columns=llave_persona)
        select_coe2.append(expression)
    select_sdem_sql = ',\n                '.join(select_sdem)
    select_coe2_sql = ',\n                '.join(select_coe2)
    output_folder = path_out / periodo
    output_folder.mkdir(parents=True, exist_ok=True)
    output_path = output_folder / f'ENOE_{periodo}_unida.parquet'
    temp_path = output_folder / f'ENOE_{periodo}_unida.tmp.parquet'
    if output_path.exists() and (not overwrite):
        raise FileExistsError(f'El archivo ya existe: {output_path}\nUtiliza overwrite=True para reemplazarlo.')
    if temp_path.exists():
        safe_remove(temp_path)
    filas_sdem = None
    filas_coe2 = None
    nulos_llave_sdem = None
    nulos_llave_coe2 = None
    duplicados_sdem = None
    duplicados_coe2 = None
    registros_cruzados = None
    registros_sin_coe2 = None
    filas_resultado = None
    columnas_resultado = None
    con = duckdb.connect(database=':memory:')
    try:
        con.execute(f'\n            CREATE TEMP VIEW sdem_canonica AS\n\n            SELECT\n                {select_sdem_sql}\n\n            FROM read_parquet(\n                {sql_path(path_sdem)}\n            )\n            ')
        con.execute(f'\n            CREATE TEMP VIEW coe2_canonica AS\n\n            SELECT\n                {select_coe2_sql}\n\n            FROM read_parquet(\n                {sql_path(path_coe2)}\n            )\n            ')
        filas_sdem = con.execute('\n            SELECT COUNT(*)\n            FROM sdem_canonica\n            ').fetchone()[0]
        filas_coe2 = con.execute('\n            SELECT COUNT(*)\n            FROM coe2_canonica\n            ').fetchone()[0]
        condicion_nulos = ' OR '.join([f'{quote_identifier(column)} IS NULL' for column in llave_persona])
        nulos_llave_sdem = con.execute(f'\n            SELECT COUNT(*)\n            FROM sdem_canonica\n            WHERE {condicion_nulos}\n            ').fetchone()[0]
        nulos_llave_coe2 = con.execute(f'\n            SELECT COUNT(*)\n            FROM coe2_canonica\n            WHERE {condicion_nulos}\n            ').fetchone()[0]
        group_keys = ', '.join([quote_identifier(column) for column in llave_persona])
        duplicados_sdem = con.execute(f'\n            SELECT COALESCE(\n                SUM(n - 1),\n                0\n            )\n\n            FROM (\n                SELECT\n                    COUNT(*) AS n\n\n                FROM sdem_canonica\n\n                GROUP BY\n                    {group_keys}\n\n                HAVING COUNT(*) > 1\n            )\n            ').fetchone()[0]
        duplicados_coe2 = con.execute(f'\n            SELECT COALESCE(\n                SUM(n - 1),\n                0\n            )\n\n            FROM (\n                SELECT\n                    COUNT(*) AS n\n\n                FROM coe2_canonica\n\n                GROUP BY\n                    {group_keys}\n\n                HAVING COUNT(*) > 1\n            )\n            ').fetchone()[0]
        problemas_llaves = {'nulos_llave_sdem': int(nulos_llave_sdem), 'nulos_llave_coe2': int(nulos_llave_coe2), 'duplicados_sdem': int(duplicados_sdem), 'duplicados_coe2': int(duplicados_coe2)}
        if strict_keys and any((value > 0 for value in problemas_llaves.values())):
            raise ValueError(f'[{periodo}] La llave de persona no cumple las validaciones:\n{problemas_llaves}')
        join_condition = '\n                AND '.join([f's.{quote_identifier(column)} = c.{quote_identifier(column)}' for column in llave_persona])
        cols_coe2_adicionales = [column for column in cols_coe2 if column not in llave_persona and column not in cols_sdem]
        output_columns = []
        for column in cols_sdem:
            output_columns.append(f's.{quote_identifier(column)}')
        for column in cols_coe2_adicionales:
            output_columns.append(f'c.{quote_identifier(column)}')
        output_columns.extend([f"'{periodo}' AS periodo", f'{anio} AS anio', f'{trimestre} AS trimestre', f"CASE WHEN c.{quote_identifier(llave_persona[0])} IS NOT NULL THEN 'both' ELSE 'left_only' END AS cruce_coe2"])
        output_columns_sql = ',\n                    '.join(output_columns)
        cobertura = con.execute(f'\n            SELECT\n                SUM(\n                    CASE\n                        WHEN c.{quote_identifier(llave_persona[0])}\n                             IS NOT NULL\n                        THEN 1\n                        ELSE 0\n                    END\n                ) AS registros_cruzados,\n\n                SUM(\n                    CASE\n                        WHEN c.{quote_identifier(llave_persona[0])}\n                             IS NULL\n                        THEN 1\n                        ELSE 0\n                    END\n                ) AS registros_sin_coe2\n\n            FROM sdem_canonica AS s\n\n            LEFT JOIN coe2_canonica AS c\n                ON {join_condition}\n            ').fetchone()
        registros_cruzados = int(cobertura[0] or 0)
        registros_sin_coe2 = int(cobertura[1] or 0)
        compression_sql = str(compression).strip().upper()
        con.execute(f"\n            COPY (\n\n                SELECT\n                    {output_columns_sql}\n\n                FROM sdem_canonica AS s\n\n                LEFT JOIN coe2_canonica AS c\n                    ON {join_condition}\n\n            )\n\n            TO {sql_path(temp_path)}\n\n            (\n                FORMAT PARQUET,\n                COMPRESSION '{compression_sql}',\n                ROW_GROUP_SIZE 100000\n            )\n            ")
    except Exception:
        con.close()
        con = None
        gc.collect()
        if temp_path.exists():
            try:
                temp_path.unlink()
            except PermissionError:
                print(f'No se pudo borrar el archivo temporal porque continúa en uso:\n{temp_path}')
        raise
    finally:
        if con is not None:
            con.close()
        gc.collect()
    if not temp_path.exists():
        raise RuntimeError(f'No se generó el archivo temporal esperado:\n{temp_path}')
    parquet_result = pq.ParquetFile(temp_path)
    filas_resultado = parquet_result.metadata.num_rows
    columnas_resultado = list(parquet_result.schema.names)
    del parquet_result
    gc.collect()
    if filas_resultado != filas_sdem:
        try:
            temp_path.unlink()
        except PermissionError:
            pass
        raise RuntimeError(f'[{periodo}] El LEFT JOIN modificó el número de filas de SDEM:\nSDEM: {filas_sdem:,}\nResultado: {filas_resultado:,}')
    if output_path.exists():
        safe_remove(output_path)
    try:
        temp_path.replace(output_path)
    except PermissionError as error:
        raise PermissionError(f'No fue posible renombrar el archivo temporal.\nTemporal: {temp_path}\nDestino: {output_path}\nVerifica que ningún programa tenga abierto el archivo.') from error
    tasa_cruce_global = registros_cruzados / filas_sdem * 100 if filas_sdem > 0 else None
    result = {'periodo': periodo, 'anio': anio, 'trimestre': trimestre, 'path_sdem': path_sdem, 'path_coe2': path_coe2, 'path_output': output_path, 'filas_sdem': int(filas_sdem), 'filas_coe2': int(filas_coe2), 'filas_resultado': int(filas_resultado), 'columnas_sdem_seleccionadas': len(cols_sdem), 'columnas_coe2_seleccionadas': len(cols_coe2), 'columnas_resultado': len(columnas_resultado), 'nombres_columnas_resultado': columnas_resultado, 'nulos_llave_sdem': int(nulos_llave_sdem), 'nulos_llave_coe2': int(nulos_llave_coe2), 'duplicados_sdem': int(duplicados_sdem), 'duplicados_coe2': int(duplicados_coe2), 'registros_cruzados': registros_cruzados, 'registros_sin_coe2': registros_sin_coe2, 'tasa_cruce_global': tasa_cruce_global, 'equivalencias_sdem': schema_sdem['applied_equivalences'], 'equivalencias_coe2': schema_coe2['applied_equivalences'], 'OK': True}
    if generate_metadata:
        try:
            metadata_end_time = datetime.now(timezone.utc)
            schema_info = _read_parquet_schema_info(output_path)
            if schema_info is not None:
                total_rows = schema_info['rows']
                column_names = schema_info['column_names']
                column_types = schema_info['column_types']
                row_groups_merge = schema_info['row_groups']
            else:
                total_rows = filas_resultado
                column_names = columnas_resultado
                column_types = [''] * len(columnas_resultado)
                row_groups_merge = None
            file_size_mb = _safe_file_size_mb(output_path)
            column_dict = _build_column_dictionary(column_names, dataset_type='merge')
            for col_name, col_type in zip(column_names, column_types):
                if col_name in column_dict:
                    column_dict[col_name]['type'] = col_type
                else:
                    column_dict[col_name] = {'label': col_name, 'description': '', 'source': None, 'type': col_type, 'unit': None, 'role': None, 'used_in': []}
            expansion_quality = {'missing_fac_tri': None, 'non_positive_fac_tri': None, 'invalid_fac_tri_records': None, 'invalid_fac_tri_rate_pct': None, 'valid_fac_tri_records': None, 'valid_fac_tri_rate_pct': None}
            income_availability = {}
            null_key_rate_pct = None
            duplicate_key_rate_pct = None
            join_rate_pct = None
            left_only_rate_pct = None
            occupied_persons = None
            occupied_with_coe2 = None
            occupied_without_coe2 = None
            occupied_match_rate_pct = None
            unpaid_occupied_sdem = None
            metadata_warnings = []
            try:
                import duckdb
                con = duckdb.connect(database=':memory:')
                tbl_sql = f"SELECT * FROM read_parquet('{output_path.resolve().as_posix()}')"
                total_records = con.execute(f'SELECT COUNT(*) FROM ({tbl_sql})').fetchone()[0]
                if 'fac_tri' in column_names:
                    fac_sql = _sql_identifier('fac_tri')
                    missing_fac = con.execute(f"\n                        SELECT COUNT(*)\n                        FROM ({tbl_sql})\n                        WHERE NULLIF(TRIM(CAST({fac_sql} AS VARCHAR)), '') IS NULL\n                        ").fetchone()[0]
                    non_positive_fac = con.execute(f"\n                        SELECT COUNT(*)\n                        FROM ({tbl_sql})\n                        WHERE NULLIF(TRIM(CAST({fac_sql} AS VARCHAR)), '') IS NOT NULL\n                          AND TRY_CAST({fac_sql} AS DOUBLE) <= 0\n                        ").fetchone()[0]
                    invalid_fac = int(missing_fac or 0) + int(non_positive_fac or 0)
                    valid_fac = int(total_records or 0) - invalid_fac
                    expansion_quality = {'missing_fac_tri': int(missing_fac), 'non_positive_fac_tri': int(non_positive_fac), 'invalid_fac_tri_records': invalid_fac, 'invalid_fac_tri_rate_pct': _safe_rate_pct(invalid_fac, total_records), 'valid_fac_tri_records': valid_fac, 'valid_fac_tri_rate_pct': _safe_rate_pct(valid_fac, total_records)}
                else:
                    metadata_warnings.append('No se encontró fac_tri en el Parquet unido para calcular expansion_factor_quality.')
                for col in ['p6b2', 'p6c', 'p6b1', 'p6_9']:
                    if col in column_names:
                        col_sql = _sql_identifier(col)
                        missing = con.execute(f"\n                            SELECT COUNT(*)\n                            FROM ({tbl_sql})\n                            WHERE NULLIF(TRIM(CAST({col_sql} AS VARCHAR)), '') IS NULL\n                            ").fetchone()[0]
                        non_missing = int(total_records or 0) - int(missing or 0)
                        income_availability[f'{col}_missing_records'] = int(missing)
                        income_availability[f'{col}_non_missing_records'] = non_missing
                        income_availability[f'{col}_missing_rate_pct'] = _safe_rate_pct(missing, total_records)
                        income_availability[f'{col}_non_missing_rate_pct'] = _safe_rate_pct(non_missing, total_records)
                    else:
                        income_availability[f'{col}_missing_records'] = None
                        income_availability[f'{col}_non_missing_records'] = None
                        income_availability[f'{col}_missing_rate_pct'] = None
                        income_availability[f'{col}_non_missing_rate_pct'] = None
                if 'clase2' in column_names:
                    clase_sql = _sql_identifier('clase2')
                    occupied_persons = con.execute(f'\n                        SELECT COUNT(*)\n                        FROM ({tbl_sql})\n                        WHERE TRY_CAST({clase_sql} AS INTEGER) = 1\n                        ').fetchone()[0]
                    if 'cruce_coe2' in column_names:
                        cruce_sql = _sql_identifier('cruce_coe2')
                        occupied_with_coe2 = con.execute(f"\n                            SELECT COUNT(*)\n                            FROM ({tbl_sql})\n                            WHERE TRY_CAST({clase_sql} AS INTEGER) = 1\n                              AND TRIM(CAST({cruce_sql} AS VARCHAR)) = 'both'\n                            ").fetchone()[0]
                        occupied_without_coe2 = con.execute(f"\n                            SELECT COUNT(*)\n                            FROM ({tbl_sql})\n                            WHERE TRY_CAST({clase_sql} AS INTEGER) = 1\n                              AND TRIM(CAST({cruce_sql} AS VARCHAR)) = 'left_only'\n                            ").fetchone()[0]
                        occupied_match_rate_pct = _safe_rate_pct(occupied_with_coe2, occupied_persons)
                    if 'pos_ocu' in column_names:
                        pos_sql = _sql_identifier('pos_ocu')
                        unpaid_occupied_sdem = con.execute(f'\n                            SELECT COUNT(*)\n                            FROM ({tbl_sql})\n                            WHERE TRY_CAST({clase_sql} AS INTEGER) = 1\n                              AND TRY_CAST({pos_sql} AS INTEGER) = 4\n                            ').fetchone()[0]
                join_rate_pct = _safe_rate_pct(registros_cruzados, filas_sdem)
                left_only_rate_pct = _safe_rate_pct(registros_sin_coe2, filas_sdem)
                null_key_rate_pct = _safe_rate_pct((nulos_llave_sdem or 0) + (nulos_llave_coe2 or 0), filas_sdem)
                duplicate_key_rate_pct = _safe_rate_pct((duplicados_sdem or 0) + (duplicados_coe2 or 0), filas_sdem)
                con.close()
            except Exception as metadata_error:
                metadata_warnings.append(f'No fue posible calcular todos los indicadores documentales del merge: {metadata_error}')
            quality_dict = _build_quality_indicator_dictionary(dataset_type='merge')
            quality_indicators = {'join_quality': {'matched_records': registros_cruzados, 'unmatched_records': registros_sin_coe2, 'global_match_rate_pct': join_rate_pct, 'left_only_rate_pct': left_only_rate_pct, 'occupied_persons': int(occupied_persons) if occupied_persons is not None else None, 'occupied_with_coe2': int(occupied_with_coe2) if occupied_with_coe2 is not None else None, 'occupied_without_coe2': int(occupied_without_coe2) if occupied_without_coe2 is not None else None, 'occupied_match_rate_pct': occupied_match_rate_pct, 'unpaid_occupied_sdem': int(unpaid_occupied_sdem) if unpaid_occupied_sdem is not None else None}, 'key_quality': {'null_keys_sdem': nulos_llave_sdem, 'null_keys_coe2': nulos_llave_coe2, 'duplicate_keys_sdem': duplicados_sdem, 'duplicate_keys_coe2': duplicados_coe2, 'null_key_rate_pct': null_key_rate_pct, 'duplicate_key_rate_pct': duplicate_key_rate_pct}, 'expansion_factor_quality': expansion_quality, 'income_input_availability': income_availability, 'schema_quality': {'required_columns_present': True, 'historical_equivalences_applied': {'SDEM': schema_sdem.get('applied_equivalences', {}), 'COE2': schema_coe2.get('applied_equivalences', {})}, 'column_count': len(column_names), 'columns_output': column_names}}
            metadata_json = {'general_information': {'dataset_name': 'ENOE_SDEM_COE2_MERGE', 'dataset_period': periodo, 'dataset_level': 'persona', 'dataset_type': 'merge', 'created_at_utc': _utc_now_iso(), 'created_by_function': 'merge_period_parquet', 'function_version': '2.2.0', 'project_name': 'Calculo de pobreza laboral'}, 'source_data': {'left_dataset': str(path_sdem), 'right_dataset': str(path_coe2), 'left_table': 'SDEM', 'right_table': 'COE2'}, 'construction_method': {'function': 'merge_period_parquet', 'method_description': 'Unión LEFT JOIN entre SDEM y COE2 usando SDEM como tabla madre.', 'join_type': 'left_join', 'join_keys': llave_persona, 'compression': compression, 'overwrite': overwrite, 'strict_keys': strict_keys}, 'dataset_size': {'rows_left_sdem': filas_sdem, 'rows_right_coe2': filas_coe2, 'rows_output': filas_resultado, 'columns_output': len(column_names), 'file_size_mb': file_size_mb, 'row_groups': row_groups_merge}, 'schema': {'column_count': len(column_names), 'columns': column_dict}, 'critical_variables': ['p6b2', 'p6c', 'fac_tri', 'cruce_coe2'], 'quality_indicator_dictionary': quality_dict, 'quality_indicators': quality_indicators, 'methodological_observations': ['SDEM se utiliza como tabla madre.', 'La unión se realiza mediante left join.'] + metadata_warnings}
            metadata_file_path = output_folder / f'ENOE_{periodo}_unida_metadata_quality.json'
            _write_json_metadata(metadata_json, metadata_file_path)
            step_info = {'step_number': None, 'step_name': 'merge_sdem_coe2', 'function_name': 'merge_period_parquet', 'transformation_description': 'Unión LEFT JOIN entre SDEM y COE2 usando la llave de persona.', 'status': 'ok', 'started_at_utc': metadata_start_time.isoformat(), 'finished_at_utc': metadata_end_time.isoformat(), 'duration_seconds': (metadata_end_time - metadata_start_time).total_seconds(), 'input': {'objects': [{'object_name': path_sdem.name, 'object_type': 'parquet', 'path': str(path_sdem), 'role': 'left_table'}, {'object_name': path_coe2.name, 'object_type': 'parquet', 'path': str(path_coe2), 'role': 'right_table'}]}, 'parameters': {'join_type': 'left_join', 'join_keys': llave_persona, 'compression': compression, 'overwrite': overwrite, 'strict_keys': strict_keys}, 'output': {'object_name': output_path.name, 'object_type': 'parquet', 'path': str(output_path), 'role': 'dataset_unido_sdem_coe2'}, 'execution_summary': {'filas_sdem': filas_sdem, 'filas_coe2': filas_coe2, 'filas_resultado': filas_resultado, 'registros_cruzados': registros_cruzados, 'registros_sin_coe2': registros_sin_coe2, 'join_rate_pct': join_rate_pct, 'left_only_rate_pct': left_only_rate_pct}, 'warnings': [], 'errors': []}
            result['metadata'] = metadata_json
        except Exception as metadata_error:
            warnings.warn(f'No fue posible generar los metadatos del merge {periodo}: {metadata_error}', RuntimeWarning, stacklevel=2)
        try:
            paradata_end_time = datetime.now(timezone.utc)
            merge_step = {'step_number': None, 'step_name': 'merge_sdem_coe2', 'function_name': 'merge_period_parquet', 'transformation_description': 'Unión LEFT JOIN entre SDEM y COE2 usando la llave de persona.', 'status': 'ok', 'started_at_utc': metadata_start_time.isoformat(), 'finished_at_utc': paradata_end_time.isoformat(), 'duration_seconds': (paradata_end_time - metadata_start_time).total_seconds(), 'input': {'objects': [{'object_name': path_sdem.name, 'object_type': 'parquet', 'path': str(path_sdem), 'role': 'left_table'}, {'object_name': path_coe2.name, 'object_type': 'parquet', 'path': str(path_coe2), 'role': 'right_table'}]}, 'parameters': {'join_type': 'left_join', 'join_keys': llave_persona, 'compression': compression, 'overwrite': overwrite, 'strict_keys': strict_keys, 'historical_equivalences': {'SDEM': schema_sdem.get('applied_equivalences', {}), 'COE2': schema_coe2.get('applied_equivalences', {})}}, 'output': {'object_name': output_path.name, 'object_type': 'parquet', 'path': str(output_path), 'role': 'dataset_unido_sdem_coe2', 'metadata_json': str(output_folder / f'ENOE_{periodo}_unida_metadata_quality.json')}, 'execution_summary': {'filas_sdem': filas_sdem, 'filas_coe2': filas_coe2, 'filas_resultado': filas_resultado, 'columnas_resultado': len(columnas_resultado), 'registros_cruzados': registros_cruzados, 'registros_sin_coe2': registros_sin_coe2}, 'warnings': [], 'errors': []}
            _append_period_paradata_step(periodo, merge_step, path_paradata, initial_notes=['SDEM se utiliza como tabla madre.', 'COE2 se incorpora mediante LEFT JOIN.'])
            result['paradata'] = merge_step
        except Exception as paradata_error:
            warnings.warn(f'No fue posible registrar el paradato del merge {periodo}: {paradata_error}', RuntimeWarning, stacklevel=2)
    return result

def build_merged_time_series(time_series, cols_coe2, cols_sdem, llave_persona, path_out='../Merge_de_parquet', equivalences=None, periods=None, overwrite=False, reuse_existing=True, compression='zstd', strict_keys=True, stop_on_error=True, return_audit=True, *, generate_metadata=False, path_paradata='../Paradata'):
    """
    Construye la serie temporal de Parquet unidos SDEM-COE2.

    La función recorre los periodos contenidos en el diccionario
    generado por get_time_series(), ejecuta merge_period_parquet()
    para cada trimestre y retorna un nuevo diccionario ordenado
    cronológicamente con las rutas de los Parquet unidos.

    No concatena todos los trimestres en un único archivo. Cada
    trimestre se conserva como un Parquet independiente.

    Parámetros
    ----------
    time_series : dict
        Diccionario con la serie de Parquet fuente.

        Estructura esperada:
            {
                "2006T1": [ruta_coe2, ruta_sdem],
                "2006T2": [ruta_coe2, ruta_sdem],
                ...
            }

        También admite:
            {
                "2006T1": {
                    "COE2": ruta_coe2,
                    "SDEM": ruta_sdem
                }
            }

    cols_coe2 : list
        Columnas canónicas requeridas de COE2.

    cols_sdem : list
        Columnas canónicas requeridas de SDEM.

    llave_persona : list
        Columnas canónicas que forman la llave de persona.

    path_out : str o pathlib.Path
        Carpeta raíz donde se almacenarán los Parquet unidos.

    equivalences : dict, opcional
        Equivalencias históricas entre nombres de columnas.

    periods : list, opcional
        Lista de periodos que se desean procesar.

        Ejemplo:
            ["2007T4", "2020T3", "2026T1"]

        Si es None, procesa todos los periodos.

    overwrite : bool, default=False
        Si es True, vuelve a generar los archivos existentes.

    reuse_existing : bool, default=True
        Si el archivo final ya existe y overwrite=False:

        - True: reutiliza el Parquet existente.
        - False: genera un error.

    compression : str, default="zstd"
        Compresión utilizada por merge_period_parquet().

    strict_keys : bool, default=True
        Detiene el merge individual si las llaves presentan
        nulos o duplicados.

    stop_on_error : bool, default=True
        - True: detiene toda la serie al encontrar un error.
        - False: registra el error y continúa con los demás
          trimestres.

    return_audit : bool, default=True
        Si es True, retorna:
            serie_unida, auditoria

        Si es False, retorna solamente:
            serie_unida

    Retorna
    -------
    dict
        Serie temporal con las rutas de los Parquet unidos.

    pandas.DataFrame, opcional
        Tabla de auditoría con una fila por periodo.
    """
    from pathlib import Path
    from pandas import DataFrame
    import re
    import pyarrow.parquet as pq

    def parse_period(period):
        """
        Valida una clave AAAATX y retorna año y trimestre.
        """
        match = re.fullmatch('(\\d{4})T([1-4])', str(period).upper())
        if not match:
            raise ValueError(f'Periodo inválido: {period}. Se esperaba el formato AAAATX.')
        year, quarter = match.groups()
        return (int(year), int(quarter), f'{int(year)}T{int(quarter)}')

    def get_source_paths(period_value):
        """
        Obtiene las rutas COE2 y SDEM independientemente de si
        el valor del diccionario es una lista, tupla o diccionario.
        """
        if isinstance(period_value, dict):
            path_coe2 = period_value.get('COE2') or period_value.get('coe2')
            path_sdem = period_value.get('SDEM') or period_value.get('sdem')
        elif isinstance(period_value, (list, tuple)):
            if len(period_value) != 2:
                raise ValueError('El valor de cada periodo debe contener exactamente dos rutas: [COE2, SDEM].')
            path_coe2, path_sdem = period_value
        else:
            raise TypeError('El valor de cada periodo debe ser una lista, tupla o diccionario con las rutas de COE2 y SDEM.')
        return (path_coe2, path_sdem)
    path_out = Path(path_out)
    path_out.mkdir(parents=True, exist_ok=True)
    available_periods = []
    for period in time_series.keys():
        year, quarter, normalized_period = parse_period(period)
        available_periods.append((year, quarter, normalized_period, period))
    available_periods.sort(key=lambda item: (item[0], item[1]))
    if periods is not None:
        requested_periods = set()
        for period in periods:
            _, _, normalized_period = parse_period(period)
            requested_periods.add(normalized_period)
        periods_to_process = [item for item in available_periods if item[2] in requested_periods]
        available_normalized = {item[2] for item in available_periods}
        missing_requested = sorted(requested_periods - available_normalized)
        if missing_requested:
            raise KeyError(f'Los siguientes periodos no existen en time_series: {missing_requested}')
    else:
        periods_to_process = available_periods
    merged_time_series = {}
    audit_records = []
    total_periods = len(periods_to_process)
    print(f'Periodos a procesar: {total_periods}')
    for index, (year, quarter, period, original_key) in enumerate(periods_to_process, start=1):
        print(f'\n[{index}/{total_periods}] Procesando {period}')
        output_folder = path_out / period
        output_path = output_folder / f'ENOE_{period}_unida.parquet'
        record = {'periodo': period, 'anio': year, 'trimestre': quarter, 'estado': None, 'path_output': None, 'filas_sdem': None, 'filas_coe2': None, 'filas_resultado': None, 'columnas_resultado': None, 'nulos_llave_sdem': None, 'nulos_llave_coe2': None, 'duplicados_sdem': None, 'duplicados_coe2': None, 'registros_cruzados': None, 'registros_sin_coe2': None, 'tasa_cruce_global': None, 'error_tipo': None, 'error_mensaje': None, 'OK': False}
        try:
            path_coe2, path_sdem = get_source_paths(time_series[original_key])
            if path_coe2 is None:
                raise FileNotFoundError(f'[{period}] No existe una ruta asignada para COE2.')
            if path_sdem is None:
                raise FileNotFoundError(f'[{period}] No existe una ruta asignada para SDEM.')
            path_coe2 = Path(path_coe2)
            path_sdem = Path(path_sdem)
            if not path_coe2.exists():
                raise FileNotFoundError(f'[{period}] No existe el archivo COE2:\n{path_coe2}')
            if not path_sdem.exists():
                raise FileNotFoundError(f'[{period}] No existe el archivo SDEM:\n{path_sdem}')
            if output_path.exists() and (not overwrite):
                if not reuse_existing:
                    raise FileExistsError(f'El archivo ya existe:\n{output_path}')
                parquet_existing = pq.ParquetFile(output_path)
                filas_existing = parquet_existing.metadata.num_rows
                columnas_existing = list(parquet_existing.schema.names)
                del parquet_existing
                merged_time_series[period] = output_path
                record.update({'estado': 'reutilizado', 'path_output': output_path, 'filas_resultado': int(filas_existing), 'columnas_resultado': len(columnas_existing), 'OK': True})
                print(f'  Archivo existente reutilizado: {filas_existing:,} filas')
                audit_records.append(record)
                if generate_metadata:
                    step_info = {'step_number': None, 'step_name': 'merge_sdem_coe2', 'function_name': 'merge_period_parquet', 'transformation_description': 'Reutilización de Parquet unido existente.', 'status': 'reused', 'started_at_utc': _utc_now_iso(), 'finished_at_utc': _utc_now_iso(), 'duration_seconds': 0, 'input': {'objects': [{'object_name': path_sdem.name, 'object_type': 'parquet', 'path': str(path_sdem), 'role': 'left_table'}, {'object_name': path_coe2.name, 'object_type': 'parquet', 'path': str(path_coe2), 'role': 'right_table'}]}, 'parameters': {'join_type': 'left_join', 'join_keys': llave_persona, 'compression': compression, 'overwrite': overwrite, 'strict_keys': strict_keys}, 'output': {'object_name': output_path.name, 'object_type': 'parquet', 'path': str(output_path), 'role': 'dataset_unido_sdem_coe2'}, 'execution_summary': {'filas_resultado': filas_existing, 'registros_cruzados': None, 'registros_sin_coe2': None, 'join_rate_pct': None, 'left_only_rate_pct': None}, 'warnings': [], 'errors': []}
                    _append_period_paradata_step(period, step_info, path_paradata)
                continue
            merge_result = merge_period_parquet(periodo=period, path_coe2=path_coe2, path_sdem=path_sdem, cols_coe2=cols_coe2, cols_sdem=cols_sdem, llave_persona=llave_persona, path_out=path_out, equivalences=equivalences, overwrite=overwrite, compression=compression, strict_keys=strict_keys, optional_cols_coe2=optional_cols_coe2, optional_cols_sdem=optional_cols_sdem, generate_metadata=generate_metadata, path_paradata=path_paradata)
            merged_time_series[period] = merge_result['path_output']
            record.update({'estado': 'procesado', 'path_output': merge_result['path_output'], 'filas_sdem': merge_result['filas_sdem'], 'filas_coe2': merge_result['filas_coe2'], 'filas_resultado': merge_result['filas_resultado'], 'columnas_resultado': merge_result['columnas_resultado'], 'nulos_llave_sdem': merge_result['nulos_llave_sdem'], 'nulos_llave_coe2': merge_result['nulos_llave_coe2'], 'duplicados_sdem': merge_result['duplicados_sdem'], 'duplicados_coe2': merge_result['duplicados_coe2'], 'registros_cruzados': merge_result['registros_cruzados'], 'registros_sin_coe2': merge_result['registros_sin_coe2'], 'tasa_cruce_global': merge_result['tasa_cruce_global'], 'OK': True})
            print(f"  Merge completado: {merge_result['filas_resultado']:,} filas")
            print(f"  Tasa de cruce: {merge_result['tasa_cruce_global']:.2f}%")
        except Exception as error:
            record.update({'estado': 'error', 'error_tipo': type(error).__name__, 'error_mensaje': str(error), 'OK': False})
            print(f'  ERROR EN {period}: {type(error).__name__}')
            print(f'  {error}')
            audit_records.append(record)
            if generate_metadata:
                error_step = {'step_number': None, 'step_name': 'compute_poverty_labor_period', 'function_name': 'compute_pl_period', 'transformation_description': 'Intento de cálculo del índice de pobreza laboral del periodo.', 'status': 'error', 'started_at_utc': period_started_at_iso, 'finished_at_utc': _utc_now_iso(), 'duration_seconds': duration, 'input': {'object_name': path_analysis.name, 'object_type': 'parquet', 'path': str(path_analysis)}, 'parameters': {'periodo': period, 'round_digits': round_digits}, 'output': {}, 'execution_summary': {'periodo': period}, 'warnings': [], 'errors': [{'type': type(error).__name__, 'message': str(error)}]}
                _append_period_paradata_step(period, error_step, path_paradata)
            if stop_on_error:
                raise
            continue
        audit_records.append(record)
    audit = DataFrame(audit_records)
    if not audit.empty:
        audit = audit.sort_values(by=['anio', 'trimestre']).reset_index(drop=True)
    processed = int((audit['estado'] == 'procesado').sum()) if not audit.empty else 0
    reused = int((audit['estado'] == 'reutilizado').sum()) if not audit.empty else 0
    errors = int((audit['estado'] == 'error').sum()) if not audit.empty else 0
    print('\nRESUMEN DE LA CONSTRUCCIÓN')
    print(f'Periodos solicitados: {total_periods}')
    print(f'Periodos procesados: {processed}')
    print(f'Periodos reutilizados: {reused}')
    print(f'Periodos con error: {errors}')
    print(f'Parquet disponibles: {len(merged_time_series)}')
    if return_audit:
        return (merged_time_series, audit)
    return merged_time_series
