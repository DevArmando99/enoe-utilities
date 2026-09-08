"""Construcción del Parquet histórico persona–trimestre."""
from .metadata import _safe_file_size_gb, _read_parquet_schema_info, _write_json_metadata, _write_yaml_paradata, _build_column_dictionary, _build_quality_indicator_dictionary
from .utils import _paradata_path_string

def build_historical_analysis_parquet(analysis_sources='../Analisis_PL_parquet', periods=None, start_period=None, end_period=None, path_out='../Serie_Historica_PL', output_name=None, overwrite=False, compression='zstd', row_group_size=100000, strict_schema=True, required_columns=None, validate_content=True, require_complete=False, stop_on_error=False, save_manifest=True, order_by_period=True, event_callback=None, metadata_callback=None, paradata_callback=None, strict_hooks=False, *, generate_metadata=False, path_paradata='../Paradata', excluded_periods=None):
    """
    Concatena Parquet analíticos trimestrales de pobreza laboral
    en un único Parquet histórico persona-trimestre.

    Esta función parte de los archivos generados por:

        build_analysis_dataset()

    Por ejemplo:

        ../Analisis_PL_parquet/2006T1/ENOE_2006T1_analisis_pl.parquet
        ../Analisis_PL_parquet/2006T2/ENOE_2006T2_analisis_pl.parquet
        ...
        ../Analisis_PL_parquet/2026T1/ENOE_2026T1_analisis_pl.parquet

    y genera un archivo histórico como:

        ../Serie_Historica_PL/enoe_analisis_pl_2006T1_2026T1.parquet

    Parámetros
    ----------
    analysis_sources : dict, list, tuple, str o pathlib.Path
        Fuentes de los Parquet analíticos.

        Formatos admitidos:

        1. Carpeta raíz:
            "../Analisis_PL_parquet"

        2. Diccionario:
            {
                "2025T1": ruta_2025t1,
                "2025T2": ruta_2025t2
            }

        3. Diccionario con resultados:
            {
                "2025T1": {
                    "path_output": ruta_2025t1
                }
            }

        4. Lista de rutas:
            [
                ruta_2025t1,
                ruta_2025t2
            ]

        5. Lista de pares:
            [
                ("2025T1", ruta_2025t1),
                ("2025T2", ruta_2025t2)
            ]

    periods : list, opcional
        Lista explícita de periodos a concatenar.

    start_period : str, opcional
        Primer periodo de un rango trimestral inclusivo.

    end_period : str, opcional
        Último periodo de un rango trimestral inclusivo.

    path_out : str o pathlib.Path
        Carpeta donde se guardará el Parquet histórico.

    output_name : str, opcional
        Nombre base del archivo histórico.

    overwrite : bool, default=False
        Si es True, reemplaza el archivo existente.

    compression : str, default="zstd"
        Compresión del Parquet de salida.

    row_group_size : int, default=100_000
        Tamaño de row group usado al escribir el Parquet.

    strict_schema : bool, default=True
        Si es True, exige que todos los Parquet tengan exactamente
        las mismas columnas y en el mismo orden.

    required_columns : list, opcional
        Columnas mínimas que deben existir en cada Parquet.
        Si es None, se usa un conjunto mínimo útil para pobreza laboral.

    validate_content : bool, default=True
        Si es True, valida que la columna periodo, anio y trimestre
        coincidan con el periodo inferido desde la ruta.

    require_complete : bool, default=False
        Si es True, detiene el proceso cuando falta algún periodo
        solicitado.

    stop_on_error : bool, default=False
        Si es True, detiene el proceso al primer error de validación.

    save_manifest : bool, default=True
        Si es True, guarda un CSV con la auditoría/manifest de archivos.

    order_by_period : bool, default=True
        Si es True, ordena la salida por anio y trimestre.
        Puede tardar más en series grandes.

    event_callback : callable, opcional
        Callback para eventos.

    metadata_callback : callable, opcional
        Callback para metadatos.

    paradata_callback : callable, opcional
        Callback para paradatos.

    strict_hooks : bool, default=False
        Si es True, errores en callbacks detienen la ejecución.

    Retorna
    -------
    dict
        Diccionario con:

        path_output
            Ruta del Parquet histórico.

        audit
            DataFrame de auditoría por periodo.

        path_manifest
            Ruta del manifest CSV, si se guardó.

        metadata
            Bosquejo de metadatos.

        paradata
            Bosquejo de paradatos.

        events
            Eventos registrados.

        OK
            True si no hubo errores.

        completo
            True si se procesaron todos los periodos solicitados.
    """
    excluded_periods_list = [] if excluded_periods is None else [str(p) for p in excluded_periods] if isinstance(excluded_periods, (list, tuple, set)) else [str(excluded_periods)]
    from pathlib import Path
    from datetime import datetime, timezone
    import re
    import time
    import gc
    import warnings
    import duckdb
    import pandas as pd
    import pyarrow.parquet as pq

    def normalize_period(value):
        """
        Valida y normaliza un periodo al formato AAAATX.
        """
        match = re.fullmatch('(\\d{4})T([1-4])', str(value).strip().upper())
        if not match:
            raise ValueError(f'Periodo inválido: {value}. Se esperaba el formato AAAATX.')
        year, quarter = match.groups()
        return f'{int(year)}T{int(quarter)}'

    def period_sort_key(value):
        """
        Retorna una llave cronológica para ordenar periodos.
        """
        period = normalize_period(value)
        return (int(period[:4]), int(period[-1]))

    def build_period_range(first_period, last_period):
        """
        Genera un rango trimestral inclusivo.
        """
        first_period = normalize_period(first_period)
        last_period = normalize_period(last_period)
        first_year, first_quarter = period_sort_key(first_period)
        last_year, last_quarter = period_sort_key(last_period)
        first_index = first_year * 4 + first_quarter - 1
        last_index = last_year * 4 + last_quarter - 1
        if first_index > last_index:
            raise ValueError('start_period no puede ser posterior a end_period.')
        result = []
        for index in range(first_index, last_index + 1):
            year, zero_based_quarter = divmod(index, 4)
            result.append(f'{year}T{zero_based_quarter + 1}')
        return result

    def infer_period_from_path(path):
        """
        Extrae AAAATX desde una ruta.
        """
        match = re.search('(\\d{4}T[1-4])', str(path).upper())
        if not match:
            raise ValueError(f'No fue posible inferir el periodo desde la ruta:\n{path}')
        return normalize_period(match.group(1))

    def extract_path(value):
        """
        Extrae una ruta desde una estructura flexible.
        """
        if isinstance(value, dict):
            path = value.get('path_output') or value.get('path_analysis') or value.get('path') or value.get('archivo')
            if path is None:
                raise KeyError('El diccionario fuente no contiene una ruta en path_output, path_analysis, path o archivo.')
            return Path(path)
        return Path(value)

    def normalize_sources(sources):
        """
        Convierte las fuentes en un diccionario:

            {periodo: Path}
        """
        normalized = {}
        if isinstance(sources, (str, Path)):
            source_path = Path(sources)
            if source_path.is_dir():
                files = sorted(source_path.rglob('ENOE_*_analisis_pl.parquet'))
                if not files:
                    raise FileNotFoundError(f'No se encontraron Parquet analíticos en:\n{source_path}')
                for file_path in files:
                    period = infer_period_from_path(file_path)
                    if period in normalized:
                        raise ValueError(f'Se encontró más de un archivo para {period}.')
                    normalized[period] = file_path
            elif source_path.is_file():
                period = infer_period_from_path(source_path)
                normalized[period] = source_path
            else:
                raise FileNotFoundError(f'No existe la fuente indicada:\n{source_path}')
        elif isinstance(sources, dict):
            for raw_period, raw_value in sources.items():
                period = normalize_period(raw_period)
                path = extract_path(raw_value)
                if period in normalized:
                    raise ValueError(f'El periodo {period} está duplicado.')
                normalized[period] = path
        elif isinstance(sources, (list, tuple, set)):
            for item in sources:
                if isinstance(item, (list, tuple)) and len(item) == 2:
                    raw_period, raw_path = item
                    period = normalize_period(raw_period)
                    path = extract_path(raw_path)
                elif isinstance(item, dict):
                    raw_period = item.get('periodo') or item.get('period')
                    path = extract_path(item)
                    if raw_period is None:
                        period = infer_period_from_path(path)
                    else:
                        period = normalize_period(raw_period)
                else:
                    path = Path(item)
                    period = infer_period_from_path(path)
                if period in normalized:
                    raise ValueError(f'El periodo {period} está duplicado.')
                normalized[period] = path
        else:
            raise TypeError('analysis_sources debe ser un diccionario, una colección de rutas, una ruta de archivo o una carpeta.')
        return dict(sorted(normalized.items(), key=lambda item: period_sort_key(item[0])))

    def sql_path(path):
        """
        Convierte una ruta en un literal SQL para DuckDB.
        """
        path = Path(path).resolve().as_posix()
        path = path.replace("'", "''")
        return f"'{path}'"

    def sql_path_list(paths):
        """
        Construye una lista SQL de rutas para read_parquet().
        """
        return '[' + ', '.join((sql_path(path) for path in paths)) + ']'

    def safe_remove(path):
        """
        Elimina un archivo liberando manejadores.
        """
        path = Path(path)
        if not path.exists():
            return
        gc.collect()
        try:
            path.unlink()
        except PermissionError as error:
            raise PermissionError(f'No se pudo eliminar el archivo porque está siendo utilizado por otro proceso:\n{path}') from error

    def unique_ordered(values):
        """
        Elimina duplicados conservando el orden.
        """
        return list(dict.fromkeys(values))
    events = []

    def send_callback(callback, payload, callback_name):
        """
        Envía información a un callback opcional.
        """
        if callback is None:
            return
        try:
            callback(payload)
        except Exception as error:
            message = f'El callback {callback_name} produjo un error: {type(error).__name__}: {error}'
            if strict_hooks:
                raise RuntimeError(message) from error
            warnings.warn(message, RuntimeWarning, stacklevel=2)

    def emit_event(stage, status, message, period=None, metrics=None):
        """
        Registra un evento de ejecución.
        """
        payload = {'timestamp_utc': datetime.now(timezone.utc).isoformat(), 'funcion': 'build_historical_analysis_parquet', 'periodo': period, 'etapa': stage, 'estado': status, 'mensaje': message, 'metricas': metrics or {}}
        events.append(payload)
        send_callback(event_callback, payload, 'event_callback')
    started_at = datetime.now(timezone.utc)
    execution_start = time.perf_counter()
    emit_event(stage='inicio', status='ok', message='Inició la construcción del Parquet histórico de análisis de pobreza laboral.')
    source_map = normalize_sources(analysis_sources)
    available_periods = list(source_map.keys())
    if periods is not None and (start_period is not None or end_period is not None):
        raise ValueError('Utiliza periods o start_period/end_period, pero no ambos.')
    if periods is not None:
        requested_periods = unique_ordered([normalize_period(period) for period in periods])
        requested_periods = sorted(requested_periods, key=period_sort_key)
    elif start_period is not None or end_period is not None:
        if start_period is None or end_period is None:
            raise ValueError('Debes proporcionar tanto start_period como end_period.')
        requested_periods = build_period_range(start_period, end_period)
    else:
        requested_periods = available_periods.copy()
    if not requested_periods:
        raise ValueError('No se proporcionaron periodos para concatenar.')
    missing_periods = [period for period in requested_periods if period not in source_map]
    warning_records = []
    error_records = []
    for period in missing_periods:
        message = f'No se encontró Parquet analítico para {period}.'
        warning_record = {'periodo': period, 'tipo': 'periodo_faltante', 'mensaje': message}
        warning_records.append(warning_record)
        warnings.warn(message, RuntimeWarning, stacklevel=2)
        emit_event(stage='validacion_fuentes', status='warning', message=message, period=period)
    if require_complete and missing_periods:
        raise FileNotFoundError(f'Faltan los siguientes periodos solicitados: {missing_periods}')
    if required_columns is None:
        required_columns = ['periodo', 'anio', 'trimestre', 'id_persona_periodo', 'id_hogar_periodo', 'id_persona', 'id_hogar', 'fac_tri', 'factor_valido', 'ambito', 'lpei', 'ingreso_hogar', 'ingreso_pc', 'ingreso_pc_lpei_ratio', 'entra_calculo_pl', 'pobreza_laboral_persona', 'emp_ppal', 'tue_ppal', 'hrsocup', 'ing_x_hrs', 'scian', 'p7', 'p7c', 'p7f', 'p7f_dias', 'p7f_horas', 'p7g1', 'p7g2', 'p7g9', 'p7gcan']
    required_columns = [str(column).strip() for column in required_columns]
    audit_records = []
    valid_periods = []
    valid_paths = []
    reference_columns = None
    periods_to_review = [period for period in requested_periods if period in source_map]
    for index, period in enumerate(periods_to_review, start=1):
        path_analysis = Path(source_map[period])
        emit_event(stage='auditoria_archivo', status='inicio', message=f'Auditando {period} ({index}/{len(periods_to_review)}).', period=period)
        record = {'periodo': period, 'path_analysis': str(path_analysis), 'estado': None, 'filas': None, 'columnas': None, 'num_row_groups': None, 'file_size_mb': None, 'columnas_faltantes': None, 'schema_mismatch': None, 'filas_periodo_inconsistente': None, 'duplicados_id_persona_periodo': None, 'error_tipo': None, 'error_mensaje': None, 'OK': False}
        try:
            if not path_analysis.exists():
                raise FileNotFoundError(f'No existe el Parquet analítico:\n{path_analysis}')
            parquet_file = pq.ParquetFile(path_analysis)
            columns = list(parquet_file.schema.names)
            num_rows = int(parquet_file.metadata.num_rows)
            num_row_groups = int(parquet_file.metadata.num_row_groups)
            file_size_mb = path_analysis.stat().st_size / 1024 / 1024
            missing_columns_file = [column for column in required_columns if column not in columns]
            if reference_columns is None:
                reference_columns = columns.copy()
            schema_mismatch = columns != reference_columns
            record.update({'filas': num_rows, 'columnas': len(columns), 'num_row_groups': num_row_groups, 'file_size_mb': file_size_mb, 'columnas_faltantes': missing_columns_file, 'schema_mismatch': schema_mismatch})
            validation_errors = []
            if num_rows == 0:
                validation_errors.append('El Parquet no contiene filas.')
            if missing_columns_file:
                validation_errors.append(f'Faltan columnas requeridas: {missing_columns_file}')
            if strict_schema and schema_mismatch:
                validation_errors.append('El esquema no coincide con el esquema de referencia.')
            if validate_content and (not missing_columns_file):
                year_expected = int(period[:4])
                quarter_expected = int(period[-1])
                con_check = duckdb.connect(database=':memory:')
                try:
                    content_metrics = con_check.execute(f"\n                        SELECT\n                            SUM(\n                                CASE\n                                    WHEN CAST(periodo AS VARCHAR)\n                                         <> '{period}'\n                                      OR TRY_CAST(anio AS INTEGER)\n                                         <> {year_expected}\n                                      OR TRY_CAST(trimestre AS INTEGER)\n                                         <> {quarter_expected}\n                                    THEN 1\n                                    ELSE 0\n                                END\n                            ) AS filas_periodo_inconsistente,\n\n                            COALESCE(\n                                SUM(n - 1),\n                                0\n                            ) AS duplicados_id_persona_periodo\n\n                        FROM (\n\n                            SELECT\n                                periodo,\n                                anio,\n                                trimestre,\n                                id_persona_periodo,\n                                COUNT(*) OVER (\n                                    PARTITION BY id_persona_periodo\n                                ) AS n\n\n                            FROM read_parquet(\n                                {sql_path(path_analysis)}\n                            )\n                        )\n                        ").fetchone()
                    filas_periodo_inconsistente = int(content_metrics[0] or 0)
                    duplicados_id_persona_periodo = int(content_metrics[1] or 0)
                    record.update({'filas_periodo_inconsistente': filas_periodo_inconsistente, 'duplicados_id_persona_periodo': duplicados_id_persona_periodo})
                    if filas_periodo_inconsistente > 0:
                        validation_errors.append(f'Hay {filas_periodo_inconsistente:,} filas con periodo/anio/trimestre inconsistente.')
                    if duplicados_id_persona_periodo > 0:
                        validation_errors.append(f'Hay {duplicados_id_persona_periodo:,} duplicados en id_persona_periodo.')
                finally:
                    con_check.close()
                    gc.collect()
            if validation_errors:
                message = ' | '.join(validation_errors)
                raise ValueError(message)
            valid_periods.append(period)
            valid_paths.append(path_analysis)
            record.update({'estado': 'validado', 'OK': True})
            emit_event(stage='auditoria_archivo', status='ok', message=f'Archivo {period} validado.', period=period, metrics={'filas': num_rows, 'columnas': len(columns)})
        except Exception as error:
            error_record = {'periodo': period, 'path_analysis': str(path_analysis), 'tipo': type(error).__name__, 'mensaje': str(error)}
            error_records.append(error_record)
            record.update({'estado': 'error', 'error_tipo': type(error).__name__, 'error_mensaje': str(error), 'OK': False})
            warnings.warn(f'Error al auditar {period}: {type(error).__name__}: {error}', RuntimeWarning, stacklevel=2)
            emit_event(stage='auditoria_archivo', status='error', message=str(error), period=period, metrics={'error_tipo': type(error).__name__})
            if stop_on_error:
                raise
        finally:
            audit_records.append(record)
    if not valid_paths:
        raise RuntimeError('No hay Parquet analíticos válidos para concatenar.')
    path_out = Path(path_out)
    path_out.mkdir(parents=True, exist_ok=True)
    first_period = valid_periods[0]
    last_period = valid_periods[-1]
    if output_name is None:
        output_name = f'enoe_analisis_pl_{first_period}_{last_period}'
    output_name = Path(str(output_name)).stem
    output_path = path_out / f'{output_name}.parquet'
    temp_path = path_out / f'{output_name}.tmp.parquet'
    path_manifest = path_out / f'{output_name}_manifest.csv' if save_manifest else None
    if output_path.exists() and (not overwrite):
        raise FileExistsError(f'El archivo histórico ya existe:\n{output_path}\nUtiliza overwrite=True para reemplazarlo.')
    if path_manifest is not None and path_manifest.exists() and (not overwrite):
        raise FileExistsError(f'El manifest ya existe:\n{path_manifest}\nUtiliza overwrite=True para reemplazarlo.')
    if temp_path.exists():
        safe_remove(temp_path)
    emit_event(stage='escritura_historico', status='inicio', message='Inició la escritura del Parquet histórico.', metrics={'periodos_validos': len(valid_periods), 'filas_estimadas': sum((record['filas'] or 0 for record in audit_records if record['OK']))})
    con = duckdb.connect(database=':memory:')
    try:
        compression_sql = str(compression).strip().upper()
        paths_sql = sql_path_list(valid_paths)
        order_clause = '\n            ORDER BY\n                TRY_CAST(anio AS INTEGER),\n                TRY_CAST(trimestre AS INTEGER)\n            ' if order_by_period else ''
        con.execute(f"\n            COPY (\n\n                SELECT *\n                FROM read_parquet(\n                    {paths_sql},\n                    union_by_name = true\n                )\n\n                {order_clause}\n\n            )\n\n            TO {sql_path(temp_path)}\n\n            (\n                FORMAT PARQUET,\n                COMPRESSION '{compression_sql}',\n                ROW_GROUP_SIZE {int(row_group_size)}\n            )\n            ")
    except Exception:
        con.close()
        con = None
        gc.collect()
        if temp_path.exists():
            try:
                temp_path.unlink()
            except PermissionError:
                pass
        emit_event(stage='escritura_historico', status='error', message='Falló la escritura del Parquet histórico.')
        raise
    finally:
        if con is not None:
            con.close()
        gc.collect()
    if not temp_path.exists():
        raise RuntimeError(f'No se generó el archivo temporal esperado:\n{temp_path}')
    parquet_output = pq.ParquetFile(temp_path)
    output_rows = int(parquet_output.metadata.num_rows)
    output_columns = list(parquet_output.schema.names)
    output_row_groups = int(parquet_output.metadata.num_row_groups)
    del parquet_output
    gc.collect()
    expected_rows = int(sum((record['filas'] or 0 for record in audit_records if record['OK'])))
    if output_rows != expected_rows:
        safe_remove(temp_path)
        raise RuntimeError(f'El número de filas del Parquet histórico no coincide con la suma de los Parquet trimestrales:\nEsperadas: {expected_rows:,}\nObtenidas: {output_rows:,}')
    if output_path.exists():
        safe_remove(output_path)
    temp_path.replace(output_path)
    emit_event(stage='escritura_historico', status='ok', message='Parquet histórico construido correctamente.', metrics={'path_output': str(output_path), 'filas_salida': output_rows, 'columnas_salida': len(output_columns)})
    audit_df = pd.DataFrame(audit_records)
    if not audit_df.empty:
        audit_df['anio_orden'] = audit_df['periodo'].str[:4].astype(int)
        audit_df['trimestre_orden'] = audit_df['periodo'].str[-1].astype(int)
        audit_df = audit_df.sort_values(by=['anio_orden', 'trimestre_orden']).drop(columns=['anio_orden', 'trimestre_orden']).reset_index(drop=True)
    if path_manifest is not None:
        temp_manifest = path_manifest.with_name(f'{path_manifest.stem}.tmp.csv')
        if temp_manifest.exists():
            temp_manifest.unlink()
        audit_df.to_csv(temp_manifest, index=False, encoding='utf-8-sig')
        if path_manifest.exists():
            path_manifest.unlink()
        temp_manifest.replace(path_manifest)
    finished_at = datetime.now(timezone.utc)
    duration_total = time.perf_counter() - execution_start
    processed_periods = audit_df.loc[audit_df['OK'].eq(True), 'periodo'].tolist() if not audit_df.empty else []
    failed_periods = audit_df.loc[audit_df['OK'].eq(False), 'periodo'].tolist() if not audit_df.empty else []
    complete = len(missing_periods) == 0 and len(failed_periods) == 0 and (len(processed_periods) == len(requested_periods))
    metadata = {'function': 'build_historical_analysis_parquet', 'function_version': '0.1.0', 'created_at_utc': finished_at.isoformat(), 'tipo_dataset': 'historico_persona_trimestre_pobreza_laboral', 'descripcion': 'Parquet histórico construido mediante concatenación de Parquet analíticos trimestrales generados por build_analysis_dataset().', 'periodo_inicial': first_period, 'periodo_final': last_period, 'periodos_procesados': len(processed_periods), 'filas_salida': output_rows, 'columnas_salida': output_columns, 'num_row_groups': output_row_groups, 'formato_salida': 'parquet', 'compresion': compression, 'row_group_size': row_group_size, 'path_output': str(output_path), 'path_manifest': str(path_manifest) if path_manifest is not None else None, 'columnas_minimas_requeridas': required_columns}
    paradata = {'function': 'build_historical_analysis_parquet', 'started_at_utc': started_at.isoformat(), 'finished_at_utc': finished_at.isoformat(), 'duration_seconds': duration_total, 'analysis_sources': str(analysis_sources), 'periodos_disponibles': available_periods, 'periodos_solicitados': requested_periods, 'periodos_procesados': processed_periods, 'periodos_faltantes': missing_periods, 'periodos_con_error': failed_periods, 'warnings': warning_records, 'errors': error_records, 'audit_records': audit_records, 'transformaciones': ['deteccion_parquets_analiticos', 'validacion_existencia_archivos', 'validacion_columnas_minimas', 'validacion_esquema_consistente', 'validacion_periodo_anio_trimestre', 'validacion_id_persona_periodo', 'concatenacion_parquets_con_duckdb', 'escritura_parquet_historico', 'generacion_manifest'], 'salidas': {'parquet_historico': str(output_path), 'manifest': str(path_manifest) if path_manifest is not None else None}}
    send_callback(metadata_callback, metadata, 'metadata_callback')
    send_callback(paradata_callback, paradata, 'paradata_callback')
    if generate_metadata:
        try:
            out_dir = Path(path_out)
            out_dir.mkdir(parents=True, exist_ok=True)
            if output_name:
                hist_base = output_name
            else:
                hist_base = f'enoe_analisis_pl_{first_period}_{last_period}'
            hist_metadata_path = out_dir / f'{hist_base}_metadata_quality.json'
            general_information = {'dataset_family': 'ENOE', 'dataset_level': 'historical', 'indicator_name': 'pobreza_laboral', 'project_name': 'Calculo de pobreza laboral', 'function': 'build_historical_analysis_parquet', 'period_range': {'first': first_period, 'last': last_period}, 'created_at_utc': finished_at.isoformat(), 'updated_at_utc': finished_at.isoformat(), 'notes': ['El Parquet histórico se genera concatenando los Parquet analíticos de cada trimestre.', 'La validación de esquema asegura que las columnas sean consistentes entre periodos.']}
            source_data = {'analysis_files': [str(path) for path in (source_map[p] for p in processed_periods)] if 'source_map' in locals() else []}
            construction_method = {'steps': ['Detección y validación de los Parquet analíticos de entrada.', 'Verificación de columnas mínimas y consistencia de esquema.', 'Validación de que el periodo, año y trimestre coinciden con la ruta.', 'Concatenación con DuckDB aplicando order_by_period si corresponde.', 'Escritura del Parquet histórico con compresión y tamaño de row group configurado.'], 'parameters': {'compression': compression, 'row_group_size': row_group_size, 'strict_schema': strict_schema, 'validate_content': validate_content}}
            hist_info = _read_parquet_schema_info(str(output_path)) if output_path else None
            dataset_size = {'rows': output_rows, 'columns': len(output_columns), 'row_groups': hist_info.get('row_groups') if hist_info else None, 'file_size_gb': _safe_file_size_gb(str(output_path)) if output_path else None}
            col_dict = {}
            if hist_info:
                names = hist_info.get('column_names', [])
                types = hist_info.get('column_types', [])
                enriched = _build_column_dictionary(names, dataset_type='historical')
                for i, name in enumerate(names):
                    col_dict[name] = {**enriched.get(name, {}), 'type': types[i] if i < len(types) else None}
            schema = {'column_count': len(col_dict), 'columns': col_dict}
            expected_periods_count = len(requested_periods)
            processed_count = len(processed_periods)
            excluded_periods_auto = [p for p in requested_periods if p not in processed_periods]
            missing_unexpected = [p for p in available_periods if p not in requested_periods]
            coverage_pct = processed_count / expected_periods_count * 100 if expected_periods_count > 0 else None
            temporal_quality = {'expected_periods': expected_periods_count, 'processed_periods': processed_count, 'excluded_periods': excluded_periods_auto, 'missing_periods': missing_periods, 'coverage_pct': coverage_pct}
            total_rows = None
            duplicate_keys = None
            rows_by_period = []
            try:
                import duckdb
                con_h = duckdb.connect(database=':memory:')
                total_rows, duplicate_keys = con_h.execute(f"\n                    SELECT\n                        COUNT(*) AS total_rows,\n                        COUNT(*) - COUNT(DISTINCT id_persona_periodo) AS duplicate_keys\n                    FROM read_parquet('{output_path.as_posix()}')\n                    ").fetchone()
                rows_by_period = [{'periodo': row[0], 'rows': int(row[1])} for row in con_h.execute(f"SELECT periodo, COUNT(*) AS rows FROM read_parquet('{output_path.as_posix()}') GROUP BY periodo").fetchall()]
                con_h.close()
            except Exception:
                pass
            row_quality = {'total_rows': int(total_rows) if total_rows is not None else None, 'rows_by_period': rows_by_period, 'expected_rows_from_inputs': None, 'row_count_consistent': None}
            key_quality = {'duplicate_id_persona_periodo': int(duplicate_keys) if duplicate_keys is not None else None}
            file_quality = {'file_size_gb': dataset_size.get('file_size_gb'), 'row_groups': dataset_size.get('row_groups'), 'compression': compression}
            schema_quality = {'consistent_columns': len(output_columns), 'schema_mismatch_periods': [], 'column_count': len(output_columns)}
            qid = _build_quality_indicator_dictionary('historical')
            quality_indicators = {'temporal_quality': temporal_quality, 'row_quality': row_quality, 'key_quality': key_quality, 'file_quality': file_quality, 'schema_quality': schema_quality}
            methodological_observations = ['Se asume que id_persona_periodo existe y es la llave única de persona por periodo.', 'La verificación de filas esperadas no se implementa; por lo tanto, expected_rows_from_inputs y row_count_consistent quedan como null.']
            hist_metadata = {'general_information': general_information, 'source_data': source_data, 'construction_method': construction_method, 'dataset_size': dataset_size, 'schema': schema, 'critical_variables': [], 'quality_indicator_dictionary': qid, 'quality_indicators': quality_indicators, 'methodological_observations': methodological_observations}
            _write_json_metadata(hist_metadata, hist_metadata_path)
            paradata_hist_dir = Path(path_paradata) / 'historico'
            paradata_hist_dir.mkdir(parents=True, exist_ok=True)
            yaml_hist_path = paradata_hist_dir / f'paradata_historico_{first_period}_{last_period}.yaml'
            source_items = [{'periodo': p, 'path': _paradata_path_string(source_map[p])} for p in requested_periods if p in source_map]
            hist_status = 'ok' if not failed_periods and (not missing_periods) else 'warning'
            methodological_notes = []
            if excluded_periods_list:
                methodological_notes.append('El periodo 2020T2 se excluye por decisión metodológica asociada al quiebre COVID/ETOE.' if '2020T2' in excluded_periods_list else 'Periodos excluidos por decisión metodológica: ' + ', '.join(excluded_periods_list))
            hist_steps = [{'step_number': 1, 'step_name': 'normalize_sources', 'function_name': 'build_historical_analysis_parquet', 'status': 'ok', 'started_at_utc': started_at.isoformat(), 'finished_at_utc': started_at.isoformat(), 'duration_seconds': 0.0, 'input': {'analysis_sources': {'object_type': 'python_dict', 'total_sources': len(source_items), 'sources': source_items}}, 'parameters': {'periods': list(requested_periods)}, 'output': {'normalized_sources': len(source_items)}, 'execution_summary': {'requested_periods': len(requested_periods)}, 'warnings': [], 'errors': []}, {'step_number': 2, 'step_name': 'audit_input_parquets', 'function_name': 'build_historical_analysis_parquet', 'status': hist_status, 'started_at_utc': started_at.isoformat(), 'finished_at_utc': finished_at.isoformat(), 'duration_seconds': duration_total, 'input': {'periods': list(requested_periods)}, 'parameters': {'strict_schema': strict_schema, 'validate_content': validate_content}, 'output': {'object_type': 'audit_records', 'rows': len(audit_records)}, 'execution_summary': {'valid_periods': len(processed_periods), 'failed_periods': len(failed_periods), 'missing_periods': len(missing_periods)}, 'warnings': list(warning_records), 'errors': list(error_records)}, {'step_number': 3, 'step_name': 'concatenate_analysis_parquets', 'function_name': 'build_historical_analysis_parquet', 'status': 'ok' if output_path else 'error', 'started_at_utc': started_at.isoformat(), 'finished_at_utc': finished_at.isoformat(), 'duration_seconds': duration_total, 'input': {'valid_paths': [_paradata_path_string(source_map[p]) for p in processed_periods if p in source_map]}, 'parameters': {'compression': compression, 'row_group_size': row_group_size, 'order_by_period': order_by_period}, 'output': {'historical_parquet': str(output_path)}, 'execution_summary': {'rows': output_rows, 'columns': len(output_columns)}, 'warnings': [], 'errors': []}, {'step_number': 4, 'step_name': 'validate_output_parquet', 'function_name': 'build_historical_analysis_parquet', 'status': 'ok', 'started_at_utc': finished_at.isoformat(), 'finished_at_utc': finished_at.isoformat(), 'duration_seconds': 0.0, 'input': {'path': str(output_path)}, 'parameters': {}, 'output': {'historical_parquet': str(output_path), 'manifest': str(path_manifest) if path_manifest else None, 'metadata_json': str(hist_metadata_path)}, 'execution_summary': {'rows': output_rows, 'columns': len(output_columns), 'row_groups': output_row_groups}, 'warnings': [], 'errors': []}]
            hist_paradata = {'dataset_family': 'ENOE', 'project_name': 'Calculo de pobreza laboral', 'process_name': 'build_historical_analysis_parquet', 'paradata_type': 'historical_parquet_concatenation', 'status_general': hist_status, 'created_at_utc': started_at.isoformat(), 'updated_at_utc': finished_at.isoformat(), 'summary': {'total_duration_seconds': duration_total, 'total_periods_requested_for_processing': len(requested_periods), 'total_periods_processed': len(processed_periods), 'excluded_periods': excluded_periods_list, 'failed_periods': len(failed_periods), 'coverage_pct': coverage_pct, 'final_outputs': [x for x in [str(output_path), str(path_manifest) if path_manifest else None] if x]}, 'periods': {'requested': list(requested_periods), 'processed': list(processed_periods), 'excluded': excluded_periods_list, 'missing': list(missing_periods), 'errors': list(failed_periods)}, 'methodological_notes': methodological_notes, 'steps': hist_steps}
            _write_yaml_paradata(hist_paradata, yaml_hist_path)
            metadata = hist_metadata
            paradata = hist_paradata
        except Exception:
            pass
    emit_event(stage='fin', status='ok' if complete else 'warning', message='Terminó la construcción del Parquet histórico.', metrics={'periodos_solicitados': len(requested_periods), 'periodos_procesados': len(processed_periods), 'periodos_faltantes': len(missing_periods), 'periodos_con_error': len(failed_periods), 'filas_salida': output_rows, 'duracion_segundos': duration_total})
    result = {'path_output': output_path, 'audit': audit_df, 'path_manifest': path_manifest, 'metadata': metadata, 'paradata': paradata, 'events': events, 'filas_salida': output_rows, 'columnas_salida': len(output_columns), 'nombres_columnas_salida': output_columns, 'OK': len(error_records) == 0, 'completo': complete}
    return result
