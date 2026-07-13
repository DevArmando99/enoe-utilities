"""Cálculo trimestral y construcción de la serie de pobreza laboral."""
from .metadata import _utc_now_iso, _safe_file_size_mb, _read_parquet_schema_info, _write_json_metadata, _write_yaml_paradata, _append_period_paradata_step, _build_quality_indicator_dictionary
from .utils import _paradata_path_string

def compute_pl_period(periodo, path_analysis, lpei_quarterly=None, round_digits=2):
    """
    Calcula pobreza laboral CONEVAL/INEGI para un trimestre.

    Retorna un DataFrame con columnas:
        Año, Trimestre, PL
    """
    from pathlib import Path
    import re
    import duckdb
    import pandas as pd
    match = re.fullmatch('(\\d{4})T([1-4])', str(periodo).upper())
    if not match:
        raise ValueError(f'Periodo inválido: {periodo}. Usa formato AAAATX.')
    anio, trimestre = match.groups()
    anio = int(anio)
    trimestre = int(trimestre)
    path_analysis = Path(path_analysis)
    if not path_analysis.exists():
        raise FileNotFoundError(f'No existe el parquet analítico:\n{path_analysis}')
    if lpei_quarterly is not None:
        lpei_check = lpei_quarterly.copy()
        if lpei_check.index.name is not None or isinstance(lpei_check.index, pd.MultiIndex):
            lpei_check = lpei_check.reset_index()
        cols_norm = {col: str(col).replace('Año', 'anio').replace('Trimestre', 'trimestre').lower() for col in lpei_check.columns}
        lpei_check = lpei_check.rename(columns=cols_norm)
        if not {'anio', 'trimestre', 'rural', 'urbano'}.issubset(set(lpei_check.columns)):
            raise KeyError('lpei_quarterly debe tener Año/Trimestre y columnas Rural/Urbano.')
        existe_lpei = lpei_check.loc[lpei_check['anio'].eq(anio) & lpei_check['trimestre'].eq(trimestre)]
        if existe_lpei.empty:
            raise ValueError(f'No existe LPEI para {anio}T{trimestre}.')
    con = duckdb.connect(database=':memory:')
    try:
        resultado = con.execute(f"""\n            SELECT\n                {anio} AS "Año",\n                {trimestre} AS "Trimestre",\n\n                ROUND(\n                    (\n                        SUM(\n                            CASE\n                                WHEN pobreza_laboral_persona\n                                THEN fac_tri\n                                ELSE 0\n                            END\n                        )\n                        /\n                        NULLIF(SUM(fac_tri), 0)\n                    ) * 100,\n                    {round_digits}\n                ) AS PL\n\n            FROM read_parquet('{path_analysis.as_posix()}')\n\n            WHERE entra_calculo_pl = TRUE\n              AND factor_valido = TRUE\n              AND fac_tri IS NOT NULL\n              AND fac_tri > 0\n            """).df()
    finally:
        con.close()
    return resultado

def compute_pl_time_series(analysis_sources, periods=None, start_period=None, end_period=None, lpei_quarterly=None, path_out='../Resultados_PL', output_name=None, save_csv=True, save_parquet=True, overwrite=False, round_digits=2, stop_on_error=False, require_complete=False, event_callback=None, metadata_callback=None, paradata_callback=None, strict_hooks=False, *, generate_metadata=False, path_paradata='../Paradata', excluded_periods=None):
    """
    Calcula una serie temporal de pobreza laboral a partir de
    Parquet analíticos trimestrales.

    La función llama a compute_pl_period() para cada trimestre y
    construye un DataFrame ordenado con las columnas:

        Año, Trimestre, PL

    Parámetros
    ----------
    analysis_sources : dict, list, tuple, str o pathlib.Path
        Fuentes de los Parquet analíticos.

        Formatos admitidos:

        1. Diccionario de periodo y ruta:
            {
                "2025T1": ruta_2025t1,
                "2025T2": ruta_2025t2
            }

        2. Diccionario con resultados de funciones anteriores:
            {
                "2025T1": {
                    "path_output": ruta_2025t1
                }
            }

        3. Lista de rutas:
            [
                ruta_2025t1,
                ruta_2025t2
            ]

        4. Lista de pares periodo-ruta:
            [
                ("2025T1", ruta_2025t1),
                ("2025T2", ruta_2025t2)
            ]

        5. Carpeta raíz:
            "../Analisis_PL_parquet"

        Cuando recibe una carpeta, busca recursivamente archivos
        con el patrón:

            ENOE_*_analisis_pl.parquet

    periods : list, opcional
        Lista explícita de periodos que se desean calcular.

        Ejemplo:
            ["2025T1", "2025T2", "2026T1"]

    start_period : str, opcional
        Primer periodo de un rango trimestral inclusivo.

    end_period : str, opcional
        Último periodo de un rango trimestral inclusivo.

        Ejemplo:
            start_period="2025T1"
            end_period="2026T1"

    lpei_quarterly : pandas.DataFrame, opcional
        DataFrame trimestral de la LPEI utilizado como control
        metodológico por compute_pl_period().

    path_out : str o pathlib.Path
        Carpeta para guardar los resultados.

    output_name : str, opcional
        Nombre base del CSV y Parquet de salida.

    save_csv : bool, default=True
        Guarda la serie en CSV.

    save_parquet : bool, default=True
        Guarda la serie en Parquet.

    overwrite : bool, default=False
        Permite reemplazar archivos existentes.

    round_digits : int, default=2
        Número de decimales del indicador PL.

    stop_on_error : bool, default=False
        Si es True, detiene el proceso al primer error.

        Si es False, registra el error y continúa con los demás
        periodos.

    require_complete : bool, default=False
        Si es True, la ausencia de cualquier periodo solicitado
        detiene la ejecución.

    event_callback : callable, opcional
        Mensajero para eventos de procesamiento.

    metadata_callback : callable, opcional
        Mensajero para metadatos.

    paradata_callback : callable, opcional
        Mensajero para paradatos o logs de procesamiento.

    strict_hooks : bool, default=False
        Si es True, los errores de los callbacks detienen la
        ejecución.

    Retorna
    -------
    dict
        Diccionario con:

        data
            DataFrame con Año, Trimestre y PL.

        audit
            DataFrame de auditoría por periodo.

        path_csv
            Ruta del CSV generado.

        path_parquet
            Ruta del Parquet generado.

        metadata
            Bosquejo de metadatos.

        paradata
            Bosquejo del historial de procesamiento.

        events
            Eventos registrados.

        OK
            True si no ocurrieron errores de procesamiento.

        completo
            True si se procesaron todos los periodos solicitados.
    """
    excluded_periods_list = [] if excluded_periods is None else [str(p) for p in excluded_periods] if isinstance(excluded_periods, (list, tuple, set)) else [str(excluded_periods)]
    from pathlib import Path
    from datetime import datetime, timezone
    import re
    import time
    import warnings
    import pandas as pd

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
        Construye una llave cronológica para ordenar periodos.
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
        Extrae el periodo AAAATX desde una ruta.
        """
        match = re.search('(\\d{4}T[1-4])', str(path).upper())
        if not match:
            raise ValueError(f'No fue posible inferir el periodo desde la ruta:\n{path}')
        return normalize_period(match.group(1))

    def extract_path(value):
        """
        Extrae una ruta desde una estructura admitida.
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
            raise TypeError('analysis_sources debe ser un diccionario, una colección de rutas o una carpeta.')
        return dict(sorted(normalized.items(), key=lambda item: period_sort_key(item[0])))

    def unique_ordered(values):
        """
        Elimina valores repetidos conservando el orden.
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
        Registra un evento del procesamiento.
        """
        payload = {'timestamp_utc': datetime.now(timezone.utc).isoformat(), 'funcion': 'compute_pl_time_series', 'periodo': period, 'etapa': stage, 'estado': status, 'mensaje': message, 'metricas': metrics or {}}
        events.append(payload)
        send_callback(event_callback, payload, 'event_callback')
    started_at = datetime.now(timezone.utc)
    execution_start = time.perf_counter()
    emit_event(stage='inicio', status='ok', message='Inició el cálculo de la serie temporal de pobreza laboral.')
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
        raise ValueError('No se proporcionaron periodos para procesar.')
    missing_periods = [period for period in requested_periods if period not in source_map]
    warning_records = []
    error_records = []
    for period in missing_periods:
        message = f'No se encontró un Parquet analítico para {period}.'
        warning_record = {'periodo': period, 'tipo': 'periodo_faltante', 'mensaje': message}
        warning_records.append(warning_record)
        warnings.warn(message, RuntimeWarning, stacklevel=2)
        emit_event(stage='validacion_fuentes', status='warning', message=message, period=period)
    if require_complete and missing_periods:
        raise FileNotFoundError(f'Faltan los siguientes periodos solicitados: {missing_periods}')
    first_requested = requested_periods[0]
    last_requested = requested_periods[-1]
    if output_name is None:
        output_name = f'serie_pobreza_laboral_{first_requested}_{last_requested}'
    output_name = Path(str(output_name)).stem
    path_csv = None
    path_parquet = None
    if save_csv or save_parquet:
        output_folder = Path(path_out)
        output_folder.mkdir(parents=True, exist_ok=True)
        if save_csv:
            path_csv = output_folder / f'{output_name}.csv'
        if save_parquet:
            path_parquet = output_folder / f'{output_name}.parquet'
        existing_outputs = [path for path in [path_csv, path_parquet] if path is not None and path.exists()]
        if existing_outputs and (not overwrite):
            raise FileExistsError('Ya existen archivos de salida:\n' + '\n'.join((str(path) for path in existing_outputs)) + '\nUtiliza overwrite=True para reemplazarlos.')
    result_frames = []
    audit_records = []
    period_metrics = {}
    periods_to_process = [period for period in requested_periods if period in source_map]
    total_to_process = len(periods_to_process)
    for position, period in enumerate(periods_to_process, start=1):
        path_analysis = Path(source_map[period])
        period_start = time.perf_counter()
        period_started_at_iso = _utc_now_iso()
        audit_record = {'periodo': period, 'path_analysis': str(path_analysis), 'estado': None, 'PL': None, 'duracion_segundos': None, 'error_tipo': None, 'error_mensaje': None}
        emit_event(stage='calculo_periodo', status='inicio', message=f'Procesando {period} ({position}/{total_to_process}).', period=period)
        try:
            if not path_analysis.exists():
                raise FileNotFoundError(f'No existe el Parquet analítico:\n{path_analysis}')
            period_result = compute_pl_period(periodo=period, path_analysis=path_analysis, lpei_quarterly=lpei_quarterly, round_digits=round_digits)
            expected_columns = {'Año', 'Trimestre', 'PL'}
            if not expected_columns.issubset(set(period_result.columns)):
                raise KeyError(f'compute_pl_period() no devolvió las columnas esperadas para {period}.')
            if len(period_result) != 1:
                raise ValueError(f'compute_pl_period() devolvió {len(period_result)} filas para {period}; se esperaba exactamente una.')
            result_row = period_result[['Año', 'Trimestre', 'PL']].copy()
            result_frames.append(result_row)
            pl_value = float(result_row.iloc[0]['PL'])
            if generate_metadata:
                try:
                    import duckdb
                    con_p = duckdb.connect(database=':memory:')
                    metrics_p = con_p.execute(f"\n                        SELECT\n                            SUM(CASE WHEN entra_calculo_pl AND factor_valido THEN 1 ELSE 0 END) AS sample_eligible_records,\n                            SUM(CASE WHEN entra_calculo_pl AND factor_valido AND pobreza_laboral_persona THEN 1 ELSE 0 END) AS sample_poverty_records,\n                            SUM(CASE WHEN entra_calculo_pl AND factor_valido THEN fac_tri ELSE 0 END) AS expanded_eligible_population,\n                            SUM(CASE WHEN entra_calculo_pl AND factor_valido AND pobreza_laboral_persona THEN fac_tri ELSE 0 END) AS expanded_poverty_population\n                        FROM read_parquet('{path_analysis.as_posix()}')\n                        ").fetchone()
                    con_p.close()
                    sample_eligible_p = int(metrics_p[0] or 0)
                    sample_poverty_p = int(metrics_p[1] or 0)
                    expanded_eligible_p = float(metrics_p[2] or 0)
                    expanded_poverty_p = float(metrics_p[3] or 0)
                    p_hat = expanded_poverty_p / expanded_eligible_p if expanded_eligible_p > 0 else 0.0
                    n_eff = sample_eligible_p if sample_eligible_p > 0 else 1
                    std_error = math.sqrt(p_hat * (1 - p_hat) / n_eff) if n_eff > 0 else 0.0
                    cv_pct = std_error / (pl_value / 100.0) * 100 if pl_value > 0 else None
                    ci_lower = pl_value - 1.96 * std_error * 100
                    ci_upper = pl_value + 1.96 * std_error * 100
                    period_metrics[period] = {'periodo': period, 'anio': int(period[:4]), 'trimestre': int(period[-1]), 'PL': pl_value, 'expanded_eligible_population': expanded_eligible_p, 'expanded_poverty_population': expanded_poverty_p, 'sample_eligible_records': sample_eligible_p, 'sample_poverty_records': sample_poverty_p, 'standard_error_approx': std_error * 100, 'coefficient_of_variation_approx_pct': cv_pct, 'confidence_interval_95_approx': [ci_lower, ci_upper], 'quality_flag': None}
                except Exception:
                    period_metrics[period] = {'periodo': period, 'anio': int(period[:4]), 'trimestre': int(period[-1]), 'PL': pl_value, 'expanded_eligible_population': None, 'expanded_poverty_population': None, 'sample_eligible_records': None, 'sample_poverty_records': None, 'standard_error_approx': None, 'coefficient_of_variation_approx_pct': None, 'confidence_interval_95_approx': None, 'quality_flag': None}
            duration = time.perf_counter() - period_start
            audit_record.update({'estado': 'procesado', 'PL': pl_value, 'duracion_segundos': duration})
            emit_event(stage='calculo_periodo', status='ok', message=f'Se calculó PL para {period}.', period=period, metrics={'PL': pl_value, 'duracion_segundos': duration})
            if generate_metadata:
                period_step = {'step_number': None, 'step_name': 'compute_poverty_labor_period', 'function_name': 'compute_pl_period', 'transformation_description': 'Cálculo del índice de pobreza laboral del periodo a partir del Parquet analítico.', 'status': 'ok', 'started_at_utc': period_started_at_iso, 'finished_at_utc': _utc_now_iso(), 'duration_seconds': duration, 'input': {'object_name': path_analysis.name, 'object_type': 'parquet', 'path': str(path_analysis)}, 'parameters': {'periodo': period, 'round_digits': round_digits}, 'output': {'object_name': f'pl_{period}', 'object_type': 'pandas.DataFrame', 'rows': 1, 'columns': ['Año', 'Trimestre', 'PL']}, 'execution_summary': {'periodo': period, 'PL': pl_value}, 'warnings': [], 'errors': []}
                _append_period_paradata_step(period, period_step, path_paradata)
        except Exception as error:
            duration = time.perf_counter() - period_start
            error_record = {'periodo': period, 'path_analysis': str(path_analysis), 'tipo': type(error).__name__, 'mensaje': str(error)}
            error_records.append(error_record)
            audit_record.update({'estado': 'error', 'duracion_segundos': duration, 'error_tipo': type(error).__name__, 'error_mensaje': str(error)})
            warnings.warn(f'Error al procesar {period}: {type(error).__name__}: {error}', RuntimeWarning, stacklevel=2)
            emit_event(stage='calculo_periodo', status='error', message=str(error), period=period, metrics={'error_tipo': type(error).__name__, 'duracion_segundos': duration})
            if stop_on_error:
                raise
        finally:
            audit_records.append(audit_record)
    if not result_frames:
        raise RuntimeError('No fue posible calcular ningún periodo.')
    time_series_df = pd.concat(result_frames, ignore_index=True)
    time_series_df['Año'] = pd.to_numeric(time_series_df['Año'], errors='raise').astype('int64')
    time_series_df['Trimestre'] = pd.to_numeric(time_series_df['Trimestre'], errors='raise').astype('int64')
    time_series_df['PL'] = pd.to_numeric(time_series_df['PL'], errors='raise').astype('float64')
    time_series_df = time_series_df.sort_values(by=['Año', 'Trimestre']).reset_index(drop=True)
    duplicated_periods = int(time_series_df.duplicated(subset=['Año', 'Trimestre']).sum())
    if duplicated_periods > 0:
        raise ValueError(f'La serie calculada contiene {duplicated_periods} periodos duplicados.')
    audit_df = pd.DataFrame(audit_records)
    if not audit_df.empty:
        audit_df['anio_orden'] = audit_df['periodo'].str[:4].astype(int)
        audit_df['trimestre_orden'] = audit_df['periodo'].str[-1].astype(int)
        audit_df = audit_df.sort_values(by=['anio_orden', 'trimestre_orden']).drop(columns=['anio_orden', 'trimestre_orden']).reset_index(drop=True)
    if path_csv is not None:
        temp_csv = path_csv.with_name(f'{path_csv.stem}.tmp.csv')
        if temp_csv.exists():
            temp_csv.unlink()
        time_series_df.to_csv(temp_csv, index=False, encoding='utf-8-sig')
        if path_csv.exists():
            path_csv.unlink()
        temp_csv.replace(path_csv)
    if path_parquet is not None:
        temp_parquet = path_parquet.with_name(f'{path_parquet.stem}.tmp.parquet')
        if temp_parquet.exists():
            temp_parquet.unlink()
        time_series_df.to_parquet(temp_parquet, index=False)
        if path_parquet.exists():
            path_parquet.unlink()
        temp_parquet.replace(path_parquet)
    emit_event(stage='escritura_resultados', status='ok', message='Se escribieron las salidas de la serie temporal.', metrics={'filas_resultado': len(time_series_df), 'path_csv': str(path_csv) if path_csv is not None else None, 'path_parquet': str(path_parquet) if path_parquet is not None else None})
    finished_at = datetime.now(timezone.utc)
    duration_total = time.perf_counter() - execution_start
    processed_periods = audit_df.loc[audit_df['estado'].eq('procesado'), 'periodo'].tolist() if not audit_df.empty else []
    failed_periods = audit_df.loc[audit_df['estado'].eq('error'), 'periodo'].tolist() if not audit_df.empty else []
    complete = len(missing_periods) == 0 and len(failed_periods) == 0 and (len(processed_periods) == len(requested_periods))
    metadata = {'function': 'compute_pl_time_series', 'function_version': '0.1.0', 'created_at_utc': finished_at.isoformat(), 'columnas_salida': ['Año', 'Trimestre', 'PL'], 'tipo_indicador': 'porcentaje_personas_pobreza_laboral', 'unidad': 'porcentaje', 'decimales': round_digits, 'periodo_inicial': first_requested, 'periodo_final': last_requested, 'path_csv': str(path_csv) if path_csv is not None else None, 'path_parquet': str(path_parquet) if path_parquet is not None else None}
    paradata = {'function': 'compute_pl_time_series', 'started_at_utc': started_at.isoformat(), 'finished_at_utc': finished_at.isoformat(), 'duration_seconds': duration_total, 'periodos_disponibles': available_periods, 'periodos_solicitados': requested_periods, 'periodos_procesados': processed_periods, 'periodos_faltantes': missing_periods, 'periodos_con_error': failed_periods, 'warnings': warning_records, 'errors': error_records, 'audit_records': audit_records, 'salidas': {'csv': str(path_csv) if path_csv is not None else None, 'parquet': str(path_parquet) if path_parquet is not None else None}}
    send_callback(metadata_callback, metadata, 'metadata_callback')
    send_callback(paradata_callback, paradata, 'paradata_callback')
    emit_event(stage='fin', status='ok' if complete else 'warning', message='Terminó el cálculo de la serie temporal de pobreza laboral.', metrics={'periodos_solicitados': len(requested_periods), 'periodos_procesados': len(processed_periods), 'periodos_faltantes': len(missing_periods), 'periodos_con_error': len(failed_periods), 'duracion_segundos': duration_total})
    if generate_metadata:
        try:
            out_folder = Path(path_out)
            out_folder.mkdir(parents=True, exist_ok=True)
            if output_name:
                series_base = output_name
            else:
                series_base = f'serie_pobreza_laboral_{first_requested}_{last_requested}'
            metadata_path = out_folder / f'{series_base}_metadata_quality.json'
            general_information = {'dataset_family': 'ENOE', 'dataset_level': 'series', 'indicator_name': 'pobreza_laboral', 'project_name': 'Calculo de pobreza laboral', 'function': 'compute_pl_time_series', 'period_range': {'first': first_requested, 'last': last_requested}, 'created_at_utc': finished_at.isoformat(), 'updated_at_utc': finished_at.isoformat(), 'notes': ['La serie se calcula como porcentaje de personas en pobreza laboral por trimestre.', 'Los valores de PL se aproximan sin considerar diseño muestral complejo.']}
            source_data = {'analysis_files': [str(source_map[p]) for p in processed_periods]}
            construction_method = {'steps': ['Para cada periodo: se lee el Parquet analítico, se calcula el indicador PL mediante compute_pl_period() y se registran las métricas.', 'Se integran los resultados de todos los periodos en un DataFrame ordenado y se guardan en CSV y/o Parquet.', 'Se calcula la tasa de cobertura de periodos y se estiman errores estándar de manera aproximada.'], 'parameters': {'round_digits': round_digits, 'save_csv': save_csv, 'save_parquet': save_parquet}}
            ds_info = _read_parquet_schema_info(str(path_parquet)) if path_parquet is not None else None
            dataset_size = {'rows': len(time_series_df), 'columns': len(time_series_df.columns), 'file_size_mb': _safe_file_size_mb(str(path_parquet)) if path_parquet is not None else None, 'row_groups': ds_info.get('row_groups') if ds_info else None}
            schema_columns = {}
            for col in time_series_df.columns:
                schema_columns[col] = {'type': str(time_series_df[col].dtype), 'label': col, 'description': f'Columna {col} de la serie temporal', 'unit': 'porcentaje' if col == 'PL' else None, 'role': 'resultado' if col == 'PL' else 'dimension', 'source': 'Analisis_PL_parquet', 'used_in': ['compute_pl_time_series']}
            schema = {'column_count': len(schema_columns), 'columns': schema_columns}
            expected_periods_count = len(requested_periods)
            processed_count = len(processed_periods)
            excluded_periods_auto = [p for p in requested_periods if p not in processed_periods]
            missing_unexpected = [p for p in available_periods if p not in requested_periods]
            coverage_rate = processed_count / expected_periods_count * 100 if expected_periods_count > 0 else None
            series_quality = {'expected_periods': expected_periods_count, 'processed_periods': processed_count, 'excluded_periods': excluded_periods_auto, 'missing_unexpected_periods': missing_unexpected, 'period_coverage_rate_pct': coverage_rate}
            period_indicators = list(period_metrics.values()) if period_metrics else []
            quality_indicator_dictionary = _build_quality_indicator_dictionary('series')
            quality_indicators = {'series_quality': series_quality, 'period_indicators': period_indicators}
            methodological_observations = ['Los errores estándar y los intervalos de confianza se estiman de manera interna sin considerar el diseño muestral completo (aproximacion_interna_sin_diseno_muestral_completo).', 'Los periodos excluidos se deben a la ausencia del Parquet analítico correspondiente o a errores durante el procesamiento.']
            series_metadata = {'general_information': general_information, 'source_data': source_data, 'construction_method': construction_method, 'dataset_size': dataset_size, 'schema': schema, 'critical_variables': [], 'quality_indicator_dictionary': quality_indicator_dictionary, 'quality_indicators': quality_indicators, 'methodological_observations': methodological_observations}
            _write_json_metadata(series_metadata, metadata_path)
            paradata_base_dir = Path(path_paradata) / 'serie_indicador'
            paradata_base_dir.mkdir(parents=True, exist_ok=True)
            yaml_path = paradata_base_dir / f'paradata_{series_base}.yaml'
            source_items = [{'periodo': p, 'path': _paradata_path_string(source_map[p])} for p in requested_periods if p in source_map]
            series_status = 'ok' if not failed_periods and (not missing_periods) else 'warning'
            methodological_notes = []
            if excluded_periods_list:
                methodological_notes.append('El periodo 2020T2 se excluye por decisión metodológica asociada al quiebre COVID/ETOE.' if '2020T2' in excluded_periods_list else 'Periodos excluidos por decisión metodológica: ' + ', '.join(excluded_periods_list))
            normalize_step = {'step_number': 1, 'step_name': 'normalize_analysis_sources', 'function_name': 'compute_pl_time_series', 'status': 'ok', 'started_at_utc': started_at.isoformat(), 'finished_at_utc': started_at.isoformat(), 'duration_seconds': 0.0, 'input': {'analysis_sources': {'object_type': 'python_dict', 'total_sources': len(source_items), 'sources': source_items}}, 'parameters': {'periods': list(requested_periods)}, 'output': {'normalized_sources': len(source_items)}, 'execution_summary': {'requested_periods': len(requested_periods)}, 'warnings': [], 'errors': []}
            compute_step = {'step_number': 2, 'step_name': 'compute_pl_by_period', 'function_name': 'compute_pl_time_series', 'status': series_status, 'started_at_utc': started_at.isoformat(), 'finished_at_utc': finished_at.isoformat(), 'duration_seconds': duration_total, 'input': {'periods': list(requested_periods)}, 'parameters': {'round_digits': round_digits, 'stop_on_error': stop_on_error}, 'output': {'object_type': 'pandas.DataFrame', 'rows': len(time_series_df), 'columns': list(time_series_df.columns)}, 'execution_summary': {'processed_periods': len(processed_periods), 'failed_periods': len(failed_periods)}, 'warnings': list(missing_periods), 'errors': list(failed_periods)}
            write_step = {'step_number': 3, 'step_name': 'write_indicator_outputs', 'function_name': 'compute_pl_time_series', 'status': 'ok', 'started_at_utc': finished_at.isoformat(), 'finished_at_utc': finished_at.isoformat(), 'duration_seconds': 0.0, 'input': {'rows': len(time_series_df)}, 'parameters': {'save_csv': save_csv, 'save_parquet': save_parquet, 'overwrite': overwrite}, 'output': {'csv': str(path_csv) if path_csv else None, 'parquet': str(path_parquet) if path_parquet else None, 'metadata_json': str(metadata_path)}, 'execution_summary': {'outputs_written': sum((x is not None for x in [path_csv, path_parquet]))}, 'warnings': [], 'errors': []}
            serie_paradata = {'dataset_family': 'ENOE', 'project_name': 'Calculo de pobreza laboral', 'process_name': 'serie_pobreza_laboral', 'paradata_type': 'indicator_time_series_history', 'status_general': series_status, 'created_at_utc': started_at.isoformat(), 'updated_at_utc': finished_at.isoformat(), 'summary': {'total_duration_seconds': duration_total, 'total_periods_requested_for_processing': len(requested_periods), 'total_periods_processed': len(processed_periods), 'excluded_periods': excluded_periods_list, 'failed_periods': len(failed_periods), 'coverage_rate_pct': coverage_rate, 'final_outputs': [x for x in [str(path_csv) if path_csv else None, str(path_parquet) if path_parquet else None] if x]}, 'periods': {'requested': list(requested_periods), 'processed': list(processed_periods), 'excluded': excluded_periods_list, 'missing': list(missing_periods), 'errors': list(failed_periods)}, 'methodological_notes': methodological_notes, 'steps': [normalize_step, compute_step, write_step]}
            _write_yaml_paradata(serie_paradata, yaml_path)
            metadata = series_metadata
            paradata = serie_paradata
        except Exception:
            pass
    result = {'data': time_series_df, 'audit': audit_df, 'path_csv': path_csv, 'path_parquet': path_parquet, 'metadata': metadata, 'paradata': paradata, 'events': events, 'OK': len(error_records) == 0, 'completo': complete}
    return result
