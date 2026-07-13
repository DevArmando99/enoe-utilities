"""Helpers internos para metadatos JSON y paradatos YAML."""
from pathlib import Path
from datetime import datetime, timezone
import json
import math
import os
import warnings
import pyarrow.parquet as pq
import yaml
from .utils import _paradata_path_string

def _utc_now_iso():
    """Devuelve la fecha y hora actual en formato ISO 8601 UTC."""
    return datetime.now(timezone.utc).isoformat()

def _safe_file_size_mb(path):
    """Intenta obtener el tamaño de un archivo en megabytes."""
    try:
        return os.path.getsize(path) / 1024 / 1024
    except Exception:
        return None

def _safe_file_size_gb(path):
    """Intenta obtener el tamaño de un archivo en gigabytes."""
    mb = _safe_file_size_mb(path)
    return mb / 1024 if mb is not None else None

def _safe_rate_pct(numerator, denominator):
    """Calcula una tasa porcentual segura para metadatos."""
    try:
        if denominator is None or float(denominator) == 0:
            return None
        if numerator is None:
            return None
        return float(numerator) / float(denominator) * 100
    except Exception:
        return None

def _sql_identifier(column_name):
    """Protege nombres de columnas para consultas SQL auxiliares."""
    return '"' + str(column_name).replace('"', '""') + '"'

def _resolve_metadata_physical_column(column_names, canonical_column, equivalences=None):
    """
    Resuelve una columna física para metadatos sin alterar el cálculo.
    Se usa únicamente para métricas documentales de calidad.
    """
    equivalences = equivalences or {}
    normalized_to_raw = {str(col).strip().lower(): col for col in column_names or []}
    candidates = equivalences.get(canonical_column, [canonical_column])
    if canonical_column not in candidates:
        candidates = [canonical_column, *candidates]
    for candidate in candidates:
        key = str(candidate).strip().lower()
        if key in normalized_to_raw:
            return normalized_to_raw[key]
    return None

def _read_parquet_schema_info(path):
    """
    Lee información básica del esquema de un Parquet sin cargar sus filas.

    Retorna un diccionario con:
        rows: Número de filas
        columns: Número de columnas
        row_groups: Número de row groups
        column_names: Lista de nombres de columnas
        column_types: Lista de tipos Arrow en cadena
    Si ocurre un error al leer el esquema, devuelve None.
    """
    try:
        pf = pq.ParquetFile(path)
        schema = pf.schema_arrow
        return {'rows': pf.metadata.num_rows, 'columns': len(schema.names), 'row_groups': pf.metadata.num_row_groups, 'column_names': schema.names, 'column_types': [str(schema.field(n).type) for n in schema.names]}
    except Exception:
        return None

def _write_json_metadata(metadata, path):
    """Escribe un diccionario como JSON UTF-8 con indentación."""
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _to_paradata_serializable(obj):
    """Convierte recursivamente objetos a tipos YAML estándar."""
    from datetime import date, datetime
    try:
        import numpy as np
    except Exception:
        np = None
    try:
        import pandas as pd
    except Exception:
        pd = None
    if np is not None:
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            value = float(obj)
            return None if math.isnan(value) else value
        if isinstance(obj, np.bool_):
            return bool(obj)
    if obj is None or isinstance(obj, (str, bool, int, float)):
        if isinstance(obj, float) and math.isnan(obj):
            return None
        return obj
    if isinstance(obj, Path):
        return _paradata_path_string(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if pd is not None:
        try:
            missing = pd.isna(obj)
            if isinstance(missing, bool) and missing:
                return None
        except Exception:
            pass
    if isinstance(obj, dict):
        return {str(k): _to_paradata_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_to_paradata_serializable(v) for v in obj]
    return str(obj)

def _write_yaml_paradata(paradata, path):
    """Escribe paradatos YAML estándar mediante reemplazo atómico."""
    path = Path(path)
    temp_path = path.with_suffix(path.suffix + '.tmp')
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        serializable = _to_paradata_serializable(paradata)
        with open(temp_path, 'w', encoding='utf-8') as f:
            yaml.safe_dump(serializable, f, allow_unicode=True, sort_keys=False)
        with open(temp_path, 'r', encoding='utf-8') as f:
            yaml.safe_load(f)
        temp_path.replace(path)
        return True
    except Exception as error:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass
        warnings.warn(f'No fue posible escribir el paradato YAML {path}: {type(error).__name__}: {error}', RuntimeWarning, stacklevel=2)
        return False

def _load_yaml_if_exists(path):
    """Carga YAML; distingue archivo inexistente de archivo inválido."""
    path = Path(path)
    if not path.exists():
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}
    except Exception as error:
        warnings.warn(f'El paradato YAML existente no pudo leerse y no será sobrescrito: {path}. {type(error).__name__}: {error}', RuntimeWarning, stacklevel=2)
        return False

def _parse_iso_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None

def _extract_paradata_output_paths(output_obj):
    """Extrae rutas documentales de una salida, conservando el orden."""
    paths = []
    interesting = {'path', 'files', 'objects', 'csv', 'parquet', 'historical_parquet', 'manifest', 'metadata', 'metadata_json'}

    def visit(value, key=None):
        if value is None:
            return
        if isinstance(value, dict):
            for k, v in value.items():
                if str(k) in interesting or key in interesting:
                    visit(v, str(k))
                elif isinstance(v, (dict, list, tuple, set)):
                    visit(v, str(k))
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                visit(item, key)
            return
        if key in interesting or isinstance(value, Path):
            text = _paradata_path_string(value)
            if text and text.strip():
                paths.append(text)
    visit(output_obj)
    deduped = []
    seen = set()
    for item in paths:
        norm = item.replace('\\', '/')
        if norm not in seen:
            seen.add(norm)
            deduped.append(norm)
    return deduped

def _append_period_paradata_step(periodo, step_info, path_paradata, initial_notes=None):
    """Agrega de forma segura un paso al historial YAML de un periodo."""
    base_dir = Path(path_paradata)
    period_dir = base_dir / str(periodo)
    period_dir.mkdir(parents=True, exist_ok=True)
    yaml_path = period_dir / f'paradata_{periodo}.yaml'
    existing = _load_yaml_if_exists(yaml_path)
    if existing is False:
        return None
    new_step = dict(step_info or {})
    new_step.setdefault('warnings', [])
    new_step.setdefault('errors', [])
    status = str(new_step.get('status') or 'warning').strip().lower()
    valid_statuses = {'ok', 'reused', 'warning', 'error', 'skipped'}
    if status not in valid_statuses:
        new_step.setdefault('warnings', []).append({'warning_type': 'unknown_step_status', 'message': f'Estado desconocido: {status}'})
        status = 'warning'
    new_step['status'] = status
    if existing is None:
        existing = {'periodo': str(periodo), 'dataset_family': 'ENOE', 'project_name': 'Calculo de pobreza laboral', 'paradata_type': 'period_processing_history', 'status_general': 'ok', 'created_at_utc': new_step.get('started_at_utc'), 'updated_at_utc': new_step.get('finished_at_utc'), 'summary': {}, 'methodological_notes': list(initial_notes or []), 'steps': []}
    else:
        notes = list(existing.get('methodological_notes') or [])
        for note in initial_notes or []:
            if note not in notes:
                notes.append(note)
        existing['methodological_notes'] = notes
    steps = list(existing.get('steps') or [])
    steps.append(new_step)
    for index, step in enumerate(steps, start=1):
        step['step_number'] = index
        step.setdefault('warnings', [])
        step.setdefault('errors', [])
    existing['steps'] = steps
    counts = {name: sum((1 for s in steps if s.get('status') == name)) for name in valid_statuses}
    starts = [_parse_iso_datetime(s.get('started_at_utc')) for s in steps]
    ends = [_parse_iso_datetime(s.get('finished_at_utc')) for s in steps]
    starts = [x for x in starts if x is not None]
    ends = [x for x in ends if x is not None]
    first_dt = min(starts) if starts else None
    last_dt = max(ends) if ends else None
    total_duration = (last_dt - first_dt).total_seconds() if first_dt and last_dt else None
    sum_duration = 0.0
    has_duration = False
    for step in steps:
        value = step.get('duration_seconds')
        try:
            if value is not None:
                sum_duration += float(value)
                has_duration = True
        except Exception:
            pass
    final_outputs = []
    seen = set()
    for step in steps:
        for item in _extract_paradata_output_paths(step.get('output')):
            norm = item.replace('\\', '/')
            if norm not in seen:
                seen.add(norm)
                final_outputs.append(norm)
    summary = {'total_steps': len(steps), 'successful_steps': counts['ok'] + counts['reused'], 'ok_steps': counts['ok'], 'reused_steps': counts['reused'], 'warning_steps': counts['warning'], 'error_steps': counts['error'], 'skipped_steps': counts['skipped'], 'total_duration_seconds': total_duration, 'sum_step_duration_seconds': sum_duration if has_duration else None, 'first_step_started_at_utc': first_dt.isoformat() if first_dt else None, 'last_step_finished_at_utc': last_dt.isoformat() if last_dt else None, 'final_outputs': final_outputs}
    existing['summary'] = summary
    if not existing.get('created_at_utc'):
        existing['created_at_utc'] = summary['first_step_started_at_utc']
    existing['updated_at_utc'] = summary['last_step_finished_at_utc'] or _utc_now_iso()
    existing['status_general'] = 'error' if summary['error_steps'] > 0 else 'warning' if summary['warning_steps'] > 0 else 'ok'
    return existing if _write_yaml_paradata(existing, yaml_path) else None

def _append_required_columns_validation_paradata(validation_result, path_paradata, initial_notes=None):
    """Registra en YAML el DataFrame ya calculado por validate_required_columns()."""
    try:
        if validation_result is None or getattr(validation_result, 'empty', True):
            return {}
        outputs = {}
        for periodo, group in validation_result.groupby('Periodo', sort=True):
            records = group.to_dict(orient='records')
            ok_all = all((bool(row.get('OK')) for row in records))
            missing = [row for row in records if not bool(row.get('OK'))]
            step = {'step_number': None, 'step_name': 'required_columns_validation', 'function_name': 'validate_required_columns', 'transformation_description': 'Registro del resultado ya calculado de validación de columnas requeridas.', 'status': 'ok' if ok_all else 'warning', 'started_at_utc': _utc_now_iso(), 'finished_at_utc': _utc_now_iso(), 'duration_seconds': 0.0, 'input': {'tables': [row.get('Tabla') for row in records]}, 'parameters': {}, 'output': {'object_type': 'pandas.DataFrame', 'rows': len(records)}, 'execution_summary': {'tables': records, 'all_ok': ok_all}, 'warnings': missing, 'errors': []}
            outputs[str(periodo)] = _append_period_paradata_step(str(periodo), step, path_paradata, initial_notes)
        return outputs
    except Exception as error:
        warnings.warn(f'No fue posible registrar la validación de columnas en paradatos: {error}', RuntimeWarning, stacklevel=2)
        return {}

def _append_merged_validation_paradata(periodo, validation_result, path_paradata, initial_notes=None):
    """Registra en YAML el resultado ya calculado por validate_merged_period()."""
    try:
        result = dict(validation_result or {})
        ok = bool(result.get('OK'))
        errors = list(result.get('errores') or [])
        warns = list(result.get('advertencias') or [])
        status = 'error' if errors else 'warning' if warns or not ok else 'ok'
        step = {'step_number': None, 'step_name': 'merged_parquet_validation', 'function_name': 'validate_merged_period', 'transformation_description': 'Registro del resultado ya calculado de validación del Parquet unido.', 'status': status, 'started_at_utc': _utc_now_iso(), 'finished_at_utc': _utc_now_iso(), 'duration_seconds': 0.0, 'input': {'path': result.get('path_merged')}, 'parameters': {}, 'output': {'object_type': 'validation_result', 'path': result.get('path_merged')}, 'execution_summary': result, 'warnings': warns, 'errors': errors}
        return _append_period_paradata_step(str(periodo), step, path_paradata, initial_notes)
    except Exception as error:
        warnings.warn(f'No fue posible registrar la validación del merge en paradatos: {error}', RuntimeWarning, stacklevel=2)
        return None

def _build_column_dictionary(columns, dataset_type='raw'):
    """
    Construye un diccionario enriquecido por columna con información
    descriptiva. Este helper no pretende ser exhaustivo; se incluye
    documentación relevante para las columnas principales del pipeline.

    Parámetros
    ----------
    columns : list
        Lista de nombres de columnas para las que se construirá el
        diccionario.

    dataset_type : str
        Tipo de dataset que se documenta (raw, merge, analysis, series,
        historical). Puede afectar las etiquetas y roles predeterminados.

    Retorna
    -------
    dict
        Diccionario con claves igual al nombre de la columna y valores
        con campos: label, description, source, type, unit, role, used_in.
    """
    catalog = {'cd_a': {'label': 'Clave de entidad federativa - A', 'description': 'Identificador de entidad federativa en la Encuesta.', 'source': 'SDEM', 'unit': None, 'role': 'llave_hogar', 'used_in': ['merge_sdem_coe2', 'build_analysis_dataset', 'validate_merged_period']}, 'cve_ent': {'label': 'Clave de entidad federativa', 'description': 'Código de la entidad federativa donde reside la persona.', 'source': 'SDEM', 'unit': None, 'role': 'territorio', 'used_in': ['resolve_parquet_columns', 'validate_required_columns', 'merge_period_parquet', 'build_analysis_dataset', 'validate_merged_period']}, 'ent': {'label': 'Clave de entidad federativa histórica', 'description': 'Nombre físico histórico de la clave de entidad federativa. Se usa como equivalente de cve_ent cuando corresponde.', 'source': 'SDEM', 'unit': None, 'role': 'territorio_equivalente', 'used_in': ['resolve_parquet_columns', 'validate_required_columns', 'build_analysis_dataset', 'schema_quality']}, 'con': {'label': 'Consecutivo', 'description': 'Identificador interno de la vivienda en el listado de viviendas de la muestra.', 'source': 'SDEM', 'unit': None, 'role': 'llave_hogar', 'used_in': ['merge_sdem_coe2', 'build_analysis_dataset']}, 'n_pro_viv': {'label': 'Número de la vivienda en la selección de muestra', 'description': 'Indica el orden de selección de la vivienda dentro del conglomerado.', 'source': 'SDEM', 'unit': None, 'role': 'llave_hogar', 'used_in': ['merge_sdem_coe2', 'build_analysis_dataset']}, 'v_sel': {'label': 'Identificador de vivienda seleccionada', 'description': 'Clave de selección de vivienda dentro del conglomerado.', 'source': 'SDEM', 'unit': None, 'role': 'llave_hogar', 'used_in': ['merge_sdem_coe2', 'build_analysis_dataset']}, 'n_ent': {'label': 'Número de entidad', 'description': 'Número de la entidad federativa de residencia.', 'source': 'SDEM', 'unit': None, 'role': 'llave_hogar', 'used_in': ['merge_sdem_coe2', 'build_analysis_dataset']}, 'n_hog': {'label': 'Número de hogar', 'description': 'Número secuencial del hogar dentro de la vivienda.', 'source': 'SDEM', 'unit': None, 'role': 'llave_hogar', 'used_in': ['merge_sdem_coe2', 'build_analysis_dataset']}, 'h_mud': {'label': 'Indicador de hogar mudado', 'description': 'Variable que indica si el hogar se mudó entre periodos.', 'source': 'SDEM', 'unit': None, 'role': 'llave_hogar', 'used_in': ['build_analysis_dataset']}, 'n_ren': {'label': 'Número de renglón de la persona', 'description': 'Identificador secuencial de la persona dentro del hogar.', 'source': 'SDEM', 'unit': None, 'role': 'llave_persona', 'used_in': ['merge_sdem_coe2', 'build_analysis_dataset']}, 'r_def': {'label': 'Resultado de la entrevista', 'description': 'Identifica si la entrevista fue completa y válida.', 'source': 'SDEM', 'unit': None, 'role': 'residencia', 'used_in': ['build_analysis_dataset', 'validate_merged_period']}, 'c_res': {'label': 'Condición de residencia', 'description': 'Indica si la persona es residente habitual del hogar.', 'source': 'SDEM', 'unit': None, 'role': 'residencia', 'used_in': ['build_analysis_dataset', 'validate_merged_period']}, 'par_c': {'label': 'Parentesco con el jefe del hogar', 'description': 'Relación de parentesco de la persona con el jefe del hogar.', 'source': 'SDEM', 'unit': None, 'role': 'sociodemografico', 'used_in': ['build_analysis_dataset']}, 'sex': {'label': 'Sexo', 'description': 'Sexo de la persona (1=hombre, 2=mujer).', 'source': 'SDEM', 'unit': None, 'role': 'sociodemografico', 'used_in': ['build_analysis_dataset']}, 'eda': {'label': 'Edad', 'description': 'Edad de la persona en años cumplidos.', 'source': 'SDEM', 'unit': 'años', 'role': 'sociodemografico', 'used_in': ['build_analysis_dataset']}, 'n_hij': {'label': 'Número de hijas/os', 'description': 'Número de hijas e hijos reportados por la persona.', 'source': 'SDEM', 'unit': 'personas', 'role': 'sociodemografico', 'used_in': ['build_analysis_dataset']}, 'e_con': {'label': 'Estado conyugal', 'description': 'Estado civil o conyugal de la persona.', 'source': 'SDEM', 'unit': None, 'role': 'sociodemografico', 'used_in': ['build_analysis_dataset']}, 't_loc_tri': {'label': 'Tipo de localidad trimestral', 'description': 'Tipo de localidad trimestral usado para clasificar la residencia en ámbito urbano o rural.', 'source': 'SDEM', 'unit': None, 'role': 'territorio', 'used_in': ['resolve_parquet_columns', 'validate_required_columns', 'merge_period_parquet', 'build_analysis_dataset', 'lpei_quality', 'validate_merged_period']}, 't_loc': {'label': 'Tipo de localidad', 'description': 'Nombre físico histórico del tipo de localidad. Se usa como equivalente de t_loc_tri cuando corresponde.', 'source': 'SDEM', 'unit': None, 'role': 'territorio_equivalente', 'used_in': ['resolve_parquet_columns', 'validate_required_columns', 'build_analysis_dataset', 'lpei_quality']}, 'ambito': {'label': 'Ámbito', 'description': 'Ámbito rural o urbano de acuerdo con la LPEI.', 'source': 'LPEI', 'unit': None, 'role': 'territorio', 'used_in': ['build_analysis_dataset', 'compute_pl_period']}, 'clase2': {'label': 'Condición de actividad de la persona', 'description': 'Clasificación de la situación laboral (ocupado, desocupado, etc.).', 'source': 'SDEM', 'unit': None, 'role': 'laboral', 'used_in': ['build_analysis_dataset', 'validate_merged_period']}, 'pos_ocu': {'label': 'Posición en la ocupación', 'description': 'Tipo de vínculo laboral (asalariado, empleador, cuenta propia).', 'source': 'SDEM', 'unit': None, 'role': 'laboral', 'used_in': ['build_analysis_dataset', 'validate_merged_period']}, 'fac_tri': {'label': 'Factor de expansión trimestral', 'description': 'Ponderador trimestral que permite expandir los registros muestrales a población.', 'source': 'SDEM', 'unit': 'personas', 'role': 'ponderador', 'used_in': ['merge_period_parquet', 'build_analysis_dataset', 'compute_pl_period', 'compute_pl_time_series', 'indicator_quality', 'expansion_factor_quality']}, 'fac': {'label': 'Factor de expansión', 'description': 'Nombre físico histórico del factor de expansión. Se usa como equivalente de fac_tri cuando corresponde.', 'source': 'SDEM', 'unit': 'personas', 'role': 'ponderador_equivalente', 'used_in': ['resolve_parquet_columns', 'validate_required_columns', 'build_analysis_dataset', 'expansion_factor_quality']}, 'salario': {'label': 'Salario mínimo mensual', 'description': 'Valor del salario mínimo vigente en el periodo.', 'source': 'COE2', 'unit': 'pesos mexicanos', 'role': 'ingreso_auxiliar', 'used_in': ['build_analysis_dataset']}, 'p6_9': {'label': 'Ingreso mensual de trabajo no remunerado', 'description': 'Ingreso mensual percibido por trabajo no remunerado.', 'source': 'COE2', 'unit': 'pesos mexicanos', 'role': 'ingreso', 'used_in': ['merge_sdem_coe2', 'build_analysis_dataset']}, 'p6b1': {'label': 'Ingreso laboral mensual por rangos (antes de 2005)', 'description': 'Ingreso laboral por rangos reportado cuando no se dispone de P6B2.', 'source': 'COE2', 'unit': 'pesos mexicanos', 'role': 'ingreso', 'used_in': ['merge_sdem_coe2', 'build_analysis_dataset']}, 'p6b2': {'label': 'Ingreso laboral mensual exacto', 'description': 'Monto mensual de ingreso laboral declarado directamente.', 'source': 'COE2', 'unit': 'pesos mexicanos', 'role': 'ingreso', 'used_in': ['merge_sdem_coe2', 'build_analysis_dataset', 'indicator_quality']}, 'p6c': {'label': 'Ingreso por rangos de salario mínimo', 'description': 'Número de salarios mínimos cuando no hay reporte exacto de ingreso.', 'source': 'COE2', 'unit': 'salarios mínimos', 'role': 'ingreso', 'used_in': ['merge_sdem_coe2', 'build_analysis_dataset']}, 'es_ocupado': {'label': 'Bandera de persona ocupada', 'description': 'Variable binaria que indica si la persona estuvo ocupada.', 'source': 'Derivada', 'unit': '0/1', 'role': 'laboral', 'used_in': ['build_analysis_dataset', 'indicator_quality']}, 'es_sin_pago': {'label': 'Bandera de trabajador sin pago', 'description': 'Variable binaria que indica si el trabajador no recibe pago por su trabajo.', 'source': 'Derivada', 'unit': '0/1', 'role': 'laboral', 'used_in': ['build_analysis_dataset']}, 'p6b2_valido': {'label': 'Bandera de ingreso P6B2 válido', 'description': 'Indica si la variable p6b2 tiene un valor numérico positivo y plausible.', 'source': 'Derivada', 'unit': '0/1', 'role': 'ingreso_auxiliar', 'used_in': ['build_analysis_dataset']}, 'multiplicador_p6c': {'label': 'Multiplicador salarial para imputación', 'description': 'Número de salarios mínimos vigente para imputar ingreso a partir de p6c.', 'source': 'Derivada', 'unit': 'salarios mínimos', 'role': 'ingreso_auxiliar', 'used_in': ['build_analysis_dataset']}, 'p6c_valido': {'label': 'Bandera de P6C válido', 'description': 'Indica si p6c está dentro de un rango válido para imputación.', 'source': 'Derivada', 'unit': '0/1', 'role': 'ingreso_auxiliar', 'used_in': ['build_analysis_dataset']}, 'ingreso_imputado_p6c': {'label': 'Ingreso imputado usando p6c', 'description': 'Ingreso laboral individual imputado a partir del valor de p6c y el salario mínimo.', 'source': 'Derivada', 'unit': 'pesos mexicanos', 'role': 'ingreso', 'used_in': ['build_analysis_dataset']}, 'ingreso_laboral_ind': {'label': 'Ingreso laboral individual observado/imputado', 'description': 'Ingreso laboral individual obtenido directamente o mediante imputación.', 'source': 'Derivada', 'unit': 'pesos mexicanos', 'role': 'ingreso', 'used_in': ['build_analysis_dataset']}, 'fuente_ingreso': {'label': 'Fuente del ingreso laboral', 'description': 'Indica si el ingreso proviene de p6b2, imputación o es no recuperable.', 'source': 'Derivada', 'unit': None, 'role': 'ingreso_auxiliar', 'used_in': ['build_analysis_dataset']}, 'ingreso_no_recuperable': {'label': 'Indicador de ingreso no recuperable', 'description': 'Bandera que indica que el ingreso no pudo recuperarse ni imputarse.', 'source': 'Derivada', 'unit': '0/1', 'role': 'ingreso', 'used_in': ['build_analysis_dataset']}, 'id_hogar': {'label': 'Identificador único del hogar', 'description': 'Clave única del hogar construida a partir de las llaves de vivienda y hogar.', 'source': 'Derivada', 'unit': None, 'role': 'llave_hogar', 'used_in': ['build_analysis_dataset']}, 'id_persona': {'label': 'Identificador único de persona', 'description': 'Clave única de la persona construida a partir de las llaves del hogar y del renglón.', 'source': 'Derivada', 'unit': None, 'role': 'llave_persona', 'used_in': ['build_analysis_dataset', 'build_historical_analysis_parquet']}, 'id_hogar_periodo': {'label': 'Identificador del hogar en el periodo', 'description': 'Clave del hogar concatenada con el periodo trimestral.', 'source': 'Derivada', 'unit': None, 'role': 'llave_hogar', 'used_in': ['build_analysis_dataset', 'build_historical_analysis_parquet']}, 'id_persona_periodo': {'label': 'Identificador de persona en el periodo', 'description': 'Clave única de persona concatenada con el periodo trimestral.', 'source': 'Derivada', 'unit': None, 'role': 'llave_persona', 'used_in': ['build_analysis_dataset', 'build_historical_analysis_parquet']}, 'integrantes_hogar': {'label': 'Número de integrantes del hogar', 'description': 'Cuenta de personas residentes válidas en el hogar.', 'source': 'Derivada', 'unit': 'personas', 'role': 'hogar', 'used_in': ['build_analysis_dataset']}, 'ingreso_hogar_observado': {'label': 'Ingreso laboral observado del hogar', 'description': 'Suma de ingresos laborales individuales observados (p6b2) dentro del hogar.', 'source': 'Derivada', 'unit': 'pesos mexicanos', 'role': 'hogar', 'used_in': ['build_analysis_dataset']}, 'hogar_ingreso_incompleto': {'label': 'Indicador de ingreso incompleto en el hogar', 'description': 'Marca hogares donde no se observaron todos los ingresos laborales de sus integrantes.', 'source': 'Derivada', 'unit': '0/1', 'role': 'hogar', 'used_in': ['build_analysis_dataset']}, 'ingresos_directos_hogar': {'label': 'Número de ingresos directos en el hogar', 'description': 'Número de personas con ingreso p6b2 válido dentro del hogar.', 'source': 'Derivada', 'unit': 'personas', 'role': 'hogar', 'used_in': ['build_analysis_dataset']}, 'ingresos_imputados_hogar': {'label': 'Número de ingresos imputados en el hogar', 'description': 'Número de personas con ingreso imputado por p6c en el hogar.', 'source': 'Derivada', 'unit': 'personas', 'role': 'hogar', 'used_in': ['build_analysis_dataset']}, 'ingresos_no_recuperables_hogar': {'label': 'Número de ingresos no recuperables en el hogar', 'description': 'Número de personas cuya información de ingreso no pudo recuperarse ni imputarse.', 'source': 'Derivada', 'unit': 'personas', 'role': 'hogar', 'used_in': ['build_analysis_dataset']}, 'ingreso_hogar': {'label': 'Ingreso laboral total del hogar', 'description': 'Suma de ingresos observados e imputados de los integrantes del hogar.', 'source': 'Derivada', 'unit': 'pesos mexicanos', 'role': 'hogar', 'used_in': ['build_analysis_dataset']}, 'ingreso_pc': {'label': 'Ingreso laboral per cápita del hogar', 'description': 'Ingreso laboral total del hogar dividido entre integrantes_hogar.', 'source': 'Derivada', 'unit': 'pesos mexicanos', 'role': 'hogar', 'used_in': ['build_analysis_dataset']}, 'lpei': {'label': 'Línea de pobreza extrema por ingresos', 'description': 'Valor trimestral de la línea de pobreza extrema por ingresos (rural o urbana).', 'source': 'LPEI', 'unit': 'pesos mexicanos', 'role': 'poverty_threshold', 'used_in': ['build_analysis_dataset', 'compute_pl_period']}, 'factor_valido': {'label': 'Bandera de factor de expansión válido', 'description': 'Indica si el factor de expansión fac_tri es válido y positivo.', 'source': 'Derivada', 'unit': '0/1', 'role': 'ponderador', 'used_in': ['build_analysis_dataset']}, 'entra_calculo_pl': {'label': 'Bandera de elegibilidad para el cálculo de pobreza laboral', 'description': 'Indica si la persona cumple los criterios para el cálculo del indicador de pobreza laboral.', 'source': 'Derivada', 'unit': '0/1', 'role': 'eligibilidad', 'used_in': ['build_analysis_dataset', 'compute_pl_period']}, 'motivo_elegibilidad_pl': {'label': 'Motivo de elegibilidad o no elegibilidad para PL', 'description': 'Texto o código que explica por qué la persona entra o no al cálculo de PL.', 'source': 'Derivada', 'unit': None, 'role': 'eligibilidad', 'used_in': ['build_analysis_dataset']}, 'ingreso_pc_lpei_ratio': {'label': 'Relación ingreso per cápita sobre LPEI', 'description': 'Ratio entre el ingreso per cápita del hogar y la LPEI; si es menor a 1 indica pobreza.', 'source': 'Derivada', 'unit': None, 'role': 'poverty_metric', 'used_in': ['build_analysis_dataset']}, 'pobreza_laboral_persona': {'label': 'Bandera de pobreza laboral a nivel persona', 'description': 'Indica si la persona pertenece a un hogar con ingreso per cápita menor a la LPEI.', 'source': 'Derivada', 'unit': '0/1', 'role': 'poverty_indicator', 'used_in': ['build_analysis_dataset', 'compute_pl_period']}, 'periodo': {'label': 'Periodo trimestral', 'description': 'Clave del periodo en formato AAAATX.', 'source': 'Derivada', 'unit': None, 'role': 'periodo', 'used_in': ['merge_sdem_coe2', 'build_analysis_dataset', 'compute_pl_period', 'compute_pl_time_series', 'build_historical_analysis_parquet']}, 'anio': {'label': 'Año calendario', 'description': 'Año del periodo trimestral.', 'source': 'Derivada', 'unit': 'año', 'role': 'periodo', 'used_in': ['merge_sdem_coe2', 'build_analysis_dataset', 'compute_pl_period', 'compute_pl_time_series', 'build_historical_analysis_parquet']}, 'trimestre': {'label': 'Trimestre del año', 'description': 'Número de trimestre (1–4) del periodo.', 'source': 'Derivada', 'unit': 'trimestre', 'role': 'periodo', 'used_in': ['merge_sdem_coe2', 'build_analysis_dataset', 'compute_pl_period', 'compute_pl_time_series', 'build_historical_analysis_parquet']}, 'cruce_coe2': {'label': 'Estatus de cruce con COE2', 'description': 'Indica si la persona tiene un registro correspondiente en COE2 (both) o no (left_only).', 'source': 'Derivada', 'unit': None, 'role': 'merge_status', 'used_in': ['merge_sdem_coe2', 'validate_merged_period']}, 'PL': {'label': 'Índice de pobreza laboral', 'description': 'Porcentaje de la población cuyo ingreso per cápita del hogar es menor a la LPEI.', 'source': 'Derivada', 'unit': 'porcentaje', 'role': 'poverty_indicator', 'used_in': ['compute_pl_time_series']}}
    result = {}
    for col in columns:
        base = catalog.get(col, {})
        result[col] = {'label': base.get('label', col), 'description': base.get('description', ''), 'source': base.get('source', None), 'type': None, 'unit': base.get('unit', None), 'role': base.get('role', None), 'used_in': base.get('used_in', [])}
    return result

def _build_quality_indicator_dictionary(dataset_type='raw'):
    """
    Devuelve un diccionario de descripciones para indicadores de calidad.
    El contenido varía según el tipo de dataset. No incluye valores;
    estos se definirán en "quality_indicators" de los metadatos.
    """
    definitions = {}
    if dataset_type in ('raw', 'raw_sdem', 'raw_coe2'):
        definitions.update({'conversion_quality': {'label': 'Calidad de conversión', 'description': 'Cantidad de filas y bloques leídos desde el CSV original.', 'main_variable': None, 'auxiliary_variables': [], 'formula_simplified': None, 'interpretation': 'Valores mayores indican mayor volumen de datos procesados.', 'applies_to': 'conversion_zip_parquet', 'warning_threshold': None, 'critical_threshold': None}, 'schema_quality': {'label': 'Calidad del esquema', 'description': 'Evalúa el número de columnas detectadas y la ausencia de duplicados después de normalizar.', 'main_variable': None, 'auxiliary_variables': [], 'formula_simplified': None, 'interpretation': 'Un mayor número de columnas indica mayor riqueza de variables disponibles.', 'applies_to': 'schema', 'warning_threshold': None, 'critical_threshold': None}, 'critical_variable_quality': {'label': 'Calidad de variables críticas', 'description': 'Disponibilidad de variables clave para el análisis.', 'main_variable': None, 'auxiliary_variables': [], 'formula_simplified': None, 'interpretation': 'Ayuda a identificar problemas de cobertura en variables esenciales.', 'applies_to': 'raw_parquet', 'warning_threshold': None, 'critical_threshold': None}})
    if dataset_type in ('merge', 'merged'):
        definitions.update({'join_rate_pct': {'label': 'Tasa de cruce', 'description': 'Porcentaje de registros que se unieron con COE2.', 'main_variable': 'cruce_coe2', 'auxiliary_variables': [], 'formula_simplified': 'registros_cruzados / filas_sdem * 100', 'interpretation': 'Valores altos indican mejor cobertura del merge.', 'applies_to': 'persona', 'warning_threshold': None, 'critical_threshold': None}, 'left_only_rate_pct': {'label': 'Tasa left_only', 'description': 'Porcentaje de registros de SDEM sin correspondencia en COE2.', 'main_variable': 'cruce_coe2', 'auxiliary_variables': [], 'formula_simplified': 'registros_sin_coe2 / filas_sdem * 100', 'interpretation': 'Valores altos indican pérdida de información de ingresos.', 'applies_to': 'persona', 'warning_threshold': None, 'critical_threshold': None}, 'null_key_rate_pct': {'label': 'Tasa de llaves nulas', 'description': 'Porcentaje de registros con llave incompleta.', 'main_variable': None, 'auxiliary_variables': [], 'formula_simplified': '(nulos_llave_sdem + nulos_llave_coe2) / filas_sdem * 100', 'interpretation': 'Valores altos indican problemas en la llave de unión.', 'applies_to': 'persona', 'warning_threshold': None, 'critical_threshold': None}, 'duplicate_key_rate_pct': {'label': 'Tasa de llaves duplicadas', 'description': 'Porcentaje de llaves duplicadas.', 'main_variable': None, 'auxiliary_variables': [], 'formula_simplified': '(duplicados_sdem + duplicados_coe2) / filas_sdem * 100', 'interpretation': 'Valores altos indican problemas de unicidad en la llave.', 'applies_to': 'persona', 'warning_threshold': None, 'critical_threshold': None}, 'expansion_factor_quality': {'label': 'Calidad del factor de expansión', 'description': 'Disponibilidad y validez de la variable fac_tri.', 'main_variable': 'fac_tri', 'auxiliary_variables': [], 'formula_simplified': None, 'interpretation': 'Identifica registros con factor de expansión faltante o no positivo.', 'applies_to': 'persona', 'warning_threshold': None, 'critical_threshold': None}, 'income_input_availability': {'label': 'Disponibilidad de insumos de ingreso laboral', 'description': 'Número de registros con P6B2, P6C y otros insumos de ingreso.', 'main_variable': None, 'auxiliary_variables': [], 'formula_simplified': None, 'interpretation': 'Permite verificar la cobertura de las variables de ingreso.', 'applies_to': 'persona', 'warning_threshold': None, 'critical_threshold': None}})
    if dataset_type in ('analysis', 'analisis'):
        definitions.update({'residence_quality': {'label': 'Calidad de residencia', 'description': 'Filtrado de residentes válidos y tasa de descarte por residencia.', 'main_variable': 'c_res', 'auxiliary_variables': ['r_def'], 'formula_simplified': 'valid_residents / input_rows * 100', 'interpretation': 'Indica el porcentaje de registros que cumplen con residencia válida.', 'applies_to': 'persona', 'warning_threshold': None, 'critical_threshold': None}, 'income_quality': {'label': 'Calidad del ingreso', 'description': 'Cobertura de ingresos observados, imputados y no recuperables.', 'main_variable': 'p6b2', 'auxiliary_variables': ['p6c'], 'formula_simplified': '(direct_p6b2_records + imputed_records) / occupied_persons * 100', 'interpretation': 'Mide la proporción de ingresos recuperados (directamente o imputados) respecto a los ocupados.', 'applies_to': 'persona ocupada', 'warning_threshold': None, 'critical_threshold': None}, 'imputation_quality': {'label': 'Calidad de imputación', 'description': 'Proporción de ingresos imputados entre los candidatos y los ocupados remunerados.', 'main_variable': 'p6c', 'auxiliary_variables': ['p6b2', 'salario'], 'formula_simplified': 'imputed_eligible_records / candidates_for_imputation * 100', 'interpretation': 'Valores altos reflejan mayor dependencia de la imputación por rangos salariales.', 'applies_to': 'persona ocupada', 'warning_threshold': None, 'critical_threshold': None}, 'exclusion_quality': {'label': 'Calidad de exclusión', 'description': 'Tasa de registros excluidos del cálculo por ingreso incompleto del hogar.', 'main_variable': 'hogar_ingreso_incompleto', 'auxiliary_variables': [], 'formula_simplified': 'personas_hogares_incompletos / residentes_validos * 100', 'interpretation': 'Valores altos indican pérdidas metodológicas significativas.', 'applies_to': 'persona', 'warning_threshold': None, 'critical_threshold': None}, 'expansion_factor_quality': {'label': 'Calidad del factor de expansión', 'description': 'Disponibilidad y validez del factor fac_tri.', 'main_variable': 'fac_tri', 'auxiliary_variables': [], 'formula_simplified': None, 'interpretation': 'Proporciona información sobre ponderadores faltantes o inválidos.', 'applies_to': 'persona', 'warning_threshold': None, 'critical_threshold': None}, 'lpei_quality': {'label': 'Calidad de la LPEI', 'description': 'Asignación de LPEI y distribución por ámbito rural/urbano.', 'main_variable': 'lpei', 'auxiliary_variables': ['ambito'], 'formula_simplified': 'records_with_lpei / residents_valid * 100', 'interpretation': 'Evalúa la cobertura de la línea de pobreza extrema en el dataset.', 'applies_to': 'persona', 'warning_threshold': None, 'critical_threshold': None}, 'indicator_quality': {'label': 'Calidad del indicador de pobreza laboral', 'description': 'Proporción de población elegible y población pobre expandida.', 'main_variable': 'pobreza_laboral_persona', 'auxiliary_variables': ['fac_tri', 'ingreso_pc', 'lpei', 'p6b2'], 'formula_simplified': 'sum(pobreza_laboral_persona * fac_tri) / sum(fac_tri) * 100', 'interpretation': 'Mide la incidencia de pobreza laboral en la población elegible.', 'applies_to': 'persona', 'warning_threshold': None, 'critical_threshold': None}, 'sociodemographic_quality': {'label': 'Calidad sociodemográfica', 'description': 'Ausencia de valores faltantes en variables sociodemográficas clave.', 'main_variable': None, 'auxiliary_variables': ['e_con', 'n_hij', 'par_c'], 'formula_simplified': None, 'interpretation': 'Ayuda a valorar la completitud del contexto sociodemográfico.', 'applies_to': 'persona', 'warning_threshold': None, 'critical_threshold': None}})
    if dataset_type in ('series', 'pl_series'):
        definitions.update({'series_quality': {'label': 'Calidad de la serie de pobreza laboral', 'description': 'Cobertura de periodos esperados vs. procesados.', 'main_variable': None, 'auxiliary_variables': [], 'formula_simplified': 'processed_periods / expected_periods * 100', 'interpretation': 'Valor cercano a 100% indica que se procesaron todos los periodos esperados.', 'applies_to': 'serie', 'warning_threshold': None, 'critical_threshold': None}, 'period_indicators': {'label': 'Indicadores por periodo', 'description': 'Contiene métricas detalladas por periodo, como error estándar y coeficiente de variación.', 'main_variable': 'PL', 'auxiliary_variables': ['expanded_eligible_population', 'expanded_poverty_population', 'sample_eligible_records', 'sample_poverty_records'], 'formula_simplified': None, 'interpretation': 'Permite evaluar la precisión del indicador trimestre a trimestre.', 'applies_to': 'serie', 'warning_threshold': None, 'critical_threshold': None}})
    if dataset_type in ('historical', 'historial'):
        definitions.update({'temporal_quality': {'label': 'Calidad temporal', 'description': 'Cobertura temporal: periodos esperados, procesados y faltantes.', 'main_variable': None, 'auxiliary_variables': [], 'formula_simplified': 'processed_periods / expected_periods * 100', 'interpretation': 'Permite evaluar la completitud de la serie histórica.', 'applies_to': 'historico', 'warning_threshold': None, 'critical_threshold': None}, 'schema_quality': {'label': 'Calidad del esquema', 'description': 'Consistencia de columnas a lo largo de todos los periodos.', 'main_variable': None, 'auxiliary_variables': [], 'formula_simplified': None, 'interpretation': 'Verifica que todas las columnas clave están presentes y consistentes.', 'applies_to': 'historico', 'warning_threshold': None, 'critical_threshold': None}, 'row_quality': {'label': 'Calidad de filas', 'description': 'Suma de filas de los insumos vs. filas del archivo histórico.', 'main_variable': None, 'auxiliary_variables': [], 'formula_simplified': None, 'interpretation': 'Ayuda a identificar pérdidas o duplicaciones al concatenar.', 'applies_to': 'historico', 'warning_threshold': None, 'critical_threshold': None}, 'key_quality': {'label': 'Calidad de llaves históricas', 'description': 'Duplicidad en id_persona_periodo en el conjunto histórico.', 'main_variable': 'id_persona_periodo', 'auxiliary_variables': [], 'formula_simplified': None, 'interpretation': 'Un recuento alto indica problemas de identificación única a lo largo del tiempo.', 'applies_to': 'historico', 'warning_threshold': None, 'critical_threshold': None}, 'file_quality': {'label': 'Calidad de archivo', 'description': 'Métricas del archivo final, como tamaño, grupos y compresión.', 'main_variable': None, 'auxiliary_variables': [], 'formula_simplified': None, 'interpretation': 'Permite conocer el volumen y características del Parquet histórico.', 'applies_to': 'historico', 'warning_threshold': None, 'critical_threshold': None}})
    definitions.update({'fac_tri_invalid_rate_pct': {'label': 'Tasa de factor de expansión inválido', 'description': 'Porcentaje de registros con factor de expansión faltante, no numérico o no positivo.', 'main_variable': 'fac_tri', 'auxiliary_variables': ['fac'], 'formula_simplified': 'registros_fac_tri_invalidos / registros_totales * 100', 'interpretation': 'Valores altos indican problemas en el ponderador usado para expansión poblacional.', 'applies_to': 'persona', 'warning_threshold': None, 'critical_threshold': None}, 'p6b2_missing_rate_pct': {'label': 'Tasa de no disponibilidad de P6B2', 'description': 'Porcentaje de registros sin ingreso laboral mensual exacto.', 'main_variable': 'p6b2', 'auxiliary_variables': [], 'formula_simplified': 'p6b2_missing_records / registros_totales * 100', 'interpretation': 'Valores altos indican menor disponibilidad de ingreso exacto y mayor dependencia de reglas de recuperación.', 'applies_to': 'COE2/merge', 'warning_threshold': None, 'critical_threshold': None}, 'p6c_missing_rate_pct': {'label': 'Tasa de no disponibilidad de P6C', 'description': 'Porcentaje de registros sin información de rango salarial P6C.', 'main_variable': 'p6c', 'auxiliary_variables': [], 'formula_simplified': 'p6c_missing_records / registros_totales * 100', 'interpretation': 'Valores altos reducen la posibilidad de recuperar ingresos faltantes mediante rangos.', 'applies_to': 'COE2/merge', 'warning_threshold': None, 'critical_threshold': None}, 'occupied_match_rate_pct': {'label': 'Tasa de cruce de ocupados', 'description': 'Porcentaje de personas ocupadas en SDEM que tienen correspondencia en COE2.', 'main_variable': 'cruce_coe2', 'auxiliary_variables': ['clase2'], 'formula_simplified': 'occupied_with_coe2 / occupied_persons * 100', 'interpretation': 'Valores bajos pueden indicar pérdida de información laboral relevante después del merge.', 'applies_to': 'merge_sdem_coe2', 'warning_threshold': None, 'critical_threshold': None}, 'valid_resident_rate_pct': {'label': 'Tasa de residentes válidos', 'description': 'Porcentaje de registros que cumplen criterios de entrevista y residencia válidos.', 'main_variable': 'c_res', 'auxiliary_variables': ['r_def'], 'formula_simplified': 'valid_residents / input_rows * 100', 'interpretation': 'Resume la cobertura de la población elegible por residencia antes del cálculo.', 'applies_to': 'dataset_analitico', 'warning_threshold': None, 'critical_threshold': None}, 'discarded_by_residence_rate_pct': {'label': 'Tasa de descarte por residencia', 'description': 'Porcentaje de registros descartados por no cumplir criterios de entrevista o residencia.', 'main_variable': 'c_res', 'auxiliary_variables': ['r_def'], 'formula_simplified': 'discarded_by_residence / input_rows * 100', 'interpretation': 'Valores altos indican pérdida por filtros de residencia.', 'applies_to': 'dataset_analitico', 'warning_threshold': None, 'critical_threshold': None}, 'recovered_income_rate_pct': {'label': 'Tasa de ingreso recuperado', 'description': 'Porcentaje de ocupados remunerados cuyo ingreso fue recuperado de forma directa o imputada.', 'main_variable': 'ingreso_laboral_ind', 'auxiliary_variables': ['p6b2', 'p6c'], 'formula_simplified': 'recovered_income_records / remunerated_occupied_persons * 100', 'interpretation': 'Valores altos indican buena cobertura de ingresos utilizables para el cálculo.', 'applies_to': 'ocupados_remunerados', 'warning_threshold': None, 'critical_threshold': None}, 'imputation_rate_candidates_pct': {'label': 'Tasa de imputación sobre candidatos', 'description': 'Porcentaje de casos candidatos a imputación que fueron recuperados mediante P6C.', 'main_variable': 'p6c', 'auxiliary_variables': ['p6b2', 'salario'], 'formula_simplified': 'p6c_imputed_income_records / candidates_for_imputation * 100', 'interpretation': 'Valores altos indican mayor recuperación de ingresos faltantes por rangos salariales.', 'applies_to': 'candidatos_imputacion', 'warning_threshold': None, 'critical_threshold': None}, 'imputation_rate_remunerated_pct': {'label': 'Tasa de imputación sobre remunerados', 'description': 'Porcentaje de ocupados remunerados cuyo ingreso fue imputado mediante P6C.', 'main_variable': 'p6c', 'auxiliary_variables': ['p6b2', 'salario'], 'formula_simplified': 'p6c_imputed_income_records / remunerated_occupied_persons * 100', 'interpretation': 'Valores altos indican mayor dependencia de imputación en el universo remunerado.', 'applies_to': 'ocupados_remunerados', 'warning_threshold': None, 'critical_threshold': None}, 'non_imputed_candidates_rate_pct': {'label': 'Tasa de candidatos no imputados', 'description': 'Porcentaje de candidatos a imputación que no pudieron recuperarse con P6C.', 'main_variable': 'ingreso_no_recuperable', 'auxiliary_variables': ['p6b2', 'p6c'], 'formula_simplified': 'non_imputed_candidates_records / candidates_for_imputation * 100', 'interpretation': 'Valores altos anticipan mayor exclusión de hogares por ingresos no recuperables.', 'applies_to': 'candidatos_imputacion', 'warning_threshold': None, 'critical_threshold': None}, 'exclusion_rate_records_pct': {'label': 'Tasa de exclusión del cálculo', 'description': 'Porcentaje de residentes válidos en hogares con ingreso laboral incompleto.', 'main_variable': 'hogar_ingreso_incompleto', 'auxiliary_variables': ['p6b2', 'p6c'], 'formula_simplified': 'persons_in_incomplete_income_households / valid_residents * 100', 'interpretation': 'Valores altos indican mayor pérdida metodológica por ingresos no recuperables.', 'applies_to': 'residentes_validos', 'warning_threshold': None, 'critical_threshold': None}, 'exclusion_rate_expanded_pct': {'label': 'Tasa de exclusión expandida', 'description': 'Porcentaje de población expandida excluida por ingreso incompleto.', 'main_variable': 'hogar_ingreso_incompleto', 'auxiliary_variables': ['fac_tri'], 'formula_simplified': 'expanded_excluded_population / expanded_resident_population * 100', 'interpretation': 'Permite evaluar la pérdida poblacional ponderada por exclusión metodológica.', 'applies_to': 'residentes_validos', 'warning_threshold': None, 'critical_threshold': None}, 'valid_fac_tri_rate_pct': {'label': 'Tasa de factor de expansión válido', 'description': 'Porcentaje de registros con factor de expansión positivo y utilizable.', 'main_variable': 'fac_tri', 'auxiliary_variables': [], 'formula_simplified': 'valid_fac_tri_records / registros_totales * 100', 'interpretation': 'Valores altos indican disponibilidad adecuada del ponderador.', 'applies_to': 'persona', 'warning_threshold': None, 'critical_threshold': None}, 'lpei_assignment_rate_pct': {'label': 'Tasa de asignación de LPEI', 'description': 'Porcentaje de registros con LPEI asignada según ámbito rural/urbano.', 'main_variable': 'lpei', 'auxiliary_variables': ['ambito', 't_loc_tri'], 'formula_simplified': 'registros_con_lpei / residentes_validos * 100', 'interpretation': 'Valores bajos sugieren problemas en la clasificación rural/urbana o en la tabla LPEI.', 'applies_to': 'residentes_validos', 'warning_threshold': None, 'critical_threshold': None}, 'unknown_scope_rate_pct': {'label': 'Tasa de ámbito desconocido', 'description': 'Porcentaje de registros sin clasificación rural o urbana.', 'main_variable': 'ambito', 'auxiliary_variables': ['t_loc_tri', 't_loc'], 'formula_simplified': 'unknown_scope_records / residentes_validos * 100', 'interpretation': 'Valores altos pueden impedir la asignación correcta de LPEI.', 'applies_to': 'residentes_validos', 'warning_threshold': None, 'critical_threshold': None}, 'eligibility_rate_pct': {'label': 'Tasa de elegibilidad para PL', 'description': 'Porcentaje de residentes válidos que entran al cálculo de pobreza laboral.', 'main_variable': 'entra_calculo_pl', 'auxiliary_variables': ['factor_valido', 'hogar_ingreso_incompleto', 'lpei'], 'formula_simplified': 'eligible_records / valid_residents * 100', 'interpretation': 'Valores bajos indican pérdida por reglas de elegibilidad del indicador.', 'applies_to': 'residentes_validos', 'warning_threshold': None, 'critical_threshold': None}, 'poverty_labor_rate_pct': {'label': 'Pobreza laboral ponderada', 'description': 'Porcentaje ponderado de personas cuyo ingreso laboral per cápita del hogar es menor a la LPEI.', 'main_variable': 'pobreza_laboral_persona', 'auxiliary_variables': ['fac_tri', 'ingreso_pc', 'lpei'], 'formula_simplified': 'sum(pobreza_laboral_persona * fac_tri) / sum(fac_tri) * 100', 'interpretation': 'Es el indicador de pobreza laboral calculado para el periodo.', 'applies_to': 'personas_elegibles_calculo_pl', 'warning_threshold': None, 'critical_threshold': None}, 'poverty_labor_rate_unweighted_pct': {'label': 'Pobreza laboral no ponderada', 'description': 'Porcentaje muestral no ponderado de personas pobres laborales entre registros elegibles.', 'main_variable': 'pobreza_laboral_persona', 'auxiliary_variables': ['entra_calculo_pl'], 'formula_simplified': 'sample_poverty_records / sample_eligible_records * 100', 'interpretation': 'Control descriptivo no oficial; no sustituye el cálculo ponderado.', 'applies_to': 'muestra_elegible', 'warning_threshold': None, 'critical_threshold': None}})
    return definitions
