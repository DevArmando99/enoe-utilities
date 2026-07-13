"""Lectura y conversión de microdatos ENOE a Parquet."""
from pathlib import Path
from zipfile import ZipFile
from datetime import datetime, timezone
import re
import warnings
from pandas import read_csv, DataFrame
import pyarrow as pa
import pyarrow.parquet as pq
from .metadata import _utc_now_iso, _safe_file_size_mb, _safe_rate_pct, _sql_identifier, _resolve_metadata_physical_column, _read_parquet_schema_info, _write_json_metadata, _append_period_paradata_step, _build_column_dictionary, _build_quality_indicator_dictionary

def get_time_series(path_in, path_out, chunk_size=100000, compression='snappy', overwrite=False, *, generate_metadata=False, path_paradata='../Paradata'):
    """
    Funcion que procesa los archivos ZIP de la ENOE y transforma
    las tablas SDEM y COE2 de CSV a Parquet sin cargar todos los
    dataframes en memoria.

    Retorna un diccionario con:

        clave: AAAAT{1,2,3,4}

        valor:
        [
            ruta del archivo COE2.parquet,
            ruta del archivo SDEM.parquet
        ]

    Los archivos CSV se leen por bloques y cada bloque se incorpora
    progresivamente al archivo Parquet correspondiente.
    """

    def csv_zip_to_parquet(zip_file, csv_member, parquet_path, table_name):
        """
        Lee un CSV directamente desde un archivo ZIP por bloques
        y escribe un unico archivo Parquet.

        No extrae el CSV completo ni mantiene todo el dataframe
        en memoria.
        """
        temp_path = parquet_path.with_suffix(parquet_path.suffix + '.tmp')
        if temp_path.exists():
            temp_path.unlink()
        writer = None
        parquet_schema = None
        total_rows = 0
        total_chunks = 0
        try:
            with zip_file.open(csv_member) as csv_buffer:
                csv_reader = read_csv(csv_buffer, encoding='ISO-8859-1', dtype='string', chunksize=chunk_size, low_memory=False)
                for chunk in csv_reader:
                    chunk.columns = [str(column).strip().lower() for column in chunk.columns]
                    if chunk.columns.duplicated().any():
                        duplicate_columns = chunk.columns[chunk.columns.duplicated()].tolist()
                        raise ValueError(f'[{table_name}] Columnas duplicadas después de normalizar: {duplicate_columns}')
                    table = pa.Table.from_pandas(chunk, preserve_index=False)
                    table = table.replace_schema_metadata()
                    if writer is None:
                        parquet_schema = table.schema
                        writer = pq.ParquetWriter(where=temp_path, schema=parquet_schema, compression=compression, use_dictionary=True)
                    elif not table.schema.equals(parquet_schema, check_metadata=False):
                        table = table.cast(parquet_schema, safe=False)
                    writer.write_table(table)
                    total_rows += table.num_rows
                    total_chunks += 1
                    del chunk
                    del table
            if writer is None:
                raise ValueError(f'[{table_name}] El archivo CSV está vacío: {csv_member}')
        except Exception:
            if writer is not None:
                writer.close()
            if temp_path.exists():
                temp_path.unlink()
            raise
        else:
            writer.close()
            if parquet_path.exists():
                parquet_path.unlink()
            temp_path.replace(parquet_path)
        return {'path': parquet_path, 'rows': total_rows, 'chunks': total_chunks}
    folder_in = Path(path_in)
    folder_out = Path(path_out)
    folder_out.mkdir(parents=True, exist_ok=True)
    if generate_metadata:
        paradata_info = {}
        metadata_info = {}
    time_series = {}
    zip_periods = []
    for zip_name in folder_in.glob('*.zip'):
        match = re.search('(\\d{4})_?trim([1-4])', zip_name.name.lower())
        if not match:
            warnings.warn(f'No se pudo identificar el periodo en: {zip_name.name}')
            continue
        anio, trimestre = match.groups()
        zip_periods.append((int(anio), int(trimestre), zip_name))
    zip_periods.sort(key=lambda item: (item[0], item[1]))
    for anio, trimestre, zip_name in zip_periods:
        key = f'{anio}T{trimestre}'
        if key in time_series:
            warnings.warn(f'Periodo duplicado: {key}. Se omitirá el archivo {zip_name.name}')
            continue
        output_folder = folder_out / key
        output_folder.mkdir(parents=True, exist_ok=True)
        if generate_metadata:
            paradata_info[key] = {'steps': [], 'first_step_start': None, 'last_step_end': None, 'final_outputs': []}
            metadata_info[key] = {}
        parquet_paths = {'COE2': None, 'SDEM': None}
        print(f'\nProcesando {key}: {zip_name.name}')
        with ZipFile(zip_name, 'r') as zip_file:
            zip_members = zip_file.namelist()
            important_tables = {'COE2': [], 'SDEM': []}
            for member in zip_members:
                member_name = Path(member).name
                member_upper = member_name.upper()
                if Path(member_name).suffix.lower() != '.csv':
                    continue
                if 'COE2' in member_upper:
                    important_tables['COE2'].append(member)
                elif 'SDEM' in member_upper:
                    important_tables['SDEM'].append(member)
            for table_name in ['COE2', 'SDEM']:
                candidates = important_tables[table_name]
                if len(candidates) == 0:
                    warnings.warn(f'[{key}] Falta archivo {table_name} en el ZIP {zip_name.name}')
                    continue
                if len(candidates) > 1:
                    warnings.warn(f'[{key}] Se encontraron varios archivos {table_name}: {candidates}. Se utilizará el primero.')
                csv_member = sorted(candidates)[0]
                parquet_path = output_folder / f'{table_name}.parquet'
                if generate_metadata:
                    step_start_time = datetime.now(timezone.utc)
                if parquet_path.exists() and (not overwrite):
                    print(f'  {table_name}: ya existe, se conserva {parquet_path.name}')
                    parquet_paths[table_name] = parquet_path
                    if generate_metadata:
                        try:
                            pf = pq.ParquetFile(parquet_path)
                            row_count = pf.metadata.num_rows
                            row_groups = pf.metadata.num_row_groups
                            column_names = pf.schema.names
                        except Exception:
                            row_count = None
                            row_groups = None
                            column_names = None
                        step_end_time = datetime.now(timezone.utc)
                        duration = (step_end_time - step_start_time).total_seconds()
                        step_info = {'step_number': len(paradata_info[key]['steps']) + 1, 'step_name': f'conversion_zip_csv_to_parquet_{table_name.lower()}', 'function_name': 'get_time_series', 'transformation_description': f'Conversión de {table_name} desde CSV dentro de ZIP a Parquet (reutilizado)', 'status': 'reused', 'started_at_utc': step_start_time.isoformat(), 'finished_at_utc': step_end_time.isoformat(), 'duration_seconds': duration, 'input': {'object_name': zip_name.name, 'object_type': 'zip', 'path': str(zip_name), 'role': 'archivo_crudo_enoe', 'contained_files_used': [{'object_name': Path(csv_member).name, 'object_type': 'csv', 'role': f'tabla_{table_name.lower()}'}]}, 'parameters': {'chunk_size': chunk_size, 'compression': compression, 'overwrite': overwrite}, 'output': {'object_name': parquet_path.name, 'object_type': 'parquet', 'path': str(parquet_path), 'role': f'parquet_{table_name.lower()}'}, 'execution_summary': {'rows': row_count, 'chunks': None, 'row_groups': row_groups}, 'warnings': [], 'errors': []}
                        paradata_info[key]['steps'].append(step_info)
                        if paradata_info[key]['first_step_start'] is None or step_start_time < paradata_info[key]['first_step_start']:
                            paradata_info[key]['first_step_start'] = step_start_time
                        if paradata_info[key]['last_step_end'] is None or step_end_time > paradata_info[key]['last_step_end']:
                            paradata_info[key]['last_step_end'] = step_end_time
                        paradata_info[key]['final_outputs'].append(str(parquet_path))
                        metadata_info[key][table_name] = {'zip_name': zip_name, 'csv_member': csv_member, 'parquet_path': parquet_path, 'rows': row_count, 'chunks': None}
                    continue
                result = csv_zip_to_parquet(zip_file=zip_file, csv_member=csv_member, parquet_path=parquet_path, table_name=table_name)
                parquet_paths[table_name] = result['path']
                print(f"  {table_name}: {result['rows']:,} filas | {result['chunks']:,} bloques | {result['path'].name}")
                if generate_metadata:
                    step_end_time = datetime.now(timezone.utc)
                    duration = (step_end_time - step_start_time).total_seconds()
                    step_info = {'step_number': len(paradata_info[key]['steps']) + 1, 'step_name': f'conversion_zip_csv_to_parquet_{table_name.lower()}', 'function_name': 'get_time_series', 'transformation_description': f'Conversión de {table_name} desde CSV dentro de ZIP a Parquet', 'status': 'ok', 'started_at_utc': step_start_time.isoformat(), 'finished_at_utc': step_end_time.isoformat(), 'duration_seconds': duration, 'input': {'object_name': zip_name.name, 'object_type': 'zip', 'path': str(zip_name), 'role': 'archivo_crudo_enoe', 'contained_files_used': [{'object_name': Path(csv_member).name, 'object_type': 'csv', 'role': f'tabla_{table_name.lower()}'}]}, 'parameters': {'chunk_size': chunk_size, 'compression': compression, 'overwrite': overwrite}, 'output': {'object_name': result['path'].name, 'object_type': 'parquet', 'path': str(result['path']), 'role': f'parquet_{table_name.lower()}'}, 'execution_summary': {'rows': result['rows'], 'chunks': result['chunks'], 'row_groups': None}, 'warnings': [], 'errors': []}
                    paradata_info[key]['steps'].append(step_info)
                    if paradata_info[key]['first_step_start'] is None or step_start_time < paradata_info[key]['first_step_start']:
                        paradata_info[key]['first_step_start'] = step_start_time
                    if paradata_info[key]['last_step_end'] is None or step_end_time > paradata_info[key]['last_step_end']:
                        paradata_info[key]['last_step_end'] = step_end_time
                    paradata_info[key]['final_outputs'].append(str(result['path']))
                    metadata_info[key][table_name] = {'zip_name': zip_name, 'csv_member': csv_member, 'parquet_path': result['path'], 'rows': result['rows'], 'chunks': result['chunks']}
        time_series[key] = [parquet_paths['COE2'], parquet_paths['SDEM']]
    if generate_metadata:
        for period_key, pdata in paradata_info.items():
            for step_info in pdata.get('steps', []):
                _append_period_paradata_step(periodo=period_key, step_info=step_info, path_paradata=path_paradata, initial_notes=['SDEM se utiliza como tabla madre en el merge posterior.', 'COE2 aporta las variables laborales y de ingreso utilizadas por el pipeline.'])
        for period_key, tables in metadata_info.items():
            for table_name, data in tables.items():
                parquet_path = Path(data['parquet_path'])
                schema_info = _read_parquet_schema_info(parquet_path)
                if schema_info is not None:
                    row_count = schema_info['rows']
                    row_groups = schema_info['row_groups']
                    column_names = schema_info['column_names']
                    column_types = schema_info['column_types']
                else:
                    row_count = data.get('rows')
                    row_groups = None
                    column_names = []
                    column_types = []
                file_size_mb = _safe_file_size_mb(parquet_path)
                column_dict = _build_column_dictionary(column_names, dataset_type='raw')
                for col_name, col_type in zip(column_names, column_types):
                    if col_name in column_dict:
                        column_dict[col_name]['type'] = col_type
                    else:
                        column_dict[col_name] = {'label': col_name, 'description': '', 'source': None, 'type': col_type, 'unit': None, 'role': None, 'used_in': []}
                if table_name.upper() == 'SDEM':
                    critical_vars = ['fac_tri', 'r_def', 'c_res', 'clase2', 'pos_ocu']
                elif table_name.upper() == 'COE2':
                    critical_vars = ['p6b2', 'p6c', 'p6b1', 'p6_9']
                else:
                    critical_vars = []
                critical_quality = {}
                critical_present = []
                missing_critical = []
                historical_equivalences_applied = {}
                metadata_warnings = []

                def _missing_count_for_column(con_obj, tbl_sql, physical_col):
                    if physical_col is None:
                        return None
                    col_sql = _sql_identifier(physical_col)
                    return con_obj.execute(f"\n                        SELECT COUNT(*)\n                        FROM ({tbl_sql})\n                        WHERE NULLIF(TRIM(CAST({col_sql} AS VARCHAR)), '') IS NULL\n                        ").fetchone()[0]
                try:
                    import duckdb
                    con = duckdb.connect(database=':memory:')
                    table_sql = f"SELECT * FROM read_parquet('{parquet_path.resolve().as_posix()}')"
                    total = con.execute(f'SELECT COUNT(*) FROM ({table_sql})').fetchone()[0]
                    column_equivalences = {'fac_tri': ['fac_tri', 'fac'], 't_loc_tri': ['t_loc_tri', 't_loc'], 'cve_ent': ['cve_ent', 'ent']}
                    if table_name.upper() == 'SDEM':
                        physical_map = {}
                        schema_check_vars = ['fac_tri', 'r_def', 'c_res', 'clase2', 'pos_ocu', 't_loc_tri', 'cve_ent']
                        for canonical in schema_check_vars:
                            physical = _resolve_metadata_physical_column(column_names, canonical, column_equivalences)
                            physical_map[canonical] = physical
                            if physical is None:
                                missing_critical.append(canonical)
                            else:
                                critical_present.append(canonical)
                                if physical != canonical:
                                    historical_equivalences_applied[canonical] = physical
                        fac_col = physical_map.get('fac_tri')
                        critical_quality['fac_tri_physical_column'] = fac_col
                        if fac_col is not None:
                            fac_sql = _sql_identifier(fac_col)
                            fac_missing = _missing_count_for_column(con, table_sql, fac_col)
                            fac_nonpos = con.execute(f"\n                                SELECT COUNT(*)\n                                FROM ({table_sql})\n                                WHERE NULLIF(TRIM(CAST({fac_sql} AS VARCHAR)), '') IS NOT NULL\n                                  AND TRY_CAST({fac_sql} AS DOUBLE) <= 0\n                                ").fetchone()[0]
                            fac_invalid = (fac_missing or 0) + (fac_nonpos or 0)
                            critical_quality['fac_tri_missing_records'] = int(fac_missing)
                            critical_quality['fac_tri_non_positive_records'] = int(fac_nonpos)
                            critical_quality['fac_tri_invalid_rate_pct'] = _safe_rate_pct(fac_invalid, total)
                        else:
                            critical_quality['fac_tri_missing_records'] = None
                            critical_quality['fac_tri_non_positive_records'] = None
                            critical_quality['fac_tri_invalid_rate_pct'] = None
                        for var in ['r_def', 'c_res', 'clase2', 'pos_ocu']:
                            phys = physical_map.get(var)
                            critical_quality[f'{var}_missing_records'] = int(_missing_count_for_column(con, table_sql, phys)) if phys is not None else None
                        tloc_col = physical_map.get('t_loc_tri')
                        critical_quality['t_loc_tri_physical_column'] = tloc_col
                        critical_quality['t_loc_tri_missing_records'] = int(_missing_count_for_column(con, table_sql, tloc_col)) if tloc_col is not None else None
                        cve_col = physical_map.get('cve_ent')
                        critical_quality['cve_ent_physical_column'] = cve_col
                        critical_quality['cve_ent_missing_records'] = int(_missing_count_for_column(con, table_sql, cve_col)) if cve_col is not None else None
                    elif table_name.upper() == 'COE2':
                        for canonical in critical_vars:
                            physical = _resolve_metadata_physical_column(column_names, canonical)
                            if physical is None:
                                missing_critical.append(canonical)
                                critical_quality[f'{canonical}_missing_records'] = None
                                critical_quality[f'{canonical}_non_missing_records'] = None
                                critical_quality[f'{canonical}_missing_rate_pct'] = None
                                critical_quality[f'{canonical}_non_missing_rate_pct'] = None
                                continue
                            critical_present.append(canonical)
                            missing = int(_missing_count_for_column(con, table_sql, physical))
                            non_missing = int(total - missing)
                            critical_quality[f'{canonical}_missing_records'] = missing
                            critical_quality[f'{canonical}_non_missing_records'] = non_missing
                            critical_quality[f'{canonical}_missing_rate_pct'] = _safe_rate_pct(missing, total)
                            critical_quality[f'{canonical}_non_missing_rate_pct'] = _safe_rate_pct(non_missing, total)
                    con.close()
                except Exception as metadata_error:
                    metadata_warnings.append(f'No fue posible calcular todos los indicadores críticos raw: {metadata_error}')
                    if table_name.upper() == 'SDEM':
                        critical_quality = {'fac_tri_physical_column': None, 'fac_tri_missing_records': None, 'fac_tri_non_positive_records': None, 'fac_tri_invalid_rate_pct': None, 'r_def_missing_records': None, 'c_res_missing_records': None, 'clase2_missing_records': None, 'pos_ocu_missing_records': None, 't_loc_tri_physical_column': None, 't_loc_tri_missing_records': None, 'cve_ent_physical_column': None, 'cve_ent_missing_records': None}
                quality_indicator_dict = _build_quality_indicator_dictionary(dataset_type='raw')
                quality_indicators = {'conversion_quality': {'rows_converted': data.get('rows'), 'chunks_processed': data.get('chunks'), 'compression_used': compression}, 'schema_quality': {'num_columns': len(column_names), 'duplicate_columns_after_normalization': 0, 'critical_variables_present': critical_present, 'missing_critical_variables': missing_critical, 'historical_equivalences_applied': historical_equivalences_applied}, 'critical_variable_quality': critical_quality}
                metadata_json = {'general_information': {'dataset_name': table_name.upper(), 'dataset_period': period_key, 'dataset_level': 'persona', 'dataset_type': 'raw_parquet', 'created_at_utc': _utc_now_iso(), 'created_by_function': 'get_time_series', 'function_version': '2.2.0', 'project_name': 'Calculo de pobreza laboral'}, 'source_data': {'source_zip': str(data.get('zip_name')), 'source_csv': str(Path(data.get('csv_member')).name) if data.get('csv_member') else None, 'source_table': table_name.upper(), 'encoding': 'ISO-8859-1'}, 'construction_method': {'function': 'get_time_series', 'method_description': 'Conversión de archivos CSV de ENOE contenidos en ZIP a formato Parquet.', 'parameters': {'chunk_size': chunk_size, 'compression': compression, 'overwrite': overwrite}}, 'dataset_size': {'rows': row_count, 'columns': len(column_names), 'file_size_mb': file_size_mb, 'row_groups': row_groups, 'rows_converted': data.get('rows'), 'chunks_processed': data.get('chunks')}, 'schema': {'column_count': len(column_names), 'columns': column_dict}, 'critical_variables': critical_vars, 'quality_indicator_dictionary': quality_indicator_dict, 'quality_indicators': quality_indicators, 'methodological_observations': metadata_warnings}
                metadata_path = parquet_path.with_name(f'{table_name}_metadata_quality.json')
                _write_json_metadata(metadata_json, metadata_path)
    return time_series
