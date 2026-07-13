"""Utilidades generales internas para ENOE Utilities."""

from pathlib import Path


def _paradata_path_string(path):
    """Convierte una ruta a texto portable para paradatos sin resolverla."""
    if path is None:
        return None
    try:
        return Path(path).as_posix()
    except Exception:
        return str(path).replace("\\", "/")
