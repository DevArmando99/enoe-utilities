"""Public API for ENOE Utilities."""

from .io import get_time_series
from .lpei import get_pl_series, build_lpei_quarterly_long
from .validation import validate_required_columns, validate_merged_period
from .merge import resolve_parquet_columns, merge_period_parquet, build_merged_time_series
from .analysis import build_analysis_dataset
from .indicators import compute_pl_period, compute_pl_time_series
from .historical import build_historical_analysis_parquet

__version__ = "0.1.0"

__all__ = [
    "get_time_series",
    "get_pl_series",
    "build_lpei_quarterly_long",
    "validate_required_columns",
    "validate_merged_period",
    "resolve_parquet_columns",
    "merge_period_parquet",
    "build_merged_time_series",
    "build_analysis_dataset",
    "compute_pl_period",
    "compute_pl_time_series",
    "build_historical_analysis_parquet",
]
