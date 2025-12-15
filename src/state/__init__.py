"""
State Package

Watermark and state management for incremental processing.
"""

from state.watermark import (
    get_watermark,
    update_watermark,
    get_max_watermark_from_df,
    create_watermark_table,
    WatermarkManager,
)

__all__ = [
    "get_watermark",
    "update_watermark",
    "get_max_watermark_from_df",
    "create_watermark_table",
    "WatermarkManager",
]
