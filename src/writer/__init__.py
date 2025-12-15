"""
Writer Package

Delta table write operations.
"""

from writer.delta_writer import (
    write_insert,
    write_merge,
    write_delete_insert,
    write_truncate_insert,
    DeltaWriter,
)

__all__ = [
    "write_insert",
    "write_merge",
    "write_delete_insert",
    "write_truncate_insert",
    "DeltaWriter",
]
