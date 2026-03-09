import functools
import logging
import logging.config
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

from evernote.edam.type.ttypes import Note, Notebook

from evernote_backup.cli_app_util import is_output_to_terminal

IS_TESTING = "pytest" in sys.modules


class LevelPrefixFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        formatted_message = super().format(record)

        # For INFO level, return the formatted message as-is
        if record.levelno == logging.INFO:
            return formatted_message

        # For other levels, add the log level prefix
        return f"{record.levelname}: {formatted_message}"


def init_logging(log_level: str, log_file: Optional[Path] = None) -> None:
    main_logger = "evernote_backup"

    format_short = "%(message)s"
    format_long = "%(asctime)s | %(levelname)s | %(message)s"

    if is_output_to_terminal():
        console_formatter = {
            "()": LevelPrefixFormatter,
            "format": format_short,
        }
    else:
        console_formatter = {"format": format_long}

    config: dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "console": console_formatter,
            "file": {"format": format_long},
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level,
                "formatter": "console",
            }
        },
        "loggers": {
            main_logger: {
                "level": log_level,
                "handlers": ["console"],
                "propagate": IS_TESTING,
            }
        },
    }

    if log_file:
        config["handlers"]["file"] = {
            "class": "logging.FileHandler",
            "level": log_level,
            "formatter": "file",
            "filename": str(log_file),
            "encoding": "utf-8",
            "delay": True,
        }
        config["loggers"][main_logger]["handlers"].append("file")

    logging.config.dictConfig(config)


def log_format_note(note: Note) -> str:  # pragma: no cover
    n_info = [
        f"'{note.title}' [{note.guid}]",
        f"notebook_id [{note.notebookGuid}]",
    ]

    if not note.active:
        n_info.append("DELETED")

    return ", ".join(n_info)


def log_format_notebook(notebook: Notebook) -> str:  # pragma: no cover
    nb_info = [f"'{notebook.name}' [{notebook.guid}]"]

    if notebook.stack:
        nb_info.append(f"stack '{notebook.stack}'")

    return ", ".join(nb_info)


def get_time_txt(seconds: int) -> str:
    seconds_hour = 3600
    seconds_minute = 60

    if seconds > seconds_hour:
        return time.strftime("%H:%M:%S", time.gmtime(seconds))
    elif seconds > seconds_minute:
        return time.strftime("%M:%S", time.gmtime(seconds))

    return time.strftime("0:%S", time.gmtime(seconds))


def format_duration(seconds: float) -> str:
    """Format duration with appropriate units, never omitting decimal places.
    
    Converts seconds to larger units when appropriate:
    - Over 60 seconds: use minutes
    - Over 60 minutes: use hours  
    - Over 24 hours: use days
    - Over 30 days: use months
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted string with appropriate unit
    """
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.2f} minutes"
    
    hours = minutes / 60
    if hours < 24:
        return f"{hours:.2f} hours"
    
    days = hours / 24
    if days < 30:
        return f"{days:.2f} days"
    
    months = days / 30
    return f"{months:.2f} months"


def log_operation_time(func: Callable) -> Callable:
    """Decorator to log operation start time and completion time with duration.
    
    Args:
        func: Function to wrap with timing
        
    Returns:
        Wrapped function with timing logs
    """
    logger = logging.getLogger(__name__)
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Extract function name for logging
        func_name = func.__name__
        if hasattr(func, '__qualname__'):
            func_name = func.__qualname__
            
        logger.info(f"Starting {func_name} at {start_time_str}")
        
        try:
            result = func(*args, **kwargs)
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            duration_str = format_duration(duration)
            
            logger.info(f"Completed {func_name} in {duration_str}")
            return result
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            duration_str = format_duration(duration)
            
            logger.error(f"Failed {func_name} after {duration_str}: {e}")
            raise
    
    return wrapper
