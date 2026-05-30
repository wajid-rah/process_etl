import logging
import os


def get_logger(name: str, log_file: str = "logs/etl_pipeline.log") -> logging.Logger:
    """
    Returns a named logger that writes to both file and console.
    Call once per module using: logger = get_logger(__name__)
    """

    """
    Returns a named logger that writes to both file and console.
    Call once per module using: logger = get_logger(__name__)
    """
    # Convert to absolute path based on /app working directory
    # Prevents path confusion when script runs from different locations
    if not os.path.isabs(log_file):  # /app/utils/logger.py
        base_dir = os.path.dirname(os.path.abspath(__file__))  # /app/utils
        # Go up one level from utils/ to reach /app
        app_dir = os.path.dirname(base_dir)  # /app
        log_file = os.path.join(app_dir, log_file)  # /app/logs/etl_pipeline.log  ← full path

    # logs/ is a Docker mounted volume — Docker creates and owns this folder
    # Do NOT call os.makedirs() here — Docker volume mount controls it
    # Removed: os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if logger already configured
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# logging
# │
# │	#Controls log appearance  Eg: "2026-05-17 10:20:30 - main - INFO - Pipeline Started"
# ├── Formatter(  "%(asctime)s - %(name)s - %(levelname)s - %(message)s" ) -> formatter
# │
# │
# │	#Create handler that writes logs into file
# ├── FileHandler(log_file_path) ->file_handler
# │   								└── setFormatter(formatter) # Apply formatting to file logs.
# │									└── setLevel(logging.ERROR) #<- Example
# │
# │	#Create handler that writes logs to Console
# ├── StreamHandler(log_file_path) ->console_handler
# │   								└── setFormatter(formatter) # Apply formatting to console logs.
# │									└── setLevel(logging.ERROR) #<- Example
# │
# │	# Useful for identifying which module or file generates log
# └──── get_logger(__name__) -> logger
#     							└── setLevel(logging.INFO)
#								└── DEBUG/INFO/WARNING/ERROR/CRITICAL   eg: logger.INFO("pipeline started..")
#								└── addHandler() #Attach handlers to logger.
#									Example
#										logger.addHandler(file_handler)
#										logger.addHandler(console_handler)
#
#										Logger
#										├── FileHandler    # write to file
#										└── ConsoleHandler # print to console
#
#
#                    Logger
#                       │
#        ┌──────────────┴──────────────┐
#        │                             │
# FileHandler                    StreamHandler
# (write to file)                (print to console)
#        │                             │
#        └──────────────┬──────────────┘
#                       │
#                   Formatter
#             (controls appearance)
#
#
