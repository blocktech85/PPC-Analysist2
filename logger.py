import logging
import os

# Log file next to this module when possible, else cwd
_log_dir = os.path.dirname(os.path.abspath(__file__))
_log_path = os.path.join(_log_dir, "app.log")

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(_log_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)