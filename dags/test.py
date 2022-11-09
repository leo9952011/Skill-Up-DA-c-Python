from pathlib import Path
import logging
import logging.config

BASE_DIR = Path().parent.parent

logger_cfg = BASE_DIR / "logger.cfg"

logging.config.fileConfig(
    fname=logger_cfg,
)

# Crear el logger ya configurado.
logger = logging.getLogger("GHUBuenos_Aires_dag_etl")

logger.warning("test")
