import logging
import logging.config

logging.config.fileConfig(
    fname="logger.cfg",
)

# create logger
logger = logging.getLogger("GHUBuenos_Aires_dag_etl")

# 'application' code
logger.debug("debug message")
logger.info("info message")
logger.warning("warn message")
logger.error("error message")
logger.critical("critical message")

logger = logging.getLogger("GHUCine_dag_etl")

# 'application' code
logger.debug("debug message")
logger.info("info message")
logger.warning("warn message")
logger.error("error message")
logger.critical("critical message")
