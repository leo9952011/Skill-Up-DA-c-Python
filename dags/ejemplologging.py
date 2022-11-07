import logging


def create_logger():  

    logger = logging.getLogger('models_logger') 
    logger.setLevel(logging.INFO) 
    logfile = logging.FileHandler(r'C:\Users\Usuario\Desktop\Alk-Project-Py\Skill-Up-DA-c-Python\dags\logs\log_file.log') 
    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt) 
    logfile.setFormatter(formatter) 
    logger.addHandler(logfile) 
      
    return logger 

log = create_logger()

log.info("esto sale desde local")