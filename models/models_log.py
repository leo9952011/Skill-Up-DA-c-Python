import logging

def create_logger():  

    logger = logging.getLogger('models_logger') 
    logger.setLevel(logging.INFO) 
    
    logfile = logging.FileHandler(r'dags/logs/logs_file.log') 
    
    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    formatter = logging.Formatter(fmt) 
      
    logfile.setFormatter(formatter) 
    logger.addHandler(logfile) 
      
    return logger 