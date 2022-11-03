from typing import List

from .models_log import create_logger
log = create_logger()

def get_query(path:List):
    
    """_summary_
    read sql files
    """
    for i in path:
        try:
            with open(r'include\UMoron.sql', 'r') as myfile:
                data = myfile.read()
                print(data)
                log.info(f"query {i} in process")
        except:
            log.warning(f"SyntaxError: invalid syntax for {i} ")
            pass
        