select universidades, carreras, fechas_de_inscripcion, nombres, sexo, codigos_postales, emails  
from uba_kenedy uk 
where to_date(fechas_de_inscripcion , 'yy-Mon-dd') between '2020-09-01' and '2021-02-01'
and universidades = 'universidad-de-buenos-aires';