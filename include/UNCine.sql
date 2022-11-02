select universities , careers , inscription_dates , names , sexo, locations, emails  
from lat_sociales_cine
where to_date(inscription_dates, 'DD-MM-YYYY') between '2020-09-01' and '2021-02-01'
and universities  = 'UNIVERSIDAD-DEL-CINE';