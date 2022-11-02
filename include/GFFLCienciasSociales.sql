SELECT  universities, 
        careers, 
		inscription_dates, 
		names, 
		sexo, 
		birth_dates, 
		locations,
		emails
FROM 
	lat_sociales_cine lsc 
WHERE 
	lsc.universities = '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES'
	AND TO_DATE(lsc.inscription_dates,'DD-MM-YYYY') BETWEEN  '2020-09-01' AND '2021-02-01';