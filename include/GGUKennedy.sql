SELECT  universidades, 
		carreras, 
		fechas_de_inscripcion, 
		nombres, 
		sexo, 
		fechas_nacimiento, 
		codigos_postales,
		emails
FROM 
    uba_kenedy uk 
WHERE 
    uk.universidades = 'universidad-j.-f.-kennedy'
    AND TO_DATE(uk.fechas_de_inscripcion,'YY-MON-DD') BETWEEN '2020-09-01' AND '2021-02-01';