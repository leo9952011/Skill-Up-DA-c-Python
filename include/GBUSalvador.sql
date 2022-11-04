-- Grupo de Universidades B
-- Necesito para UNComahue y UdelSalvador, 
-- personas anotadas entre las fechas 01/9/2020 al 01/02/2021
--
-- university
-- career
-- inscription_date
-- * first_name
-- * last_name
-- gender
-- * age
-- * postal_code
-- location
-- email
--
-- columnas que no tengo (*)
-- en last_name traigo nombre 
-- first_name, age y postal_code las genero con valor NULL
-- y en un paso posterior las transformo con pandas
--
-- TO_DATE <https://www.postgresqltutorial.com/postgresql-date-functions/postgresql-to_date/>
--
-- Para UdelSalvador

SELECT
	universidad AS university,
	carrera AS career,
	fecha_de_inscripcion AS inscription_date,
	nombre AS last_name,
	NULL AS first_name,
	sexo AS gender,
	fecha_nacimiento,
	NULL AS age,
	NULL AS postal_code,
	localidad AS location,
	email 
FROM public.salvador_villa_maria
WHERE universidad = 'UNIVERSIDAD_DEL_SALVADOR'
	AND TO_DATE(fecha_de_inscripcion,'DD-Mon-YY') BETWEEN '2020/09/01' AND '2021/02/01'
