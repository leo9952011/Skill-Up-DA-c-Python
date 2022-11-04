SELECT
	universidad AS university,
	carrera AS career,
	fecha_de_inscripcion AS inscription_date,
	NULL AS first_name,
	name AS last_name,
	sexo AS gender,
	fecha_nacimiento,
	NULL AS age,
	codigo_postal AS postal_code,
	NULL AS location,
	correo_electronico as email
FROM
	public.flores_comahue
WHERE
	universidad = 'UNIVERSIDAD DE FLORES' AND fecha_de_inscripcion BETWEEN '2020-09-01' AND '2021-02-01';