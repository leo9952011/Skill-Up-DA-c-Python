SELECT
	universidad AS university,
	carrera AS career,
	fecha_de_inscripcion AS inscription_date,
	NULL AS first_name,
	nombre AS last_name,
	sexo AS gender,
	fecha_nacimiento,
	NULL AS age,
	NULL AS postal_code,
	localidad AS location,
	email
FROM
	public.salvador_villa_maria
WHERE
	universidad = 'UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA' AND TO_DATE(fecha_de_inscripcion, 'DD-MON-YY') BETWEEN '2020-09-01' AND '2021-02-01';