SELECT names, sexo, email, fechas_nacimiento, univiersities, inscription_dates, carrera, localidad 
	FROM public.rio_cuarto_interamericana
	WHERE univiersities='Universidad-nacional-de-río-cuarto' AND
	TO_DATE(inscription_dates,'YY/MON/DD') BETWEEN '2020-09-01' AND '2021-02-01';
	