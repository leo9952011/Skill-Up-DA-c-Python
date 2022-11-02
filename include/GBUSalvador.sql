-- Grupo de Universidades B
-- Necesito para UNComahue y UdelSalvador, 
-- personas anotadas entre las fechas 01/9/2020 al 01/02/2021
-- traigo todas las columnas
--
-- TO_DATE <https://www.postgresqltutorial.com/postgresql-date-functions/postgresql-to_date/>
--
-- Para UdelSalvador

SELECT *
FROM public.salvador_villa_maria
WHERE universidad = 'UNIVERSIDAD_DEL_SALVADOR'
	AND TO_DATE(fecha_de_inscripcion,'DD-Mon-YY') BETWEEN '2020/09/01' AND '2021/02/01'


