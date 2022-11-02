-- Grupo de Universidades B
-- Necesito para UNComahue y UdelSalvador, 
-- personas anotadas entre las fechas 01/9/2020 al 01/02/2021
-- traigo todas las columnas
--
-- TO_DATE <https://www.postgresqltutorial.com/postgresql-date-functions/postgresql-to_date/>
--
-- Para UNComahue

SELECT *
FROM public.flores_comahue
WHERE universidad = 'UNIV. NACIONAL DEL COMAHUE'
	AND TO_DATE(fecha_de_inscripcion,'YYYY-MM-DD') BETWEEN '2020/09/01' AND '2021/02/01'
