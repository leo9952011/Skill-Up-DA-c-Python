-- Grupo de Universidades H
-- Necesito para U. Buenos Aires y U. de Cine, 
-- personas anotadas entre las fechas 01/9/2020 al 01/02/2021
--
-- university
-- career
-- inscription_date
-- * first_name
-- * last_name
-- gender
-- * age
-- postal_code
-- * location
-- email
--
-- las columnas que no tengo (*) las genero con valor NULL

-- Para Universidad de Buenos Aires
select
    universidades as university,
    carreras as career,
    fechas_de_inscripcion  as inscription_date,
    null as first_name,
    nombres  as last_name,
    fechas_nacimiento  as birth_date,
    sexo as gender,
    null as age,
    codigos_postales  as postal_code,
    null as location,
    emails as email 
from uba_kenedy
where to_date(fechas_de_inscripcion , 'YY-MON-DD') between '2020-09-01' and '2021-02-01'
and universidades  = 'universidad-de-buenos-aires';
