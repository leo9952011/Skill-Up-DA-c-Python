select universidad as university,
careers as career,
fecha_de_inscripcion as inscription_date,
names as first_name,
null as last_name,
sexo as gender,
birth_dates as age,
codigo_postal as postal_code,
direcciones as location,
correos_electronicos as email
from public.palermo_tres_de_febrero
where universidad in ('universidad_nacional_de_tres_de_febrero')
and to_date(fecha_de_inscripcion, 'DD/Mon/YY')
between '01/Sep/20' and '01/Feb/21';

