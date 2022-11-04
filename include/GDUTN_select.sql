select university,
career,
inscription_date,
nombre  as first_name,
null as last_name,
sexo as gender,
birth_date as age,
NULL as postal_code,
direccion as location,
email
from public.jujuy_utn
where university in ('universidad tecnológica nacional')
and to_date(inscription_date, 'YY/MM/DD')
between '2020/09/01' and '2021/02/01';
