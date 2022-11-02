SELECT nombrre, sexo, eemail, nacimiento, universidad, fechaiscripccion, carrerra, codgoposstal
	FROM public.moron_nacional_pampa 
	WHERE universidad='Universidad de morón' AND
	TO_DATE(fechaiscripccion,'DD/MM/YYYY') BETWEEN '2020-09-01' AND '2021-02-01';
	