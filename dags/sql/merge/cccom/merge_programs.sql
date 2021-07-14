begin transaction;

update cccom_dw.dim_programs
set program_name = s.program_name,
program_default = s.program_default,
--TODO: merchant_key
date_created = s.date_created,
deleted = s.deleted,
private = s.private,
load_date = sysdate
from cccom_dw.stg_programs s
where s.program_id = dim_programs.program_id;

insert into cccom_dw.dim_programs(
program_key,
source_key,
program_id,
program_name,
merchant_key,
program_default,
date_created,
deleted,
private
)
select max_program_key + row_number() over (order by p.program_id),
1,
p.program_id,
p.program_name,
nvl(m.merchant_key, -1),
p.program_default,
p.date_created,
p.deleted,
p.private
from cccom_dw.stg_programs p
left join cccom_dw.dim_merchants m on (m.merchant_id = p.issuer_id)
cross join (select nvl(max(program_key),100) as max_program_key from cccom_dw.dim_programs)
where p.program_id not in (select d.program_id from cccom_dw.dim_programs d);

end transaction;
