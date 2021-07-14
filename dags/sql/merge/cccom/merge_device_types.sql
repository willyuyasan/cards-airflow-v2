begin transaction;

update cccom_dw.dim_device_types
set name = s.name,
description = s.description,
load_date = sysdate
from cccom_dw.stg_device_types s
where s.device_type_id = dim_device_types.device_type_id;

insert into cccom_dw.dim_device_types(
device_type_key,
source_key,
device_type_id,
name,
description
)
select last_key + row_number() over (order by s.device_type_id),
1,
s.device_type_id,
s.name,
s.description
from cccom_dw.stg_device_types s
cross join (select nvl(max(device_type_key),100) as last_key from cccom_dw.dim_device_types)
where s.device_type_id not in (select d.device_type_id from cccom_dw.dim_device_types d);

end transaction;
