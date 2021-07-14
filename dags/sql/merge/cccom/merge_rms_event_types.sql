begin transaction;

update cccom_dw.dim_event_types
set event_type_id = s.event_type_id,
code = s.code,
description = s.description,
create_by = s.create_by,
create_at = s.create_at,
load_date = sysdate
from cccom_dw.stg_rms_event_types s
where s.event_type_id = dim_event_types.event_type_id;

insert into cccom_dw.dim_event_types(
event_type_id,
code,
description,
create_at,
create_by
)
select event_type_id,
code,
description,
create_at,
create_by
from cccom_dw.stg_rms_event_types sr
where sr.event_type_id not in (select event_type_id from cccom_dw.dim_event_types);

end transaction;
