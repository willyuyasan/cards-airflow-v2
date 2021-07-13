insert into cccom_dw.map_trans_apply (
  trans_id,
  apply_id,
  epc,
  date_inserted
)
select lower(s.trans_id),
s.apply_id,
s.epc,
s.date_inserted
from cccom_dw.stg_trans_apply_map s
where s.date_inserted >= trunc(sysdate) - 7
and not exists (
  select 1
  from cccom_dw.map_trans_apply f
  where f.date_inserted >= trunc(sysdate) - 8
  and f.trans_id = lower(s.trans_id)
  and f.apply_id = s.apply_id
);
