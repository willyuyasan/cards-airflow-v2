select trans_id,
apply_id,
ifnull(epc,''),
date_inserted
from transactions_apply_map
where date_inserted >= last_day(now()) + interval 1 day - interval 3 month;
