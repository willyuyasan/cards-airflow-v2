select payin_tier_card_assignment_id,
payin_tier_id,
card_id,
amount,
dynamic,
start_time,
ifnull(end_time,'') as end_time,
create_time,
update_time,
deleted,
ifnull(deleted_time,'') as deleted_time
from cccomus.payin_tier_card_assignments;
