select payin_tier_website_assignment_id,
payin_tier_id,
website_id,
start_time,
ifnull(end_time,'') as end_time,
create_time,
update_time,
deleted,
ifnull(deleted_time,'') as deleted_time
from cccomus.payin_tier_website_assignments;
