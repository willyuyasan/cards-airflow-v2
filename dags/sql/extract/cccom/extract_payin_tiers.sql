select payin_tier_id,
issuer_id,
payin_tier_name,
left(ifnull(description,''),100) as description,
create_time,
update_time,
deleted,
`default` as is_default,
ifnull(deleted_time,'')
from cccomus.payin_tiers;
