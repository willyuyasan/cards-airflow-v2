update cccom_dw.dim_website_payin_tiers
set website_key = nvl(dw.website_key, -1),
start_date = date(ptwa.start_time),
end_date = date(ptwa.end_time),
deleted = ptwa.deleted,
payin_tier_id = pt.payin_tier_id,
payin_tier_name = pt.payin_tier_name,
payin_tier_default = pt.is_default,
payin_tier_deleted = pt.deleted,
load_date = sysdate,
update_time = date(ptwa.update_time)
from cccom_dw.stg_payin_tier_website_assignments ptwa
join cccom_dw.stg_payin_tiers pt on (pt.payin_tier_id = ptwa.payin_tier_id)
left join cccom_dw.dim_websites dw on (dw.website_id = ptwa.website_id)
where ptwa.payin_tier_website_assignment_id = dim_website_payin_tiers.payin_tier_website_assignment_id
and (
dim_website_payin_tiers.website_key != nvl(dw.website_key, -1)
or dim_website_payin_tiers.start_date != date(ptwa.start_time)
or dim_website_payin_tiers.end_date != date(ptwa.end_time)
or (dim_website_payin_tiers.end_date is null and ptwa.end_time is not null)
or (dim_website_payin_tiers.end_date is not null and ptwa.end_time is null)
or dim_website_payin_tiers.deleted != ptwa.deleted
or dim_website_payin_tiers.payin_tier_id != pt.payin_tier_id
or dim_website_payin_tiers.payin_tier_name != pt.payin_tier_name
or dim_website_payin_tiers.payin_tier_default != pt.is_default
or dim_website_payin_tiers.payin_tier_deleted != pt.deleted
or dim_website_payin_tiers.update_time != date(ptwa.update_time)
or (dim_website_payin_tiers.update_time is null and ptwa.update_time is not null)
or (dim_website_payin_tiers.update_time is not null and ptwa.update_time is null)
);

insert into cccom_dw.dim_website_payin_tiers (
  payin_tier_website_assignment_id,
  website_key,
  start_date,
  end_date,
  deleted,
  payin_tier_id,
  payin_tier_name,
  payin_tier_default,
  payin_tier_deleted,
  update_time
)
select ptwa.payin_tier_website_assignment_id,
nvl(dw.website_key, -1) as website_key,
date(ptwa.start_time),
date(ptwa.end_time),
ptwa.deleted,
pt.payin_tier_id,
pt.payin_tier_name,
pt.is_default as payin_tier_default,
pt.deleted payin_tier_deleted,
date(ptwa.update_time)
from cccom_dw.stg_payin_tier_website_assignments ptwa
join cccom_dw.stg_payin_tiers pt on (pt.payin_tier_id = ptwa.payin_tier_id)
left join cccom_dw.dim_websites dw on (dw.website_id = ptwa.website_id)
where ptwa.payin_tier_website_assignment_id not in (
  select f.payin_tier_website_assignment_id from cccom_dw.dim_website_payin_tiers f
);
