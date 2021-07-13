update cccom_dw.dim_product_payin_tiers
set product_key = nvl(dp.product_key, -1),
amount = ptca.amount,
dynamic = ptca.dynamic,
start_date = date(ptca.start_time),
end_date = date(ptca.end_time),
deleted = ptca.deleted,
payin_tier_id = pt.payin_tier_id,
payin_tier_name = pt.payin_tier_name,
payin_tier_default = pt.is_default,
payin_tier_deleted = pt.deleted,
load_date = sysdate,
update_time = date(ptca.update_time)
from cccom_dw.stg_payin_tier_card_assignments ptca
join cccom_dw.stg_payin_tiers pt on (pt.payin_tier_id = ptca.payin_tier_id)
left join cccom_dw.dim_products dp on (dp.card_id = ptca.card_id)
where ptca.payin_tier_card_assignment_id = dim_product_payin_tiers.payin_tier_card_assignment_id
and (
dim_product_payin_tiers.product_key != nvl(dp.product_key, -1)
or dim_product_payin_tiers.amount != ptca.amount
or dim_product_payin_tiers.dynamic != ptca.dynamic
or dim_product_payin_tiers.start_date != date(ptca.start_time)
or dim_product_payin_tiers.end_date != date(ptca.end_time)
or (dim_product_payin_tiers.end_date is null and ptca.end_time is not null)
or (dim_product_payin_tiers.end_date is not null and ptca.end_time is null)
or dim_product_payin_tiers.deleted != ptca.deleted
or dim_product_payin_tiers.payin_tier_id != pt.payin_tier_id
or dim_product_payin_tiers.payin_tier_name != pt.payin_tier_name
or dim_product_payin_tiers.payin_tier_default != pt.is_default
or dim_product_payin_tiers.payin_tier_deleted != pt.deleted
or dim_product_payin_tiers.update_time != date(ptca.update_time)
or (dim_product_payin_tiers.update_time is null and ptca.update_time is not null)
or (dim_product_payin_tiers.update_time is not null and ptca.update_time is null)
);

insert into cccom_dw.dim_product_payin_tiers (
  payin_tier_card_assignment_id,
  product_key,
  amount,
  dynamic,
  start_date,
  end_date,
  deleted,
  payin_tier_id,
  payin_tier_name,
  payin_tier_default,
  payin_tier_deleted,
  update_time
)
select ptca.payin_tier_card_assignment_id,
nvl(dp.product_key, -1) as product_key,
ptca.amount,
ptca.dynamic,
date(ptca.start_time),
date(ptca.end_time),
ptca.deleted,
pt.payin_tier_id,
pt.payin_tier_name,
pt.is_default as payin_tier_default,
pt.deleted payin_tier_deleted,
date(ptca.update_time)
from cccom_dw.stg_payin_tier_card_assignments ptca
join cccom_dw.stg_payin_tiers pt on (pt.payin_tier_id = ptca.payin_tier_id)
left join cccom_dw.dim_products dp on (dp.card_id = ptca.card_id)
where ptca.payin_tier_card_assignment_id not in (
  select f.payin_tier_card_assignment_id from cccom_dw.dim_product_payin_tiers f
);
