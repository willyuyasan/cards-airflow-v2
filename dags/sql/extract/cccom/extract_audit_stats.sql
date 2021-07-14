select 'affiliates' as entity, '' as sub_type, cast(date_format(time_inserted,'%Y-%m-01') as date) dt, count(*)
from cccomus.partner_affiliates
group by dt
union all
select 'keywords' as entity, '' as sub_type, cast(date_format(k.insert_time,'%Y-%m-01') as date) dt, count(*)
from cccomus.keywords k
join cccomus.keyword_text kt on (kt.keyword_text_id = k.keyword_text_id)
group by dt
union all
select 'merchants' as entity, '' as sub_type, curdate() dt, count(*)
from cccomus.cms_merchants
union all
select 'payin_tier_card_assignments' as entity, '' as sub_type, cast(date_format(start_time,'%Y-%m-01') as date) dt, count(*)
from cccomus.payin_tier_card_assignments
group by dt
union all
select 'payin_tier_website_assignments' as entity, '' as sub_type, cast(date_format(start_time,'%Y-%m-01') as date) dt, count(*)
from cccomus.payin_tier_website_assignments
group by dt
union all
select 'products' as entity, '' as sub_type, cast(date_format(dateCreated,'%Y-%m-01') as date) dt, count(*)
from cccomus.cms_cards
group by dt
union all
select 'product_types' as entity, '' as sub_type, cast(date_format(date_inserted,'%Y-%m-01') as date) dt, count(*)
from cccomus.cms_product_types
group by dt
union all
select 'pages' as entity, '' as sub_type, cast(date_format(insert_time,'%Y-%m-01') as date) dt, count(*)
from cccomus.pages
group by dt
union all
select 'programs' as entity, '' as sub_type, cast(date_format(date_created,'%Y-%m-01') as date) dt, count(*)
from cccomus.cms_programs
group by dt
union all
select 'websites' as entity, '' as sub_type, curdate() dt, count(*)
from cccomus.partner_websites
union all
select 'clicks' as entity, '' as sub_type, date(date_inserted) dt, count(*)
from cccomus.transactions_click
where date_inserted >= (last_day(now()) + interval 1 day - interval 3 month)
group by dt
union all
select 'sales' as entity, transtype as sub_type, date(providerprocessdate) dt, count(*)
from (
  select transtype, providerprocessdate
  from cccomus.transactions_recent
  where transtype != 1
  and providerprocessdate >= (last_day(now()) + interval 1 day - interval 3 month)
  union all
  select transtype, providerprocessdate
  from cccomus.transactions_upload
  where transtype != 1
  and providerprocessdate >= (last_day(now()) + interval 1 day - interval 3 month)
) t
group by sub_type, dt
union all
select 'transactions_apply_map' as entity, '' as sub_type, date(date_inserted) dt, count(*)
from cccomus.transactions_apply_map
where date_inserted >= last_day(now()) + interval 1 day - interval 3 month
group by dt
union all
select 'applications' as entity, 'Submitted' as sub_type, date(last_updated) dt, count(*)
from cccomus.applications
where last_updated >= last_day(now()) + interval 1 day - interval 3 month
and ifnull(state,'') != 'DELETED'
group by dt
union all
select 'applications' as entity, 'Declined' as sub_type, date(provider_process_time) dt, count(*)
from cccomus.declined_applications
where provider_process_time >= last_day(now()) + interval 1 day - interval 3 month
group by dt
