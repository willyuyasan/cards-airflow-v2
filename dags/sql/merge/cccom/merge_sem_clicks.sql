-- TODO: handle updates
begin transaction;

insert into cccom_dw.fact_sem_clicks(
organization_name,
trans_id,
click_date,
click_id,
impression_date,
exit_page_key,
product_key,
client_transaction_id,
keyword,
portfolio,
search_engine,
campaign,
ad_group,
device,
device_type_key,
offer_click,
click_match_type,
source_key
)
select t.organization_name,
t.transaction_id,
t.transaction_time as click_date,
lower(tam.trans_id) as click_id,
t.click_impression_time as impression_date,
nvl(pg.page_key,-1) as exit_page_key,
nvl(p.product_key,-1) as product_key,
t.client_transaction_id,
t.keyword,
t.portfolio,
t.search_engine,
t.campaign,
t.ad_group,
t.device,
case t.device
  when 'Computers' then 101
  when 'Mobile' then 102
  when 'Tablets' then 103
  else -1
end as device_type_key,
t.offer_click,
t.click_match_type,
2 as source_key
from (
  select s.* from cccom_dw.stg_sem_clicks s
  where not exists (
    select 1
    from cccom_dw.fact_sem_clicks f
    where f.organization_name = s.organization_name
    and f.client_transaction_id = s.client_transaction_id
    and f.impression_date = s.click_impression_time
    and f.click_date = s.transaction_time
    and f.offer_click = s.offer_click
    and f.source_key = 2
  )
) t
left join cccom_dw.dim_pages pg on (pg.page_id = t.page_id)
left join cccom_dw.dim_products p on (p.card_id = t.banner_id)
left join cccom_dw.stg_trans_apply_map tam on (tam.apply_id = t.transaction_id);

end transaction;
