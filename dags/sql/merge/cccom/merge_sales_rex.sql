begin transaction;

delete from cccom_dw.fact_sales_rex
where trans_id in (
  select s.trans_id from cccom_dw.stg_sale_trans_rex s
);

insert into cccom_dw.fact_sales_rex(
trans_id,
source_key,
click_id,
click_date,
click_datetime,
trans_type_key,
trans_type,
payout_status,
payout_date_key,
payout_date,
order_id,
product_key,
--banner_id,
affiliate_key,
--affiliate_id,
camp_category_id,
parent_trans_id,
commission,
ip,
recurring_comm_id,
cross_sale_product_key,
--data2,
campaign_id,
keyword_key,
exit_page_key,
--exit_page_id,
page_position_key,
provider_event_date_key,
provider_event_date,
provider_process_date_key,
provider_process_date,
provider_process_datetime,
merchant_name,
provider_id,
quantity,
provider_channel,
estimated_revenue,
date_estimated,
provider_status,
provider_corrected,
provider_websiteid,
provider_websitename,
provider_action_id,
provider_action_name,
product_sku,
date_adjusted,
external_visit_id,
ref_inception_date,
source_table_id,
referrer_url,
count_as_sale,
count_as_sale_comments,
estimated_data_filename
)
select
t.trans_id,
1,
lower(t.ref_trans),
date_trunc('day', t.date_inserted),
t.date_inserted,
nvl(tt.trans_type_key, -1),
nvl(tt.trans_type,-1),
t.payout_status,
case when t.date_payout_str is null or t.date_payout_str = '' or t.date_payout_str = '0000-00-00 00:00:00' then null else to_number(to_char(date(t.date_payout_str),'YYYYMMDD'),'99999999') end,
case when t.date_payout_str is null or t.date_payout_str = '' or t.date_payout_str = '0000-00-00 00:00:00' then null else date(t.date_payout_str) end,
t.order_id,
nvl(p.product_key,-1),
--t.banner_id,
nvl(a.affiliate_key,-1),
--t.affiliate_id,
t.camp_category_id,
t.parent_trans_id,
t.commission,
t.ip,
t.recurring_comm_id,
nvl(csp.product_key,-1),
--t.data2,
t.channel,
nvl(k.keyword_key,-1),
nvl(ep.page_key,-1),
--t.exit_page_id,
nvl(pp.page_position_key,-1),
to_number(to_char(date(t.provider_event_date),'YYYYMMDD'),'99999999'),
t.provider_event_date,
to_number(to_char(date(t.provider_process_date),'YYYYMMDD'),'99999999'),
date_trunc('day', t.provider_process_date),
t.provider_process_date,
t.merchant_name,
t.provider_id,
nvl(t.quantity,0),
t.provider_channel,
nvl(t.estimated_revenue,0),
t.date_estimated,
t.provider_status,
t.provider_corrected,
t.provider_websiteid,
t.provider_websitename,
t.provider_action_id,
t.provider_action_name,
t.product_id,
t.date_adjusted,
t.external_visit_id,
case when t.ref_inception_date_str is null or t.ref_inception_date_str = '' or t.ref_inception_date_str = '0000-00-00 00:00:00' then null else date(t.ref_inception_date_str) end,
t.source_table_id,
trim(t.referrer_url)::varchar(300),
case 
  when dc.stg_sales_column = 'provider_action_name' and cccom_dw.f_regexp_like(nvl(t.provider_action_name,''), dc.stg_sales_regex) then 0
  when dc.stg_sales_column = 'product_id' and cccom_dw.f_regexp_like(nvl(t.product_id,''), dc.stg_sales_regex) then 0
  else 1
end as count_as_sale,
-- TODO: Set comments without having to re-execute the regex  
case 
  when dc.stg_sales_column = 'provider_action_name' and cccom_dw.f_regexp_like(nvl(t.provider_action_name,''), dc.stg_sales_regex) then dc.exclusion_type
  when dc.stg_sales_column = 'product_id' and cccom_dw.f_regexp_like(nvl(t.product_id,''), dc.stg_sales_regex) then dc.exclusion_type
  else ''
end as count_as_sale_comments,
t.estimated_data_filename
from (select s.* from cccom_dw.stg_sale_trans_rex s where s.trans_id not in (select f.trans_id from cccom_dw.fact_sales_rex f)) t
left join cccom_dw.dim_products p on (p.card_id = t.banner_id)
left join cccom_dw.dim_affiliates a on (a.affiliate_id = t.affiliate_id)
left join cccom_dw.dim_products csp on (csp.card_id = t.data2)
left join cccom_dw.dim_pages ep on (ep.page_id = t.exit_page_id)
left join cccom_dw.dim_page_positions pp on (pp.page_position_key = t.page_position)
left join cccom_dw.dim_trans_types tt on (tt.trans_type = t.trans_type)
left join cccom_dw.dim_keywords k on (k.keyword_id = t.episode)
left join cccom_dw.ctl_sales_exclusions dc on (
  dc.merchant_key = p.merchant_key
  and nvl(dc.product_key, p.product_key) = p.product_key 
  and trunc(sysdate) between dc.start_date and nvl(dc.end_date, trunc(sysdate))
);

delete from cccom_dw.fact_sales_rex
where provider_process_datetime >= (select min(s.provider_process_date) from cccom_dw.stg_sale_trans_rex s)
and trans_id not in (select stg.trans_id from cccom_dw.stg_sale_trans_rex stg);

end transaction;