insert into cccom_dw.fact_commission_rates (
log_id,
user_id,
user_name,
affiliate_key,
product_key,
website_key,
old_rate,
old_start_date,
old_end_date,
new_rate,
new_start_date,
new_end_date,
payin_rate,
rate_type,
rate_id,
action,
create_date
)
select s.log_id,
s.user_id,
s.user_name,
nvl(af.affiliate_key, -1),
nvl(p.product_key, -1),
nvl(w.website_key, -1),
s.old_rate,
s.old_start_date,
s.old_end_date,
s.new_rate,
s.new_start_date,
s.new_end_date,
s.payin_rate,
s.rate_type,
s.rate_id,
s.action,
s.date_created
from (
  select s0.* from cccom_dw.stg_partner_commission_rates_log s0
  where s0.log_id not in (select f.log_id from cccom_dw.fact_commission_rates f)
) s
left join cccom_dw.dim_products p on (p.card_id = s.card_id)
left join cccom_dw.dim_affiliates af on (af.affiliate_id = s.affiliate_id)
left join cccom_dw.dim_websites w on (w.website_id = s.website_id);
