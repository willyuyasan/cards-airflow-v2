update cccom_dw.map_product_websites
set website_key = nvl(w.website_key, -1),
product_key = nvl(p.product_key, -1),
status = s.status,
request_date = s.request_date,
approval_date = s.approval_date,
update_date = s.update_date,
load_date = sysdate
from cccom_dw.stg_partner_card_website_map s
left join cccom_dw.dim_websites w on (w.website_id = s.website_id)
left join cccom_dw.dim_products p on (p.card_id = s.card_id)
where s.partner_card_website_map_id = map_product_websites.partner_card_website_map_id
and (
  s.update_date != map_product_websites.update_date
  or s.status != map_product_websites.status
  or nvl(w.website_key, -1) != map_product_websites.website_key
  or nvl(p.product_key, -1) != map_product_websites.product_key
);

insert into cccom_dw.map_product_websites (
  partner_card_website_map_id,
  website_key,
  product_key,
  status,
  request_date,
  approval_date,
  update_date
)
select s.partner_card_website_map_id,
nvl(w.website_key, -1) as website_key,
nvl(p.product_key, -1) as product_key,
s.status,
s.request_date,
s.approval_date,
s.update_date
from cccom_dw.stg_partner_card_website_map s
left join cccom_dw.dim_websites w on (w.website_id = s.website_id)
left join cccom_dw.dim_products p on (p.card_id = s.card_id)
where s.partner_card_website_map_id not in (
  select m.partner_card_website_map_id
  from cccom_dw.map_product_websites m
);
