begin transaction;

update cccom_dw.dim_products
set site_code = s.site_code,
card_title = s.card_title,
card_description = s.card_description,
merchant_key = nvl(m.merchant_key, -1),
merchant_id = nvl(s.merchant_id, -1),
merchant_name = nvl(m.merchant_name, 'Unknown'),
program_key = nvl(p.program_key, -1),
credit_needed_key = nvl(c.credit_needed_key, -1),
credit_needed_id = nvl(s.credit_needed_id, -1),
credit_needed_name = nvl(c.description, 'Unknown'),
syndicate = s.syndicate,
private = s.private,
active = s.active,
deleted = s.deleted,
commission_label = s.commission_label,
payout_cap = s.payout_cap,
card_level_id = s.card_level_id,
requires_approval = s.requires_approval,
secured = s.secured,
network_id = s.network_id,
product_type_key = nvl(pt.product_type_key, -1),
suppress_mobile = s.suppress_mobile,
create_date = s.create_date,
update_date = s.update_date,
partner_card_create_date = s.partner_card_create_date,
partner_card_status = s.partner_card_status,
partner_card_deleted = s.partner_card_deleted,
load_date = sysdate
from cccom_dw.stg_products s
left join cccom_dw.dim_merchants m on (m.merchant_id = s.merchant_id)
left join cccom_dw.dim_programs p on (p.program_id = s.program_id)
left join cccom_dw.dim_product_types pt on (pt.product_type_id = s.product_type_id)
left join cccom_dw.dim_credit_needed c on (c.credit_needed_id = s.credit_needed_id)
where s.card_id = cccom_dw.dim_products.card_id
and (
  s.update_date >= dim_products.update_date
  or nvl(s.partner_card_status,'') != nvl(dim_products.partner_card_status,'')
  or nvl(s.partner_card_create_date,'1970-01-01') != nvl(dim_products.partner_card_create_date, '1970-01-01')
  or nvl(s.partner_card_deleted, -1) != nvl(dim_products.partner_card_deleted, -1)
);


insert into cccom_dw.dim_products(
product_key,
source_key,
card_id,
site_code,
card_title,
card_description,
merchant_key,
merchant_id,
merchant_name,
program_key,
credit_needed_key,
credit_needed_id,
credit_needed_name,
syndicate,
private,
active,
deleted,
commission_label,
payout_cap,
card_level_id,
requires_approval,
secured,
network_id,
product_type_key,
suppress_mobile,
create_date,
update_date,
partner_card_create_date,
partner_card_status,
partner_card_deleted
)
select last_key + row_number() over (order by s.create_date, s.card_id),
1,
s.card_id,
s.site_code,
s.card_title,
s.card_description,
nvl(m.merchant_key, -1),
nvl(s.merchant_id, -1),
nvl(m.merchant_name, 'Unknown'),
nvl(p.program_key, -1),
nvl(c.credit_needed_key, -1),
nvl(s.credit_needed_id, -1),
nvl(c.description, 'Unknown'),
s.syndicate,
s.private,
s.active,
s.deleted,
s.commission_label,
s.payout_cap,
s.card_level_id,
s.requires_approval,
s.secured,
s.network_id,
nvl(pt.product_type_key, -1),
s.suppress_mobile,
s.create_date,
s.update_date,
partner_card_create_date,
partner_card_status,
partner_card_deleted
from cccom_dw.stg_products s
cross join (select nvl(max(product_key),100) as last_key from cccom_dw.dim_products)
left join cccom_dw.dim_merchants m on (m.merchant_id = s.merchant_id)
left join cccom_dw.dim_programs p on (p.program_id = s.program_id)
left join cccom_dw.dim_product_types pt on (pt.product_type_id = s.product_type_id)
left join cccom_dw.dim_credit_needed c on (c.credit_needed_id = s.credit_needed_id)
where s.card_id not in (select d.card_id from cccom_dw.dim_products d);

end transaction;
