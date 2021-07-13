update cccom_dw.dim_product_groups
set name = s.name,
deleted = s.deleted,
parent_group_id = s.parent_card_group_id,
parent_name = s.parent_name,
parent_deleted = s.parent_deleted
from cccom_dw.stg_partner_card_groups s
where s.card_group_id = dim_product_groups.group_id
and (
  s.name != dim_product_groups.name
  or s.deleted != dim_product_groups.deleted
  or s.parent_card_group_id != dim_product_groups.parent_group_id
  or s.parent_name != dim_product_groups.parent_name
  or s.parent_deleted != dim_product_groups.parent_deleted
);

insert into cccom_dw.dim_product_groups(
  product_group_key,
  source_key,
  group_type,
  group_id,
  name,
  deleted,
  parent_group_id,
  parent_name,
  parent_deleted
)
select last_key + row_number() over (order by s.card_group_id),
1,
'PARTNER',
s.card_group_id,
s.name,
s.deleted,
s.parent_card_group_id,
s.parent_name,
s.parent_deleted
from cccom_dw.stg_partner_card_groups s
cross join (select nvl(max(product_group_key),100) as last_key from cccom_dw.dim_product_groups)
where s.card_group_id not in (select d.group_id from cccom_dw.dim_product_groups d);
