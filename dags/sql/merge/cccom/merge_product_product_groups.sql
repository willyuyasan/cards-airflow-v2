insert into cccom_dw.map_product_product_groups (
  product_key,
  product_group_key,
  deleted
)
select p.product_key, pg.product_group_key, 0
from cccom_dw.stg_partner_card_group_card_map s
join cccom_dw.dim_product_groups pg on (pg.group_id = s.card_group_id)
join cccom_dw.dim_products p on (p.card_id = s.card_id) 
where (p.product_key, pg.product_group_key) not in (
  select map.product_key, map.product_group_key
  from cccom_dw.map_product_product_groups map
);
