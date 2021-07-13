begin transaction;

insert into cccom_dw.dim_product_types(
product_type_key,
source_key,
product_type_id,
product_type_name,
date_inserted,
deleted
)
select max_product_type_key + row_number() over (order by product_type_id),
1,
product_type_id,
product_type_name,
date_inserted,
deleted
from cccom_dw.stg_product_types
cross join (select nvl(max(product_type_key),100) as max_product_type_key from cccom_dw.dim_product_types)
where product_type_id not in (select product_type_id from cccom_dw.dim_product_types);

end transaction;
