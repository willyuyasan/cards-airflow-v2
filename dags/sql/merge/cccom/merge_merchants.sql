begin transaction;

update cccom_dw.dim_merchants
set merchant_name = s.merchant_name,
merchant_card_page = s.merchant_card_page,
default_payin_tier_id = s.default_payin_tier_id,
deleted = s.deleted,
load_date = sysdate
from cccom_dw.stg_merchants s
where s.merchant_id = dim_merchants.merchant_id;

insert into cccom_dw.dim_merchants (
merchant_key,
source_key,
merchant_id,
merchant_name,
merchant_card_page,
default_payin_tier_id,
deleted
)
select d.last_key + row_number() over (order by s.merchant_id),
1,
s.merchant_id,
s.merchant_name,
s.merchant_card_page,
s.default_payin_tier_id,
s.deleted
from (select nvl(max(merchant_key),100) last_key from cccom_dw.dim_merchants) d
cross join cccom_dw.stg_merchants s
where s.merchant_id not in (select d.merchant_id from cccom_dw.dim_merchants d);

end transaction;
