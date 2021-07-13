begin transaction;

update cccom_dw.dim_trans_types
set label = s.label,
description = s.description,
count_as_sale = s.count_as_sale,
sum_as_revenue = s.sum_as_revenue,
calculate_commission = s.calculate_commission,
is_rev_split = s.is_rev_split,
sum_as_epc = s.sum_as_epc,
is_paid_bonus = s.is_paid_bonus,
load_date = sysdate
from cccom_dw.stg_trans_types s
where s.trans_type = dim_trans_types.trans_type;

insert into cccom_dw.dim_trans_types(
trans_type_key,
source_key,
trans_type,
label,
description,
count_as_sale,
sum_as_revenue,
calculate_commission,
is_rev_split,
sum_as_epc,
is_paid_bonus
)
select last_key + row_number() over (order by s.trans_type),
1,
s.trans_type,
s.label,
s.description,
s.count_as_sale,s.sum_as_revenue,
s.calculate_commission,
s.is_rev_split,
s.sum_as_epc,
s.is_paid_bonus
from cccom_dw.stg_trans_types s
cross join (select nvl(max(trans_type_key),100) as last_key from cccom_dw.dim_trans_types)
where s.trans_type not in (select d.trans_type from cccom_dw.dim_trans_types d);

end transaction;
