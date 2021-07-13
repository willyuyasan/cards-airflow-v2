select
    p.id as payout_id,
    p.affiliate_id,
    p.affiliate_name,
    '' as payment_type_id,
    '' as payment_type_name,
    p.total as amount,
    p.payment_dt as process_date,
    lower(p.approval_status::text) as status,
    p.approval_dt as approval_date,
    p.create_dt as insert_date,
    '' as reference,
    case 
       when p.deleted = 'false' then  0
       else 1
    end deleted
from transactions.payments p
where p.approval_dt >= '2019-03-01';