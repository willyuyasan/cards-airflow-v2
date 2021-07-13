select p.payout_id,
p.affiliate_id,
trim(replace(p.payout_name, char(9), '')) payout_name,
p.payment_type_id,
pt.label as payment_type_name,
p.amount,
p.process_time as process_date,
ifnull(p.status,'') as status,
ifnull(p.approval_date,'') as approval_date,
ifnull(p.insert_date,'') as insert_date,
p.reference,
p.deleted
from cccomus.partner_payouts p
join cccomus.partner_payment_types pt on (pt.payment_type_id = p.payment_type_id)
where p.approval_date < '2019-03-01';
