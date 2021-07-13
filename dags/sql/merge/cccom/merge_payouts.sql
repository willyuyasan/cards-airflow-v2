begin transaction;

delete from cccom_dw.fact_payouts
where payout_id in (select payout_id from cccom_dw.stg_partner_payouts)
and origination_sys = 'REX';

insert into cccom_dw.fact_payouts (
payout_id,
affiliate_key,
payout_name,
payment_type_id,
payment_type_name,
amount,
process_date,
status,
approval_date,
insert_date,
reference,
origination_sys,
deleted
)
select s.payout_id,
nvl(a.affiliate_key, -1),
s.payout_name,
s.payment_type_id,
s.payment_type_name,
s.amount,
s.process_date,
s.status,
s.approval_date,
s.insert_date,
s.reference,
'REX',
s.deleted
from cccom_dw.stg_partner_payouts s
left join cccom_dw.dim_affiliates a on (a.affiliate_id = s.affiliate_id);

end transaction;