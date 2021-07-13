#TODO selection criteria
select tm.payout_id,
tm.transaction_id
from cccomus.partner_payout_transaction_map tm
join cccomus.partner_payouts p on p.payout_id = tm.payout_id
where p.approval_date < '2019-03-01';
