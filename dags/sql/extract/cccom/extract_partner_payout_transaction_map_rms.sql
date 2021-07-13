select p.id,
      tec.transaction_event_id
from transactions.payments p
join transactions.transaction_event_commission tec on tec.payment_id = p.id
where not p.deleted
and not tec.deleted
and p.approval_dt >= '2019-03-01';
