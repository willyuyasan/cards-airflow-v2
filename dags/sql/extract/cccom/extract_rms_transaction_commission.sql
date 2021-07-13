
select  tec.id tec_id,
        t.tracking_id click_id,
        t.origination_dt click_date,
        te.id trans_id,
        tec.amount commission,
        tec.effective_start_dt effective_start_date,
        tec.effective_end_dt effective_end_date,
        tec.adjustment_id adjustment_id,
        tec.payment_id payment_id,
        case when (t.deleted or te.deleted or tec.deleted) then
          true
        else
          false
        end deleted
from    transactions.transaction_event_commission tec
join    transactions.transaction_events te
on (    te.id = tec.transaction_event_id)
join    transactions.transactions t
on (    te.transaction_id = t.id)
where   1 = 1
-- Cutover date logic: Get RMS transactions from 12/01/2018 onwards
and coalesce(te.issuer_process_dt,'01-jan-1980') >= '01-feb-2019'
 and (tec.create_dt >= date_trunc('month', now()::date) - interval '3 month'
  or tec.update_dt >= date_trunc('month', now()::date) - interval '3 month');