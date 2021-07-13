select te.id as application_id,
  coalesce(t.tracking_id, '') as click_id,
  '' as upload_file_id,
  coalesce(te.provider_event_dt,te.issuer_process_dt) as submission_date,
  case
    when te.deleted then
         'DELETED'
    else
       case
         when te.event_type_code = 'APP' then
            'COMMITTED'
         else
            'VALID'
       end 
  end as status,
  te.update_dt as last_updated,
  case
    WHEN te.event_type_code = 'DEC' then 1
    else 0
  end as is_declined,
  '' as error_code,
 te.event_type_code
from transactions.transaction_events te
     join transactions.transactions t on te.transaction_id = t.id
     left outer join transactions.transaction_event_issues tei on tei.transaction_event_id = te.id
where te.event_type_code in ( 'APP','DEC')
  and tei.id is null;