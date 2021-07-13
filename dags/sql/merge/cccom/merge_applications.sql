insert into cccom_dw.fact_applications(
application_id,
source_key,
click_id,
upload_file_id,
submission_date,
submission_date_key,
status,
is_declined,
last_updated,
origination_sys
)
select
  application_id,
  1,
  lower(transaction_id) as click_id,
  upload_file_id,
  submission_date,
  to_number(to_char(submission_date,'YYYYMMDD'),'99999999') as submission_date_key,
  state,
  0,
  last_updated,
  'REX'
from (
  select * from cccom_dw.stg_applications
  where state != 'DELETED'
  and application_id not in (
    select f.application_id from cccom_dw.fact_applications f where f.origination_sys = 'REX')
) t;

update cccom_dw.fact_applications
set status = 'DELETED',
load_date = sysdate
where origination_sys = 'REX'
  and application_id in (
  select s.application_id from cccom_dw.stg_applications s
  where s.state = 'DELETED'
);

insert into cccom_dw.fact_applications(
application_id,
source_key,
click_id,
upload_file_id,
submission_date,
submission_date_key,
status,
error_code,
is_declined,
last_updated,
origination_sys
)
select
  declined_application_id,
  1,
  lower(transaction_id) as click_id,
  upload_file_id,
  nvl(provider_process_time, '1970-01-01'::timestamp),
  case
    when provider_process_time is null then 19700101
    else to_number(to_char(provider_process_time,'YYYYMMDD'),'99999999')
  end as submission_date_key,
  state,
  error_code,
  1,
  nvl(update_time, '1970-01-01'::timestamp),
  'REX'
from (
  select * from cccom_dw.stg_declined_applications
  where state != 'DELETED'
  and declined_application_id not in (
    select f.application_id from cccom_dw.fact_applications f where f.origination_sys = 'REX')
) t;

update cccom_dw.fact_applications
set status = 'DELETED',
load_date = sysdate
where origination_sys='REX'
  and application_id in (
  select s.declined_application_id from cccom_dw.stg_declined_applications s
  where s.state = 'DELETED'
);


delete from cccom_dw.fact_applications
where origination_sys='REX'
and submission_date >= add_months(date_trunc('month', sysdate), -3)
and is_declined = 0
and application_id not in (
    select application_id from cccom_dw.stg_applications );