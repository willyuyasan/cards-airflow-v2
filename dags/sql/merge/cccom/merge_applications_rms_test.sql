begin transaction;

insert into cccom_dw.fact_applications_rms(
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
event_type_code
)
select
  application_id,
  1,
  lower(transaction_id) as click_id,
  upload_file_id,
  submission_date,
  to_number(to_char(submission_date,'YYYYMMDD'),'99999999') as submission_date_key,
  state,
  error_code,
  is_declined,
  last_updated,
  event_type_code
from (
  select * from cccom_dw.stg_applications_rms_test
  where state != 'DELETED'
  and application_id not in (
    select f.application_id from cccom_dw.fact_applications_rms f)
) t;


update cccom_dw.fact_applications_rms
set status = 'DELETED',
load_date = sysdate
where application_id in (
  select s.application_id from cccom_dw.stg_applications_rms_test s
  where s.state = 'DELETED'
);

end transaction;