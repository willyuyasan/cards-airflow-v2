insert into cccom_dw.fact_tuna_responses (
response_id,
request_id,
insert_date,
utc_date,
error_codes,
approved_buckets
)
select -1,
s.request_id,
convert_timezone('US/Central', s.utc_date),
s.utc_date,
s.error_codes,
s.approved_buckets
from cccom_dw.stg_tuna_responses_sumo s
where not exists (
  select 1 from cccom_dw.fact_tuna_responses f
  where f.request_id = s.request_id and f.utc_date = s.utc_date
);
