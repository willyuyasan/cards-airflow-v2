insert into cccom_dw.fact_tuna_responses (
response_id,
request_id,
insert_date,
error_codes,
approved_buckets
)
select s.response_id,
s.request_id,
s.insert_date,
s.error_codes,
s.approved_buckets
from cccom_dw.stg_tuna_responses s
where not exists (
  select 1 from cccom_dw.fact_tuna_responses f
  where f.response_id = s.response_id
);
