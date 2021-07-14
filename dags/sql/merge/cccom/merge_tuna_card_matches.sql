insert into cccom_dw.fact_tuna_card_matches_hourly (
insert_date,
product_key,
bucket_id,
matches
)
select s.insert_date,
nvl(p.product_key, -1) as product_key,
s.bucket_id,
s.matches
from cccom_dw.stg_tuna_card_matches_by_hour s
left join cccom_dw.dim_products p on (p.card_id = s.card_id)
where not exists (
  select 1
  from cccom_dw.fact_tuna_card_matches_hourly f
  where f.insert_date = s.insert_date
  and f.product_key = p.product_key
  and f.bucket_id = s.bucket_id
);

insert into cccom_dw.fact_tuna_card_matches_daily (
insert_date,
product_key,
bucket_id,
matches
)
select trunc(convert_timezone('US/Central', fh.insert_date)) as day_ct,
fh.product_key,
fh.bucket_id,
sum(fh.matches)
from cccom_dw.fact_tuna_card_matches_hourly fh
where trunc(convert_timezone('US/Central', fh.insert_date)) in
(
  select dt
  from (
    select dt, first_hour, last_hour,
    lag(dt) over (order by dt) prev_dt, lead(dt) over (order by dt) next_dt
    from
    (
      select trunc(insert_date_ct) as dt,
      min(extract(hour from insert_date_ct)) first_hour,
      max(extract(hour from insert_date_ct)) last_hour
      from (
        select convert_timezone('US/Central', insert_date) as insert_date_ct
        from cccom_dw.fact_tuna_card_matches_hourly
      )
      group by dt
    )
  ) t
  where (
    (first_hour = 0 or prev_dt is not null)
    and
    (last_hour = 23 or next_dt is not null)
  )
)
and trunc(convert_timezone('US/Central', fh.insert_date)) not in
(
  select insert_date from cccom_dw.fact_tuna_card_matches_daily
)
group by day_ct,
fh.product_key,
fh.bucket_id
