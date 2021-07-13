begin transaction;

insert into cccom_dw.fact_impressions_10min (
impression_id,
source_key,
impression_date_key,
impression_date,
affiliate_key,
total_count,
unique_count
)
select t.impression_id,
1,
to_number(to_char(date(t.impression_date),'YYYYMMDD'),'99999999') as impression_date_key,
t.impression_date,
nvl(a.affiliate_key, -1) as affiliate_key,
t.all_imps_count,
t.unique_imps_count
from (
  select s.* from cccom_dw.stg_impressions s
  where s.impression_id not in (select f.impression_id from cccom_dw.fact_impressions_10min f)
) t
left join cccom_dw.dim_affiliates a on (a.affiliate_id = t.affiliate_id);

end transaction;
