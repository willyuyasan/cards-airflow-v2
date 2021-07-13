begin transaction;

delete from cccom_dw.fact_sem_keyword_cost_daily
where (organization_name, start_date) in (
  select distinct s.organization_name, s.start_date
  from cccom_dw.stg_sem_keyword_cost s
);

insert into cccom_dw.fact_sem_keyword_cost_daily (
date_key,
start_date,
end_date,
keyword,
listing_match_type,
keyword_status,
portfolio,
search_engine,
campaign,
ad_group,
impressions,
cost,
clicks,
cpc,
avg_position,
epc,
organization_name,
device
)
select to_number(to_char(date(s.start_date),'YYYYMMDD'),'99999999') as date_key,
s.start_date,
s.end_date,
s.keyword,
s.listing_match_type,
s.keyword_status,
s.portfolio,
s.search_engine,
s.campaign,
s.ad_group,
s.impressions,
s.cost,
s.clicks,
s.cpc,
s.avg_position,
s.epc,
s.organization_name,
s.device
from cccom_dw.stg_sem_keyword_cost s;

end transaction;
