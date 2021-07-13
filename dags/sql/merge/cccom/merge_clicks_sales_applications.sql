begin transaction;

create table cccom_dw.airflow_fcsa_temp as select add_months(date_trunc('month', sysdate), -3) as calc_dt;

delete
from cccom_dw.fact_clicks_sales_applications
where click_date >= (select calc_dt from cccom_dw.airflow_fcsa_temp);

insert into cccom_dw.fact_clicks_sales_applications (
  click_id,
  click_date,
  product_key,
  affiliate_key,
  website_key,
  device_type_key,
  traffic_source_affiliate_key,
  exit_page_key,
  page_position_key,
  keyword_key,
  user_variable,
  referrer_url,
  campaign_id,
  apply_id,
  network,
  target_id,
  process_date,
  orig_base_revenue_cnt,
  orig_base_revenue_amt,
  base_revenue_cnt,
  base_revenue_amt,
  slotting_revenue_amt,
  est_slotting_revenue_amt,
  affiliate_slotting_revenue_amt,
  application_cnt,
  declined_application_cnt,
  base_commission,
  slotting_commission,
  affiliate_slotting_commission,
  last_touch_channel,
  last_touch_channel_detail,
  affiliate_classification,
  site_classification
)
select c.click_id,
trunc(c.click_date) as click_date,
c.product_key,
c.affiliate_key,
c.website_key,
c.device_type_key,
c.traffic_source_affiliate_key,
c.exit_page_key,
c.page_position_key,
c.keyword_key,
c.user_variable,
c.referrer_url,
c.campaign_id,
tam.apply_id,
c.network,
c.target_id,
s.provider_process_date,
nvl(s.orig_base_revenue_cnt,0) as orig_base_revenue_cnt,
nvl(s.orig_base_revenue_amt,0) as orig_base_revenue_amt,
nvl(s.base_revenue_cnt,0) as base_revenue_cnt,
nvl(s.base_revenue_amt,0) as base_revenue_amt,
nvl(s.slotting_revenue_amt,0) as slotting_revenue_amt,
nvl(est.estimated_revenue,0) as est_slotting_revenue_amt,
nvl(s.affiliate_slotting_revenue_amt,0) as affiliate_slotting_revenue_amt,
nvl(a.application_cnt,0) as application_cnt,
nvl(da.application_cnt,0) as declined_application_cnt,
nvl(s.base_commission,0) as base_commission,
nvl(s.slotting_commission,0) as slotting_commission,
nvl(s.affiliate_slotting_commission,0) as affiliate_slotting_commission,
c.last_touch_channel,
c.last_touch_channel_detail,
c.affiliate_classification,
c.site_classification
from ( -- Get last three months clicks for which we have received
       -- sales in REX prior to the cutover date 01-Feb-2019.
      select fc.*
      from cccom_dw.fact_clicks fc
      where fc.click_date >= (select calc_dt from cccom_dw.airflow_fcsa_temp)
      and exists (select 1
            from cccom_dw.fact_sales fs
            where fs.click_id = fc.click_id
            and   fs.provider_process_date < '01-Feb-2019'
          )) c
left join (
  select s.click_id,
  min(trunc(s.provider_process_date)) as provider_process_date,
  count(case when tt.trans_type in (4,60,95) then s.trans_id else null end) orig_base_revenue_cnt,
  count(case when tt.trans_type in (4,60,95) and s.count_as_sale = 1 then s.trans_id else null end) base_revenue_cnt,
  sum(case when tt.trans_type in (4,60,95) then s.estimated_revenue else 0.0 end) orig_base_revenue_amt,
  sum(case when tt.trans_type in (4,60,95) and s.count_as_sale = 1 then s.estimated_revenue else 0.0 end) base_revenue_amt,
  sum(case when tt.trans_type in (103,104) then s.estimated_revenue else 0.0 end) slotting_revenue_amt,
  sum(case when tt.trans_type in (105,106) then s.estimated_revenue else 0.0 end) affiliate_slotting_revenue_amt,
  sum(case when tt.trans_type in (4,60,95) then s.commission else 0.0 end) base_commission,
  sum(case when tt.trans_type in (103,104) then s.commission else 0.0 end) slotting_commission,
  sum(case when tt.trans_type in (105,106) then s.commission else 0.0 end) affiliate_slotting_commission
  from cccom_dw.fact_sales s
  join cccom_dw.dim_trans_types tt using (trans_type_key)
  where tt.trans_type in (4,60,95,103,104,105,106)
  group by s.click_id
) s on (s.click_id = c.click_id)
left join (
  select click_id, count(distinct application_id) application_cnt
  from cccom_dw.fact_applications
  where is_declined = 0
  and status = 'COMMITTED'
  group by click_id
) a on (a.click_id = c.click_id)
left join (
  select click_id, count(distinct application_id) application_cnt
  from cccom_dw.fact_applications
  where is_declined = 1
  and status = 'VALID'
  group by click_id
) da on (da.click_id = c.click_id)
left join (
  select click_id, sum(estimated_revenue) as estimated_revenue
  from cccom_dw.fact_est_slotting_trans
  where nullif(click_id,'') is not null
  group by click_id
) est on (est.click_id = c.click_id)
left join cccom_dw.map_trans_apply tam on (tam.trans_id = c.click_id);

drop table cccom_dw.airflow_fcsa_temp;

end transaction;