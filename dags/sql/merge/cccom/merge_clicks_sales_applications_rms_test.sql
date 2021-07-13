begin transaction;

delete
from cccom_dw.fact_clicks_sales_applications_rms
where click_date >= add_months(date_trunc('month', sysdate), -3);

insert into cccom_dw.fact_clicks_sales_applications_rms (
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
null apply_id,
c.network,
c.target_id,
s.provider_process_date,
(nvl(s.orig_base_revenue_cnt,0) - nvl(s.orig_base_reversal_cnt,0)) as orig_base_revenue_cnt,
nvl(s.orig_base_revenue_amt,0) as orig_base_revenue_amt,
(nvl(s.base_revenue_cnt,0) - nvl(s.base_reversal_cnt,0)) as base_revenue_cnt,
nvl(s.base_revenue_amt,0) as base_revenue_amt,
0 as slotting_revenue_amt,
0 as est_slotting_revenue_amt,
0 as affiliate_slotting_revenue_amt,
nvl(a.application_cnt,0) as application_cnt,
nvl(da.application_cnt,0) as declined_application_cnt,
nvl(s.base_commission,0) as base_commission,
0 as slotting_commission,
0 as affiliate_slotting_commission,
c.last_touch_channel,
c.last_touch_channel_detail,
c.affiliate_classification,
c.site_classification
from (select fc.* from cccom_dw.fact_clicks fc where fc.click_date >= add_months(date_trunc('month', sysdate), -3)) c
left join (
  select s.click_id, 
  min(trunc(s.provider_process_date)) as provider_process_date,
  count(s.trans_id) orig_base_revenue_cnt,
  count(case when event_type_code = 'REV' then 1 else null end) orig_base_reversal_cnt,
  count(case when s.count_as_sale = 1 then
          s.trans_id
          else null end) base_revenue_cnt,
  count(case when s.count_as_sale = 1
              and event_type_code = 'REV' then
            1
          else null end) base_reversal_cnt,
  sum(s.estimated_revenue) orig_base_revenue_amt,
  sum(case when s.count_as_sale = 1 then s.estimated_revenue else 0.0 end) base_revenue_amt,
  sum(s.commission) base_commission
  from cccom_dw.fact_sales_rms s
  where s.click_date >= add_months(date_trunc('month', sysdate), -3)
  group by s.click_id
) s on (s.click_id = c.click_id)
left join (
  select click_id, count(distinct application_id) application_cnt
  from cccom_dw.fact_applications_rms
  where is_declined = 0
  and status = 'COMMITTED'
  group by click_id
) a on (a.click_id = c.click_id)
left join (
  select click_id, count(distinct application_id) application_cnt
  from cccom_dw.fact_applications_rms
  where is_declined = 1
  and status = 'VALID'
  group by click_id
) da on (da.click_id = c.click_id)
;

end transaction;
