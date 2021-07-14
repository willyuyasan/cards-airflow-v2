begin transaction;

/*
-- RS date: 14-Jan-2019
-- delete of last three months clicks data in FCSA table is
-- heppening through the merge FCSA REX script.
-- When we cross three months from the cutover date, this logic
-- can be simplified by eleminating the merge FCSA REX script and
-- uncommenting the following delete statement.
delete
from cccom_dw.fact_clicks_sales_applications
where click_date >= add_months(date_trunc('month', sysdate), -3);

*/

create table cccom_dw.airflow_fcsa_temp as select add_months(date_trunc('month', sysdate), -3) as calc_dt;

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
from (  -- RS dt: 14-Jan-2019: This is the last minut work around introduced
        -- due to the cutover date logic change (from click_date to process_date)
        -- Get clicks from RMS after the cutover date based on provider_process_date and
        -- last three months clicks from fact_clicks for which we haven't received sales
        -- records in fact_sales (from REX and RMS).
        -- When we cross three months from the cutover date, replace the below union all query
        -- with the following commented query.
        /*
        select fc.*
        from cccom_dw.fact_clicks fc
        where fc.click_date >= add_months(date_trunc('month', sysdate), -3)
         */
        select fc.*
        from cccom_dw.fact_clicks fc
        where fc.click_date >= (select calc_dt from cccom_dw.airflow_fcsa_temp)
        and exists ( select 1
                    from cccom_dw.fact_sales fs
                    where nvl(fs.click_id,'x') = fc.click_id
                    and fs.provider_process_date >= '01-Feb-2019')
        union all
        select fc.*
        from cccom_dw.fact_clicks fc
        left join cccom_dw.fact_sales fs
        on ( fc.click_id = fs.click_id)
        where fc.click_date >= (select calc_dt from cccom_dw.airflow_fcsa_temp)
        and fs.click_id is null) c
left join (
  select s.click_id,
  min(trunc(s.provider_process_date)) as provider_process_date,
  count(case when event_type_code != 'REV' then 1 else null end) orig_base_revenue_cnt,
  count(case when event_type_code = 'REV' then 1 else null end) orig_base_reversal_cnt,
  count(case when s.count_as_sale = 1 then
          s.trans_id
          else null end) base_revenue_cnt,
  count(case when s.count_as_sale = 1
              and event_type_code = 'REV' then
            1
          else null end) base_reversal_cnt,
  sum(s.estimated_revenue) orig_base_revenue_amt,
  sum(case when s.count_as_sale = 1 and event_type_code != 'REV' then s.estimated_revenue else 0.0 end) base_revenue_amt,
  sum(s.commission) base_commission
  from cccom_dw.fact_sales s
  where s.click_date >= (select calc_dt from cccom_dw.airflow_fcsa_temp)
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
;

drop table cccom_dw.airflow_fcsa_temp;

end transaction;