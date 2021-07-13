-- TODO: Handle updates

Begin;

insert into cccom_dw.fact_clicks (
  click_id,
  source_key,
  click_date_key,
  click_hour_key,
  product_key,
  affiliate_key,
  website_key,
  device_type_key,
  traffic_source_affiliate_key,
  exit_page_key,
  page_position_key,
  click_date,
  click_serial_id,
  outside_id,
  ip_address,
  commission_rate_id,
  cardmatch_offer_id,
  user_variable,
  campaign_id,
  external_visit_id,
  keyword_key,
  apply_id,
  referrer_url
)
select
  lower(t.tracking_id) as click_id,
  1,
  to_number(to_char(t.date_inserted,'YYYYMMDD'),'99999999') as click_date_key,
  extract(hour from t.date_inserted),
  nvl(prod.product_key,-1),
  nvl(a1.affiliate_key,-1),
  nvl(w.website_key,-1),
  nvl(dt.device_type_key,-1),
  nvl(a2.affiliate_key,-1),
  nvl(p.page_key,-1),
  nvl(page_position,-1),
  t.date_inserted as click_date,
  t.click_id as click_serial_id,
  t.outside_id,
  t.ip_address,
  t.commission_rate_id,
  t.cardmatch_offer_id,
  t.user_variable,
  t.campaign_id,
  t.external_visit_id,
  nvl(k.keyword_key,-1),
  tam.apply_id,
  trim(t.referrer_url)::varchar(300)
from (select * from cccom_dw.stg_click_trans where lower(tracking_id) not in (select f.click_id from cccom_dw.fact_clicks f)) t
left join cccom_dw.dim_affiliates a1 on (a1.affiliate_id = t.affiliate_id)
left join cccom_dw.dim_affiliates a2 on (a2.affiliate_id = t.traffic_source_id)
left join cccom_dw.dim_websites w on (w.website_id = t.website_id)
left join cccom_dw.dim_pages p on (p.page_id = t.exit_page_id)
left join cccom_dw.dim_products prod on (prod.card_id = t.product_id)
left join cccom_dw.dim_device_types dt on (dt.device_type_id = t.device_type_id)
left join cccom_dw.dim_keywords k on (k.keyword_id = t.keyword_id)
left join cccom_dw.map_trans_apply tam on (tam.trans_id = t.tracking_id);

-- Update apply_id
update cccom_dw.fact_clicks
set apply_id = tam.apply_id
from cccom_dw.map_trans_apply tam
where fact_clicks.apply_id is null
and tam.trans_id = fact_clicks.click_id;

-- Update site_classification
-- updates on 05/18/2020 , logic of updating SC column has been changed as follows and new logic 
-- is based on the chart below:
-- updated 11/20/2020 based on CCDC updated tracking
/*
website + aff type + affiliate + SC
--------+----------+-----------+-----------
26814     PARTNER      1046404     TPG
26814     PARTNER    <>1046404     TPG Partner
44580     PARTNER      1046404     TPG
44580     PARTNER    <>1046404     TPG Partner  
40060     TRA SRC            *     CCCom Core
40060     PARTNER       127714     CCCom NextAdvisor
40060     PARTNER      1046404     TPG
40060     PARTNER    <>1046404     CCCom Banner
40060     PARTNER       104000     CCCom Core
 5696     TRA SRC            *     CCCom Core
 5696     PARTNER       127714     CCCom NextAdvisor
 5696     PARTNER      1046404     TPG
 5696     PARTNER    <>1046404     CCCom Banner
 5696     PARTNER       104000     CCCom Core
Other     PARTNER      1046404     TPG
Other     PARTNER    <>1046404     Affiliate 
*/

-- affiliate_type = partner and affiliate_id = 1046404 
update cccom_dw.fact_clicks
   set site_classification = c.site_classification
from (
select lower(stg.tracking_id) as click_id,
       a.affiliate_key,
       ctl.site_classification
  from cccom_dw.stg_click_trans stg
  join cccom_dw.dim_affiliates a on a.affiliate_id = stg.affiliate_id
       and a.affiliate_type = 'PARTNER' and a.affiliate_id = '1046404'
  join cccom_dw.dim_websites w on w.website_id = stg.website_id
  join cccom_dw.ctl_site_classification ctl on ctl.website_id = 'ALL'
       and ctl.affiliate_id = a.affiliate_id and ctl.affiliate_type = 'ALL'
       and ctl.is_active = 'Y' 
     ) c
where fact_clicks.click_id = c.click_id
  and fact_clicks.affiliate_key = c.affiliate_key
  and nullif(fact_clicks.site_classification,'') is null;

-- affiliate_type = PARTNER and affiliate_id <> 1046404 and website_id in ( 26814, 44580)
update cccom_dw.fact_clicks
   set site_classification = c.site_classification
from (
select lower(stg.tracking_id) as click_id,
       a.affiliate_key,
       ctl.site_classification
  from cccom_dw.stg_click_trans stg
  join cccom_dw.dim_affiliates a on a.affiliate_id = stg.affiliate_id
       and a.affiliate_type = 'PARTNER' and a.affiliate_id  not in ( '1046404' )
  join cccom_dw.dim_websites w on w.website_id = stg.website_id
       and w.website_id in ( '26814','44580' )
  join cccom_dw.ctl_site_classification ctl on ctl.website_id = w.website_id 
       and ctl.affiliate_id = 'ALL' and ctl.affiliate_type = 'ALL'
       and ctl.is_active = 'Y'        
     ) c
where fact_clicks.click_id = c.click_id
  and fact_clicks.affiliate_key = c.affiliate_key
  and nullif(fact_clicks.site_classification,'') is null;

-- affiliate_type = Traffic_source and website_id in ( 40060, 5696)
update cccom_dw.fact_clicks
   set site_classification = c.site_classification
from (
select lower(stg.tracking_id) as click_id,
       a.affiliate_key,
       ctl.site_classification
  from cccom_dw.stg_click_trans stg
  join cccom_dw.dim_affiliates a on a.affiliate_id = stg.affiliate_id
       and a.affiliate_type = 'TRAFFIC_SOURCE' or a.affiliate_id = '104000'
  join cccom_dw.dim_websites w on w.website_id = stg.website_id
       and w.website_id in ( '40060','5696' )
  join cccom_dw.ctl_site_classification ctl on ctl.website_id = w.website_id 
       and ((ctl.affiliate_id = 'ALL' and ctl.affiliate_type = 'TRAFFIC_SOURCE')
                or (ctl.affiliate_id = '104000' and ctl.affiliate_type = 'PARTNER'))
       and ctl.is_active = 'Y'        
     ) c
where fact_clicks.click_id = c.click_id
  and fact_clicks.affiliate_key = c.affiliate_key
  and nullif(fact_clicks.site_classification,'') is null;

-- affiliate_id in ( 127714 ) and website_id in ( 40060)
update cccom_dw.fact_clicks
   set site_classification = c.site_classification
from (
select lower(stg.tracking_id) as click_id,
       a.affiliate_key,
       ctl.site_classification
  from cccom_dw.stg_click_trans stg
  join cccom_dw.dim_affiliates a on a.affiliate_id = stg.affiliate_id
       and a.affiliate_type = 'PARTNER' and a.affiliate_id in ( '127714' )
  join cccom_dw.dim_websites w on w.website_id = stg.website_id
       and w.website_id in ( '40060' )
  join cccom_dw.ctl_site_classification ctl on ctl.website_id = w.website_id 
       and ctl.affiliate_id = a.affiliate_id and ctl.affiliate_type = 'ALL'
       and ctl.is_active = 'Y'        
     ) c
where fact_clicks.click_id = c.click_id
  and fact_clicks.affiliate_key = c.affiliate_key
  and nullif(fact_clicks.site_classification,'') is null;

-- affiliate_id in ( 127714 ) and website_id in ( 5696)
update cccom_dw.fact_clicks
   set site_classification = c.site_classification
from (
select lower(stg.tracking_id) as click_id,
       a.affiliate_key,
       ctl.site_classification
  from cccom_dw.stg_click_trans stg
  join cccom_dw.dim_affiliates a on a.affiliate_id = stg.affiliate_id
       and a.affiliate_type = 'PARTNER' and a.affiliate_id in ( '127714' )
  join cccom_dw.dim_websites w on w.website_id = stg.website_id
       and w.website_id in ( '5696' )
  join cccom_dw.ctl_site_classification ctl on ctl.website_id = w.website_id 
       and ctl.affiliate_id = a.affiliate_id and ctl.affiliate_type = 'ALL'
       and ctl.is_active = 'Y'        
     ) c
where fact_clicks.click_id = c.click_id
  and fact_clicks.affiliate_key = c.affiliate_key
  and nullif(fact_clicks.site_classification,'') is null;

-- affiliate_id <> 1046404 and website_id in ( 40060, 5696)
update cccom_dw.fact_clicks
   set site_classification = c.site_classification
from (
select lower(stg.tracking_id) as click_id,
       a.affiliate_key,
       ctl.site_classification
  from cccom_dw.stg_click_trans stg
  join cccom_dw.dim_affiliates a on a.affiliate_id = stg.affiliate_id
       and a.affiliate_id  not in ( '1046404' )
  join cccom_dw.dim_websites w on w.website_id = stg.website_id
       and w.website_id in ( '40060','5696')
  join cccom_dw.ctl_site_classification ctl on ctl.website_id = w.website_id 
       and ctl.affiliate_id = 'ALL' and ctl.affiliate_type  = 'PARTNER'
       and ctl.is_active = 'Y' 
     ) c
where fact_clicks.click_id = c.click_id
  and fact_clicks.affiliate_key = c.affiliate_key
  and nullif(fact_clicks.site_classification,'') is null;

-- affiliate_id = others 
update cccom_dw.fact_clicks
set site_classification = c.site_classification
from (
  select lower(stg.tracking_id) as click_id,
         ctl.site_classification,
         ctl.rule_id,
         min(ctl.rule_id) over ( partition by w.website_id, a.affiliate_id, a.affiliate_type) min_rule_id
    from cccom_dw.stg_click_trans stg
    join cccom_dw.dim_affiliates a using (affiliate_id)
    join cccom_dw.dim_websites w   using (website_id)
    join cccom_dw.ctl_site_classification ctl
      on ( (ctl.website_id = 'ALL' or ctl.website_id = w.website_id)
           and (ctl.affiliate_id = 'ALL' or ctl.affiliate_id = a.affiliate_id)
           and (ctl.affiliate_type = 'ALL' or ctl.affiliate_type = a.affiliate_type) )
  where ctl.is_active = 'Y'
     ) c
where fact_clicks.click_id = c.click_id
  and c.rule_id = c.min_rule_id
  and nullif(fact_clicks.site_classification,'') is null;

end;
