begin transaction;

-- Update network and target ID on click record
update cccom_dw.fact_clicks
set network = v.network,
target_id = v.target_id
from (
  select distinct m.trans_id,
  s.network,
  s.target_id
  from cccom_dw.stg_amo_visits s
  join cccom_dw.map_trans_apply m on (m.apply_id = s.transaction_id)
  where (s.network != '' or s.target_id != '')
  -- valid visits only
  and (s.referrer_type != 'no javascript' or s.operating_system != 'not specified')
) v
where click_id = v.trans_id;

-- Update last_touch_channel and last_touch_channel_detail from AMO visits
-- and use both fields to derive affiliate_classification
update cccom_dw.fact_clicks
set last_touch_channel = v.last_touch_channel,
last_touch_channel_detail = v.last_touch_channel_detail,
affiliate_classification = 
case 
  when v.last_touch_channel = 'Direct' then 'Direct Type'
  when v.last_touch_channel = 'Referred' then 'Referred'
  when v.last_touch_channel = 'Session Refresh' then 'Session Refresh'
  when v.last_touch_channel = 'Affiliate' then 'Affiliate'
  when v.last_touch_channel = 'SEO' and v.last_touch_channel_detail like '%Google%' then 'SEO-Google'
  when v.last_touch_channel = 'SEO' and (v.last_touch_channel_detail like '%Bing%' or v.last_touch_channel_detail like '%MSN%') then 'SEO-MSN'
  when v.last_touch_channel = 'SEO' and v.last_touch_channel_detail like '%Yahoo%' then 'SEO-Yahoo'
  when v.last_touch_channel = 'SEO' and v.last_touch_channel_detail like '%AOL%' then 'SEO-AOL'
  when v.last_touch_channel = 'SEO' and v.last_touch_channel_detail like '%Ask%' then 'SEO-Ask'
  when v.last_touch_channel = 'SEO' then 'SEO-Other'
  when v.last_touch_channel = 'SEM - Search Engines' and v.last_touch_channel_detail like '%Google%' then 'SEM-Google'
  when v.last_touch_channel = 'SEM - Search Engines' and (v.last_touch_channel_detail like '%Bing%' or v.last_touch_channel_detail like '%MSN%') then 'SEM-MSN'
  when v.last_touch_channel = 'SEM - Search Engines' and v.last_touch_channel_detail like '%Yahoo%' then 'SEM-Yahoo'
  when v.last_touch_channel = 'SEM - Search Engines' and v.last_touch_channel_detail like '%AOL%' then 'SEM-AOL'
  when v.last_touch_channel = 'SEM - Search Engines' then 'SEM-Other'
  when v.last_touch_channel = 'SEM - Content Network' and v.last_touch_channel_detail = '1004' then 'SEM-Google Network'
  when v.last_touch_channel = 'SEM - Content Network' and v.last_touch_channel_detail = '1003' then 'SEM-MSN Network'
  when v.last_touch_channel = 'SEM - Content Network' then 'SEM-Other Content Network'
  when v.last_touch_channel = 'Display' and v.last_touch_channel_detail = 'a36cc55b' then 'Display-Criteo'
  when v.last_touch_channel = 'Display' and v.last_touch_channel_detail = 'b0e5a181' then 'Display-Yahoo Network'
  when v.last_touch_channel = 'Display' and v.last_touch_channel_detail = '4e5c9998' then 'Display-Yahoo'
  when v.last_touch_channel = 'Display' and v.last_touch_channel_detail = '068cb91d' then 'Display-Google Content'
  when v.last_touch_channel = 'Display' and v.last_touch_channel_detail = 'b09eab9b' then 'Display-Google Banners'
  when v.last_touch_channel = 'Display' and v.last_touch_channel_detail = '1c95f83f' then 'Display-Google YouTube'
  when v.last_touch_channel = 'Display' and v.last_touch_channel_detail = 'ccddf5db' then 'Display-Yieldmo'
  when v.last_touch_channel = 'Display' and v.last_touch_channel_detail = '70937349' then 'Display-AMO'
  when v.last_touch_channel = 'Display' and v.last_touch_channel_detail = 'fd12adda' then 'Display-Rocket Fuel'
  when v.last_touch_channel = 'Display' and v.last_touch_channel_detail = '04767b3b' then 'Display-AOL'
  when v.last_touch_channel = 'Display' and v.last_touch_channel_detail = '3a40c01e' then 'Display-Facebook' 
  when v.last_touch_channel = 'Display' and v.last_touch_channel_detail = '9ec67c3f' then 'Display-Facebook Dark Posts' 
  when v.last_touch_channel = 'Display' then 'Display-Other'
  when v.last_touch_channel = 'Social Networks' and v.last_touch_channel_detail like '%facebook%' then 'Social-Facebook' 
  when v.last_touch_channel = 'Social Networks' and v.last_touch_channel_detail like '%reddit%' then 'Social-Reddit' 
  when v.last_touch_channel = 'Social Networks' then 'Social-Other'   
  when v.last_touch_channel = 'Content Marketing' and v.last_touch_channel_detail = '127714' then 'CCCom-NextAdvisor' 
  when v.last_touch_channel = 'O & O' and v.last_touch_channel_detail in ('e9b5845b','1046404') then 'O&O-TPG' 
  when v.last_touch_channel = 'O & O' and v.last_touch_channel_detail = '128504' then 'O&O-CCF' 
  when v.last_touch_channel = 'O & O' and v.last_touch_channel_detail = '8bc56e28' then 'O&O-MMS' 
end
from (
  select distinct transaction_id,
  trim(last_touch_channel) as last_touch_channel,
  trim(last_touch_channel_detail) as last_touch_channel_detail
  from cccom_dw.stg_amo_visits
  where transaction_id != ''
  and last_touch_channel != ''
) v
where apply_id = v.transaction_id;

-- Update affiliate_classification for clicks that don't have last_touch_channel set
update cccom_dw.fact_clicks
set affiliate_classification = nvl(ac.affiliate_classification,'Affiliate')
from cccom_dw.dim_affiliates a
left join cccom_dw.map_affiliate_classification ac on (ac.affiliate_id = a.affiliate_id)
where nullif(fact_clicks.last_touch_channel,'') is null
and nullif(fact_clicks.affiliate_classification,'') is null
and fact_clicks.affiliate_key = a.affiliate_key;

end transaction;
