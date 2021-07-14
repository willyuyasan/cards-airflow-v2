begin transaction;

delete from cccom_dw.fact_amo_visits
where (suite_id, visit_date) in (
  select distinct s.suite_id, trunc(s.visit_datetime)
    from cccom_dw.stg_amo_visits s
);

insert into cccom_dw.fact_amo_visits (
visitor_id,
visit_num,
visit_id,
transaction_id,
visit_datetime,
visit_date,
suite_id,
country,
state,
city,
operating_system,
mobile_os,
mobile_device_name,
mobile_device_type,
device_type_key,
browser,
page,
landing_page,
original_referring_domain,
referring_domain,
referrer_type,
referrer,
search_engine_all,
exit_page_key,
card_position,
affiliate_key,
search_engine_natural,
search_engine_paid,
product_key,
tracking_code,
mktg_cloud_visitor_id,
affiliate_key2,
affiliate_id2,
banner_key,
banner_id,
landing_page_id,
keywords,
campaign_id,
sem_ad_group,
google_click_id,
last_touch_channel,
network,
target_id,
last_touch_channel_detail,
optimizely_campaign_id,
optimizely_experiment_id,
optimizely_variation_id,
optimizely_user_id,
current_url,
first_touch_channel,
first_touch_channel_detail,
score_create_date,
score_account_age,
score_account_id,
score_qls,
score_oc_filters,
score_used_filters,
score_credit_score,
google_sem_location,
email_campaign_tracking,
email_campaign_name,
email_campaign_id,
email_link_term
)
select v.visitor_id,
v.visit_num,
md5(v.visitor_id||'/'||v.visit_num) as visit_id,
v.transaction_id,
v.visit_datetime,
trunc(v.visit_datetime) as visit_date,
v.suite_id,
v.country,
v.state,
v.city,
v.operating_system,
v.mobile_os,
v.mobile_device_name,
v.mobile_device_type,
nvl(dt.device_type_key, -1) as device_type_key,
v.browser,
v.page,
v.landing_page,
v.original_referring_domain,
v.referring_domain,
v.referrer_type,
v.referrer,
v.search_engine_all,
nvl(pg.page_key, -1) as exit_page_key,
v.card_position,
nvl(aff1.affiliate_key, aff2.affiliate_key, -1) as affiliate_key,
v.search_engine_natural,
v.search_engine_paid,
nvl(pr.product_key, -1) as product_key,
v.tracking_code,
v.mktg_cloud_visitor_id,
nvl(aff3.affiliate_key, aff4.affiliate_key, -1) as affiliate_key2,
v.aid as affiliate_id2,
nvl(br.banner_key, -1) as banner_key,
v.bid as banner_id,
v.cid as landing_page_id,
v.did as keywords,
v.eid as campaign_id,
v.gid as sem_ad_group,
v.gclid as google_click_id,
v.last_touch_channel as last_touch_channel,
v.network as network,
v.target_id as target_id,
v.last_touch_channel_detail as last_touch_channel_detail,
v.campaign_id as campaign_id,
v.experiment_id as experiment_id,
v.variation_id as variation_id,
v.ou_id as user_id,
v.current_url as current_url,
v.first_touch_channel,
v.first_touch_channel_detail,
v.score_create_date,
v.score_account_age,
v.score_account_id,
v.score_qls,
v.score_oc_filters,
v.score_used_filters,
v.score_credit_score::int,
v.google_sem_location::int,
v.email_campaign_tracking,
v.email_campaign_name,
v.email_campaign_id,
v.email_link_term
from cccom_dw.stg_amo_visits v
left join cccom_dw.dim_device_types dt on (
  dt.device_type_id =
    case v.mobile_device_type
    when 'Mobile Phone' then 2
    when 'Tablet' then 3
    when 'Other' then 1
    else -1
    end
)
left join cccom_dw.dim_pages pg on (pg.page_id = v.exit_page_id)
left join cccom_dw.dim_page_positions pp on (pp.name = v.card_position::varchar)
left join cccom_dw.dim_affiliates aff1 on (nullif(aff1.ref_id,'') = v.affiliate_id)
left join cccom_dw.dim_affiliates aff2 on (aff2.affiliate_id = v.affiliate_id)
left join cccom_dw.dim_affiliates aff3 on (nullif(aff3.ref_id,'') = v.aid)
left join cccom_dw.dim_affiliates aff4 on (aff4.affiliate_id = v.aid)
left join cccom_dw.dim_products pr on (pr.card_id = v.card_id)
left join cccom_dw.dim_banners br on (br.banner_id = v.bid)
where (v.referrer_type != 'no javascript' or v.operating_system != 'not specified');

end transaction;
