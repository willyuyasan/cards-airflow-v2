select click_id,
ifnull(outside_id,'') as outside_id,
tracking_id,
date_inserted,
ifnull(product_id,'') as product_id,
ifnull(affiliate_id,'') as affiliate_id,
ifnull(website_id,'') as website_id,
ifnull(device_type_id,'') as device_type_id,
ifnull(traffic_source_id,'') as traffic_source_id,
ifnull(ip_address,'') as ip_address,
ifnull(exit_page_id,'') as exit_page_id,
ifnull(commission_rate_id,'') as commission_rate_id,
ifnull(cardmatch_offer_id,'') as cardmatch_offer_id,
ifnull(replace(user_variable,char(0),''),'') as user_variable,
ifnull(campaign_id,'') as campaign_id,
ifnull(page_position,'') as page_position,
ifnull(external_visit_id,'') as external_visit_id,
ifnull(keyword_id, '') as keyword_id,
ifnull(replace(replace(replace(referer_url,char(0),''),char(9),' '),'\n',''),'') as referrer_url
from cccomus.transactions_click
where date_inserted >= (last_day(now()) + interval 1 day - interval 2 month)
and date_inserted < now();
