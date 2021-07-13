select
website_id,
affiliate_id,
replace(name, char(9), ' ') as name,
replace(url, char(9), ' ') as url,
status,
#ifnull(content_type,'') as content_type,
'' as content_type,
#ifnull(traffic_source,'') as traffic_source,
'' as traffic_source,
approval_status,
deleted
from cccomus.partner_websites pw;
#where pw.website_id in (
#select tc.website_id from cccomus.transactions_click tc
#);
