begin transaction;

update cccom_dw.dim_websites
set 
-- TODO: affiliate_key 
name = s.name,
url = s.url,
status = s.status,
approval_status = s.approval_status,
deleted = s.deleted,
load_date = sysdate
from cccom_dw.stg_websites s
where s.website_id = dim_websites.website_id;

insert into cccom_dw.dim_websites (
website_key,
source_key,
website_id,
affiliate_key,
name,
url,
status,
approval_status,
deleted
)
select last_key + row_number() over (order by stg.website_id),
1,
stg.website_id,
nvl(aff.affiliate_key, -1),
stg.name,
stg.url,
stg.status,
stg.approval_status,
stg.deleted
from cccom_dw.stg_websites stg
cross join (select nvl(max(website_key),100) as last_key from cccom_dw.dim_websites)
left join cccom_dw.dim_affiliates aff on (aff.affiliate_id = stg.affiliate_id)
where stg.website_id not in (select dim.website_id from cccom_dw.dim_websites dim);

end transaction;
