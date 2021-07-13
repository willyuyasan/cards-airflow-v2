begin transaction;

update cccom_dw.dim_affiliates
set affiliate_type = s.affiliate_type,
account_id = s.account_id,
account_type = s.account_type,
in_house = s.in_house,
first_name = s.first_name,
last_name = s.last_name,
company_name = s.company_name,
city = s.city,
state = s.state,
street = s.street,
address2 = s.address2,
country = s.country,
zip = s.zip,
status = s.status,
time_inserted = s.time_inserted,
time_modified = s.time_modified,
deleted = s.deleted,
load_date = sysdate,
ref_id = s.ref_id
from cccom_dw.stg_affiliates s
where s.affiliate_id = cccom_dw.dim_affiliates.affiliate_id
and s.time_modified > cccom_dw.dim_affiliates.time_modified;

insert into cccom_dw.dim_affiliates (
affiliate_key,
source_key,
affiliate_id, 
affiliate_type,
account_id,
account_type,
in_house,
first_name,
last_name,
company_name,
city,
state,
street,
address2,
country,
zip,
status,
time_inserted,
time_modified,
deleted,
ref_id
)
select d.last_key + row_number() over (order by s.time_inserted, s.affiliate_id),
1,
s.affiliate_id, 
s.affiliate_type,
s.account_id,
s.account_type,
s.in_house,
s.first_name,
s.last_name,
s.company_name,
s.city,
s.state,
s.street,
s.address2,
s.country,
s.zip,
s.status,
s.time_inserted,
s.time_modified,
s.deleted,
s.ref_id
from (select nvl(max(affiliate_key),100) last_key from cccom_dw.dim_affiliates) d
cross join cccom_dw.stg_affiliates s
where s.affiliate_id not in (select d.affiliate_id from cccom_dw.dim_affiliates d);

end transaction;
