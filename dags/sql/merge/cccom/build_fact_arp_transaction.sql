-- delete the data older than 18 months  
-- delete from cccom_dw.fact_arp_transaction_clicks where click_date < add_months(date_trunc('month', current_date),-18);  

-- delete the last 90 days data and reload    
-- delete from cccom_dw.fact_arp_transaction_clicks where click_date >= add_months(date_trunc('month', current_date),-18);  

-- truncate the table  
truncate table cccom_dw.fact_arp_transaction_clicks_2build;

-- populate the table with 18m data 
INSERT INTO cccom_dw.fact_arp_transaction_clicks_2build
(
click_id,
click_date,
num_click_date,
date_year,
date_mon,
date_week,
date_dayofyear,
affiliate_id,
affiliate_name,
website_id,
website_url,
card_id,
card_title,
product_type_id,
product_type_name,
merchant_id,
merchant_name,
page_id,
page_name,
user_variable,
visitor_ip_address,
http_referrer,
banner_program
)
(
select bb.click_id,
 bb.click_date ,
 bb.num_click_date,
 bb.date_year, 
 bb.date_mon, 
 bb.date_week,
 bb.date_dayofyear,
 bb.affiliate_id, 
 bb.affiliate_name,
 bb.website_id,
 bb.website_url,
 bb.card_id,
 bb.card_title,
 bb.product_type_id,
 bb.product_type_name,
 bb.merchant_id,
 bb.merchant_name,
 bb.page_id,
 bb.page_name,
 bb.user_variable,
 bb.visitor_ip_address,
 bb.http_referrer,
 bb.banner_program
 from (      
select a.click_id click_id,
a.click_date click_date, 
to_number(to_char(a.click_date,'YYYYMMDD'),'99999999') num_click_date,
date_part(year , a.click_date) date_year, 
date_part(mon , a.click_date) date_mon, 
date_part(week , a.click_date) date_week,
date_part(dayofyear, a.click_date) date_dayofyear,
b.affiliate_id affiliate_id, 
b.company_name affiliate_name,
c.website_id website_id,
c.url website_url,
d.card_id card_id,
d.card_title card_title,
e.product_type_id product_type_id,
e.product_type_name product_type_name,
f.merchant_id merchant_id,
f.merchant_name merchant_name,
g.page_id page_id,
g.page_name page_name,
a.user_variable user_variable,
floor(( a.ip_address + 2147483647 ) / 16777216 ) || '.' ||
floor(  (( a.ip_address + 2147483647 ) - ((floor(( a.ip_address + 2147483647 ) / 16777216)) * 16777216 )) / 65536 ) || '.' ||
floor( ((( a.ip_address + 2147483647 ) - ((floor(( a.ip_address + 2147483647 ) / 16777216)) * 16777216 )) - (( floor((( a.ip_address + 2147483647 ) - (( floor(( a.ip_address + 2147483647 ) / 16777216 )) * 16777216 )) / 65536 )) * 65536 )) / 256 ) || '.' ||
floor((((( a.ip_address + 2147483647 ) - ((floor(( a.ip_address + 2147483647 ) / 16777216)) * 16777216 )) - (( floor((( a.ip_address + 2147483647 ) - (( floor(( a.ip_address + 2147483647 ) / 16777216 )) * 16777216 )) / 65536 )) * 65536 )) - (( floor(((( a.ip_address + 2147483647 ) - (( floor(( a.ip_address + 2147483647 ) / 16777216 )) * 16777216 )) - (( floor((( a.ip_address + 2147483647 ) - (( floor(( a.ip_address + 2147483647 ) / 16777216 )) * 16777216 )) / 65536 )) * 65536 )) / 256 )) * 256 )) / 1 ) visitor_ip_address,
a.referrer_url http_referrer,
case 
 when c.website_id in ( 5696, 40060 ) and b.affiliate_id  <> '104000' and b.affiliate_type = 'PARTNER' then
 TRUE
 else
 FALSE 
end banner_program,
case 
when c.website_id in ( 5696, 40060 ) and b.affiliate_id  <> '104000' and b.affiliate_type = 'PARTNER' then
  csp.click_date
else
null 
end cs_clicked_date      
from cccom_dw.fact_clicks a
 join cccom_dw.dim_affiliates b on b.affiliate_key = a.affiliate_key      
 join cccom_dw.dim_websites c on c.website_key = a.website_key
 join cccom_dw.dim_products d on d.product_key = a.product_key
 join cccom_dw.dim_product_types e on e.product_type_key = d.product_type_key
 join cccom_dw.dim_merchants f on f.merchant_key = d.merchant_key
 left outer join cccom_dw.dim_pages g on g.page_key = a.exit_page_key
 left outer join cccom_dw.fact_clicks csp on csp.click_id = a.user_variable    
where a.click_date >= add_months(date_trunc('month', current_date),-18)                                                        
) bb
where bb.banner_program = 'False' or ( bb.banner_program = 'True' and bb.cs_clicked_date is not null)                                                      
);

alter table cccom_dw.fact_arp_transaction_clicks rename to fact_arp_transaction_clicks_2build_temp;

alter table cccom_dw.fact_arp_transaction_clicks_2build rename to fact_arp_transaction_clicks;

alter table cccom_dw.fact_arp_transaction_clicks_2build_temp rename to fact_arp_transaction_clicks_2build;



-- delete the data older than 18 months 
-- delete from cccom_dw.fact_arp_transaction_applications where click_date < add_months(date_trunc('month', current_date),-18);  

-- delete the last 90 days data and reload
-- delete from cccom_dw.fact_arp_transaction_applications where click_date >= add_months(date_trunc('month', current_date),-18); 

truncate table cccom_dw.fact_arp_transaction_applications;

BEGIN;

-- populate the table with 3m data 
INSERT INTO cccom_dw.fact_arp_transaction_applications
(
application_id,
click_id,
click_date,
process_date,
num_click_date,
date_year,
date_mon,
date_week,
date_dayofyear,
affiliate_id,
affiliate_name,
website_id,
website_url,
card_id,
card_title,
product_type_id,
product_type_name,
merchant_id,
merchant_name,
page_id,
page_name,
user_variable
)
(
select a.application_id application_id,
a.click_id click_id,
b.click_date click_date,
a.submission_date process_date,
to_number(to_char(b.click_date,'YYYYMMDD'),'99999999') num_click_date,
date_part(year , b.click_date) date_year, 
date_part(mon , b.click_date) date_mon, 
date_part(week , b.click_date) date_week,
date_part(dayofyear, b.click_date) date_dayofyear,
c.affiliate_id affiliate_id, 
c.company_name affiliate_name,
d.website_id website_id,
d.url website_url,
e.card_id card_id,
e.card_title card_title,
h.product_type_id product_type_id,
h.product_type_name product_type_name,
f.merchant_id merchant_id,
f.merchant_name merchant_name,
g.page_id page_id,
g.page_name page_name,
b.user_variable user_variable           
from cccom_dw.fact_applications a
 join cccom_dw.fact_clicks b on b.click_id = a.click_id
 join cccom_dw.dim_affiliates c on c.affiliate_key = b.affiliate_key      
 join cccom_dw.dim_websites d on d.website_key = b.website_key
 join cccom_dw.dim_products e on e.product_key = b.product_key
 join cccom_dw.dim_merchants f on f.merchant_key = e.merchant_key
 join cccom_dw.dim_product_types h on h.product_type_key = e.product_type_key
 left outer join cccom_dw.dim_pages g on g.page_key = b.exit_page_key
where b.click_date >= add_months(date_trunc('month', current_date),-18)
 and ((a.status = 'COMMITTED' and a.is_declined = 0)  OR (a.status='VALID' and a.is_declined = 1))
);

end;

-- delete the data older than 18 months 
-- delete from cccom_dw.fact_arp_transaction_sales where process_date < add_months(date_trunc('month', current_date),-18);  

-- delete the last 90 days data and reload
-- delete from cccom_dw.fact_arp_transaction_sales where process_date >= add_months(date_trunc('month', current_date),-18); 

truncate table cccom_dw.fact_arp_transaction_sales;

BEGIN;

-- populate tha table with 3m data
INSERT INTO cccom_dw.fact_arp_transaction_sales
(
sale_id,
click_id,
click_date,
process_date,
num_click_date,
date_year,
date_mon,
date_week,
date_dayofyear,
affiliate_id,
affiliate_name,
website_id,
website_url,
card_id,
card_title,
merchant_id,
merchant_name,
user_variable,
page_id,
page_name,
product_type_id,
product_type_name,
commission_amt,
adjustment_amt,    
banner_program,
cross_sale_ind    
)
(
select a.trans_id sale_id,
a.click_id click_id,
b.click_date click_date,
a.provider_process_date process_date,
to_number(to_char(a.provider_process_date,'YYYYMMDD'),'99999999') num_proces_date,
date_part(year , a.provider_process_date) date_year,
date_part(mon , a.provider_process_date) date_mon,
date_part(week , a.provider_process_date) date_week,
date_part(dayofyear, a.provider_process_date) date_dayofyear,
c.affiliate_id affiliate_id,
c.company_name affiliate_name,
d.website_id website_id,
d.url website_url,
e.card_id card_id,
e.card_title card_title,
f.merchant_id merchant_id,
f.merchant_name merchant_name,
b.user_variable user_variable,
g.page_id page_id,
g.page_name page_name,
h.product_type_id product_type_id,
h.product_type_name product_type_name,
a.commission commission_amount,
a.adjustment_amt,    
case
 when d.website_id in ( 5696, 40060 ) and c.affiliate_id <> '104000' and c.affiliate_type = 'PARTNER' then
    TRUE
 else
    FALSE
end banner_program,
case
  when a.cross_sale_product_key > 0 then
    TRUE
  else
    FALSE
end cross_sale_ind      
from cccom_dw.fact_sales a
 left outer join cccom_dw.fact_clicks b on b.click_id = a.click_id
 join cccom_dw.dim_affiliates c on c.affiliate_key = b.affiliate_key
 join cccom_dw.dim_websites d on d.website_key = b.website_key
 join cccom_dw.dim_products e on e.product_key = b.product_key
 join cccom_dw.dim_merchants f on f.merchant_key = e.merchant_key
 left outer join cccom_dw.dim_pages g on g.page_key = b.exit_page_key
 join cccom_dw.dim_product_types h on h.product_type_key = e.product_type_key
where a.provider_process_date >= add_months(date_trunc('month', current_date),-18)
  and ((a.trans_type IN ( 4,5,6,105,106,107 ) and a.commission > 0 ) or
       (a.trans_type in ( 130 ) and a.click_id <> 'null' and a.commission < 0))
);

end; 
