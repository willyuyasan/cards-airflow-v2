drop table if exists cccom_dw.ztemp1_perf_total_click_count; 

create table cccom_dw.ztemp1_perf_total_click_count as
select a.affiliate_id affiliate_id,
a.affiliate_name affiliate_name,
a.website_id website_id,
a.website_url website_url,
a.card_id card_id,
a.card_title card_title,
a.product_type_id card_type_id,
a.product_type_name card_type_name,
a.merchant_id merchant_id,
a.merchant_name merchant_name,
a.page_id category_id,
a.page_name category_name,
a.user_variable user_variable,
trunc(a.click_date) info_date,
count(*) tot_click_count
from cccom_dw.fact_arp_transaction_clicks a
where a.click_date >= add_months(date_trunc('month', current_date),-18) 
group by a.affiliate_id,
a.affiliate_name,
a.website_id,
a.website_url,
a.card_id,
a.card_title,
a.product_type_id,
a.product_type_name,
a.merchant_id,
a.merchant_name,
a.page_id,
a.page_name,
a.user_variable,
trunc(a.click_date);
                  
  -- query to get   tot_application_count_cd
drop table if exists cccom_dw.ztemp1_perf_total_application_count_cd;

create table cccom_dw.ztemp1_perf_total_application_count_cd as
select a.affiliate_id affiliate_id, 
a.affiliate_name affiliate_name,
a.website_id website_id,
a.website_url website_url,
a.card_id card_id,
a.card_title card_title,
a.product_type_id card_type_id,
a.product_type_name card_type_name,
a.merchant_id merchant_id,
a.merchant_name merchant_name,
a.page_id category_id,
a.page_name category_name,
a.user_variable user_variable,
trunc(a.click_date) info_date,
count(*) tot_application_count_cd      
from cccom_dw.fact_arp_transaction_applications a
where a.click_date  >= add_months(date_trunc('month', current_date),-18)                                                       
group by a.affiliate_id,
a.affiliate_name,
a.website_id,
a.website_url,
a.card_id,
a.card_title,
a.product_type_id,
a.product_type_name,
a.merchant_id,
a.merchant_name,
a.page_id,
a.page_name,
a.user_variable,
trunc(a.click_date);
                 
-- query to get   tot_application_count_pd
drop table if exists cccom_dw.ztemp1_perf_total_application_count_pd;
  
create table cccom_dw.ztemp1_perf_total_application_count_pd as 
select a.affiliate_id affiliate_id, 
a.affiliate_name affiliate_name,
a.website_id website_id,
a.website_url website_url,
a.card_id card_id,
a.card_title card_title,
a.product_type_id card_type_id,
a.product_type_name card_type_name,
a.merchant_id merchant_id,
a.merchant_name merchant_name,
a.page_id category_id,
a.page_name category_name,
a.user_variable user_variable,
trunc(a.process_date) info_date,
count(*) tot_application_count_pd      
from cccom_dw.fact_arp_transaction_applications a
where a.click_date  >= add_months(date_trunc('month', current_date),-18)                                                     
group by a.affiliate_id,
a.affiliate_name,
a.website_id,
a.website_url,
a.card_id,
a.card_title,
a.product_type_id,
a.product_type_name,
a.merchant_id,
a.merchant_name,
a.page_id,
a.page_name,
a.user_variable,
trunc(a.process_date);
                  
-- query to get   tot_sale_count_by_cd, tot_sale_amt_by_cd , tot_adj_amt_by_cd
drop table if exists cccom_dw.ztemp1_perf_total_sale_multi_detail_cd;
 
create table cccom_dw.ztemp1_perf_total_sale_multi_detail_cd as
select a.affiliate_id affiliate_id, 
a.affiliate_name affiliate_name,
a.website_id website_id,
a.website_url website_url,
a.card_id card_id,
a.card_title card_title,
a.product_type_id card_type_id,
a.product_type_name card_type_name,
a.merchant_id merchant_id,
a.merchant_name merchant_name,
a.page_id category_id,
a.page_name category_name,
a.user_variable user_variable,
trunc(a.click_date) info_date,
count(*) tot_sale_count_by_cd,
sum(a.commission_amt) tot_sale_amt_by_cd,
sum(a.adjustment_amt) tot_adj_amt_by_cd     
from cccom_dw.fact_arp_transaction_sales a
where a.click_date  >= add_months(date_trunc('month', current_date),-18)                                                       
group by a.affiliate_id,
a.affiliate_name,
a.website_id,
a.website_url,
a.card_id,
a.card_title,
a.product_type_id,
a.product_type_name,
a.merchant_id,
a.merchant_name,
a.page_id,
a.page_name,
a.user_variable,
trunc(a.click_date);

-- query to get   tot_sale_count_by_pd, tot_sale_amt_by_pd , tot_adj_amt_by_pd
drop table if exists cccom_dw.ztemp1_perf_total_sale_multi_detail_pd;

create table cccom_dw.ztemp1_perf_total_sale_multi_detail_pd as
select a.affiliate_id affiliate_id,
a.affiliate_name affiliate_name,
a.website_id website_id,
a.website_url website_url,
a.card_id card_id,
a.card_title card_title,
a.product_type_id card_type_id,
a.product_type_name card_type_name,
a.merchant_id merchant_id,
a.merchant_name merchant_name,
a.page_id category_id,
a.page_name category_name,
a.user_variable user_variable,
trunc(a.process_date) info_date,
count(*) tot_sale_count_by_pd,
sum(a.commission_amt) tot_sale_amt_by_pd,
sum(a.adjustment_amt) tot_adj_amt_by_pd
from cccom_dw.fact_arp_transaction_sales a
where a.process_date  >= add_months(date_trunc('month', current_date),-18)
group by a.affiliate_id,
a.affiliate_name,
a.website_id,
a.website_url,
a.card_id,
a.card_title,
a.product_type_id,
a.product_type_name,
a.merchant_id,
a.merchant_name,
a.page_id,
a.page_name,
a.user_variable,
trunc(a.process_date); 

