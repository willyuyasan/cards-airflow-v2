begin transaction; 


-- delete the data older than 18 months 
-- delete from cccom_dw.fact_arp_performance where info_date < add_months(date_trunc('month', current_date),-18);  


-- delete the last 90 days data and reload
-- delete from cccom_dw.fact_arp_performance where info_date >= add_months(date_trunc('month', current_date),-3) ; 

-- truncate table 
truncate table cccom_dw.fact_arp_performance;


INSERT INTO cccom_dw.fact_arp_performance
(
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
info_date,
tot_click_count,
tot_application_count_cd,
tot_application_count_pd,
tot_sale_count_by_cd,
tot_sale_count_by_pd,
tot_sale_amt_by_cd,
tot_sale_amt_by_pd,
tot_adj_amt_by_cd,
tot_adj_amt_by_pd
)
(
select  coalesce(a.affiliate_id,b.affiliate_id) affiliate_id,
coalesce(a.affiliate_name,b.affiliate_name) affiliate_name,
coalesce(a.website_id,b.website_id) website_id,
coalesce(a.website_url,b.website_url) website_url,
coalesce(a.card_id,b.card_id) card_id,
coalesce(a.card_title,b.card_title) card_title,
coalesce(a.card_type_id,b.card_type_id) card_type_id,
coalesce(a.card_type_name,b.card_type_name) card_type_name,
coalesce(a.merchant_id,b.merchant_id) merchant_id,
coalesce(a.merchant_name,b.merchant_name) merchant_name,
coalesce(a.category_id,b.category_id) category_id,
coalesce(a.category_name,b.category_name) category_name,
coalesce(a.user_variable,b.user_variable) user_variable,
coalesce(a.info_date,b.info_date) info_date,
nvl(a.tot_click_count,0) tot_click_count,
nvl(a.tot_application_count_cd,0) tot_application_count_cd,
nvl(a.tot_application_count_pd,0) tot_application_count_pd,
nvl(b.tot_sale_count_by_cd,0) tot_sale_count_by_cd,
nvl(b.tot_sale_count_by_pd,0) tot_sale_count_by_pd,
nvl(b.tot_sale_amt_by_cd,0) tot_sale_amt_by_cd,
nvl(b.tot_sale_amt_by_pd,0) tot_sale_amt_by_pd,
nvl(b.tot_adj_amt_by_cd,0) tot_adj_amt_by_cd,
nvl(b.tot_adj_amt_by_pd,0) tot_adj_amt_by_pd
from cccom_dw.ztemp1_perm_performance_tab1 a
 full outer join cccom_dw.ztemp1_perm_performance_tab2 b on  b.affiliate_id  = a.affiliate_id
  and  b.website_id = a.website_id
  and  b.card_id = a.card_id
  and  b.card_type_id  = a.card_type_id
  and  b.merchant_id = a.merchant_id
  and  b.category_id = a.category_id
  and  b.info_date = a.info_date
  and  b.user_variable = a.user_variable
);

commit;
end transaction;
