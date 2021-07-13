begin transaction; 

drop table if exists cccom_dw.ztemp1_ovrvw_total_click_count;

create table cccom_dw.ztemp1_ovrvw_total_click_count as
 select a.affiliate_id affiliate_id, 
  a.affiliate_name affiliate_name,
  a.website_id website_id,
  a.website_url website_url,
  trunc(a.click_date) info_date,
  count(*) tot_click_count             
 from cccom_dw.fact_arp_transaction_clicks a
 where a.click_date >= add_months(date_trunc('month', current_date),-18)                                                      
 group by a.affiliate_id,
  a.affiliate_name,
  a.website_id,
  a.website_url,
  trunc(a.click_date);

drop table if exists cccom_dw.ztemp1_ovrvw_total_sale_multi_detail_cd;

create table cccom_dw.ztemp1_ovrvw_total_sale_multi_detail_cd as
 select a.affiliate_id affiliate_id, 
  a.affiliate_name affiliate_name,
  a.website_id website_id,
  a.website_url website_url,
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
 trunc(a.click_date);
                  
-- query to get   tot_sale_count_by_pd, tot_sale_amt_by_pd , tot_adj_amt_by_pd 
drop table if exists cccom_dw.ztemp1_ovrvw_total_sale_multi_detail_pd;

create table cccom_dw.ztemp1_ovrvw_total_sale_multi_detail_pd as
 select a.affiliate_id affiliate_id, 
  a.affiliate_name affiliate_name,
  a.website_id website_id,
  a.website_url website_url,
  trunc(a.process_date) info_date,
  count(*) tot_sale_count_by_pd,
  sum(a.commission_amt) tot_sale_amt_by_pd,
  sum(a.adjustment_amt) tot_adj_amt_by_pd     
from cccom_dw.fact_arp_transaction_sales a
where a.click_date  >= add_months(date_trunc('month', current_date),-18)                                                     
group by a.affiliate_id,
 a.affiliate_name,
 a.website_id,
 a.website_url,
 trunc(a.process_date);
                  
end transaction;

