begin transaction; 

drop table if exists cccom_dw.ztemp1_perm_performance_tab1;

CREATE TABLE cccom_dw.ztemp1_perm_performance_tab1
diststyle even
compound sortkey(affiliate_id,website_id,card_id,card_type_id,merchant_id,category_id,info_date,user_variable) as
(
select coalesce(coalesce(a.affiliate_id,b.affiliate_id),c.affiliate_id) affiliate_id,
coalesce(coalesce(a.affiliate_name,b.affiliate_name),c.affiliate_name) affiliate_name,
coalesce(coalesce(a.website_id,b.website_id),c.website_id) website_id,
coalesce(coalesce(a.website_url,b.website_url),c.website_url) website_url,
coalesce(coalesce(a.card_id,b.card_id),c.card_id) card_id,
coalesce(coalesce(a.card_title,b.card_title),c.card_title) card_title,
coalesce(coalesce(a.card_type_id,b.card_type_id),c.card_type_id) card_type_id,
coalesce(coalesce(a.card_type_name,b.card_type_name),c.card_type_name) card_type_name,
coalesce(coalesce(a.merchant_id,b.merchant_id),c.merchant_id) merchant_id,
coalesce(coalesce(a.merchant_name,b.merchant_name),c.merchant_name) merchant_name,
coalesce(coalesce(a.category_id,b.category_id),c.category_id) category_id,
coalesce(coalesce(a.category_name,b.category_name),c.category_name) category_name,
coalesce(coalesce(a.user_variable,b.user_variable),c.user_variable) user_variable,
coalesce(coalesce(a.info_date,b.info_date),c.info_date) info_date,
nvl(a.tot_click_count,0) tot_click_count,
nvl(b.tot_application_count_cd,0) tot_application_count_cd,
nvl(c.tot_application_count_pd,0) tot_application_count_pd
from cccom_dw.ztemp1_perf_total_click_count a
 full outer join cccom_dw.ztemp1_perf_total_application_count_cd b on  b.affiliate_id  = a.affiliate_id
  and  b.website_id = a.website_id
  and  b.card_id = a.card_id
  and  b.card_type_id  = a.card_type_id
  and  b.merchant_id = a.merchant_id
  and  b.category_id = a.category_id
  and  b.user_variable = a.user_variable
  and  b.info_date = a.info_date
 full outer join cccom_dw.ztemp1_perf_total_application_count_pd c on  c.affiliate_id  = a.affiliate_id
  and  c.website_id = a.website_id
  and  c.card_id = a.card_id
  and  c.card_type_id  = a.card_type_id
  and  c.merchant_id = a.merchant_id
  and  c.category_id = a.category_id
  and  c.user_variable = a.user_variable
  and  c.info_date = a.info_date
);

commit;
end transaction;


begin transaction;

drop table if exists cccom_dw.ztemp1_perm_performance_tab2;

create table cccom_dw.ztemp1_perm_performance_tab2
diststyle even
compound sortkey(affiliate_id,website_id,card_id,card_type_id,merchant_id,category_id,info_date,user_variable) as
(
select coalesce(coalesce(a.affiliate_id,d.affiliate_id), e.affiliate_id) affiliate_id,
coalesce(coalesce(a.affiliate_name, d.affiliate_name), e.affiliate_name) affiliate_name,
coalesce(coalesce(a.website_id, d.website_id), e.website_id) website_id,
coalesce(coalesce(a.website_url, d.website_url),e.website_url) website_url,
coalesce(coalesce(a.card_id, d.card_id), e.card_id) card_id,
coalesce(coalesce(a.card_title, d.card_title), e.card_title) card_title,
coalesce(coalesce(a.card_type_id, d.card_type_id), e.card_type_id) card_type_id,
coalesce(coalesce(a.card_type_name, d.card_type_name), e.card_type_name) card_type_name,
coalesce(coalesce(a.merchant_id,d.merchant_id), e.merchant_id) merchant_id,
coalesce(coalesce(a.merchant_name, d.merchant_name), e.merchant_name) merchant_name,
coalesce(coalesce(a.category_id, d.category_id), e.category_id) category_id,
coalesce(coalesce(a.category_name, d.category_name), e.category_name) category_name,
coalesce(coalesce(a.user_variable, d.user_variable), e.user_variable) user_variable,
coalesce(coalesce(a.info_date, d.info_date), e.info_date) info_date,
nvl(d.tot_sale_count_by_cd,0) tot_sale_count_by_cd,
nvl(e.tot_sale_count_by_pd,0) tot_sale_count_by_pd,
nvl(d.tot_sale_amt_by_cd,0) tot_sale_amt_by_cd,
nvl(e.tot_sale_amt_by_pd,0) tot_sale_amt_by_pd,
nvl(d.tot_adj_amt_by_cd,0) tot_adj_amt_by_cd,
nvl(e.tot_adj_amt_by_pd,0) tot_adj_amt_by_pd
from cccom_dw.ztemp1_perf_total_click_count a
 full outer join cccom_dw.ztemp1_perf_total_sale_multi_detail_cd d on  d.affiliate_id  = a.affiliate_id
  and  d.website_id = a.website_id
  and  d.card_id = a.card_id
  and  d.card_type_id = a.card_type_id
  and  d.merchant_id = a.merchant_id
  and  d.category_id = a.category_id
  and  d.user_variable = a.user_variable
  and  d.info_date = a.info_date
 full outer join cccom_dw.ztemp1_perf_total_sale_multi_detail_pd e on  e.affiliate_id  = a.affiliate_id
  and  e.website_id = a.website_id
  and  e.card_id = a.card_id
  and  e.card_type_id  = a.card_type_id
  and  e.merchant_id = a.merchant_id
  and  e.category_id = a.category_id
  and  e.user_variable = a.user_variable
  and  e.info_date = a.info_date
);

commit;
end transaction;
