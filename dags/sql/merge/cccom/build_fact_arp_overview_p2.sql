begin transaction; 

-- delete the data older than 18 months 
-- delete from cccom_dw.fact_arp_overview where info_date < add_months(date_trunc('month', current_date),-18);  


-- delete the last 90 days data and reload
-- delete from cccom_dw.fact_arp_overview where info_date >= add_months(date_trunc('month', current_date),-3);

-- truncate table 
truncate table cccom_dw.fact_arp_overview;

-- populate the table
INSERT INTO cccom_dw.fact_arp_overview
 (
  affiliate_id,
  affiliate_name,
  website_id,
  website_url,
  info_date,
  tot_click_count,
  tot_sale_count_by_cd,
  tot_sale_count_by_pd,
  tot_sale_amt_by_cd,
  tot_sale_amt_by_pd,
  tot_adj_amt_by_cd,
  tot_adj_amt_by_pd  
 )
 (
         select  coalesce(coalesce(a.affiliate_id,d.affiliate_id), e.affiliate_id) affiliate_id,  
                  coalesce(coalesce(a.affiliate_name, d.affiliate_name), e.affiliate_name) affiliate_name,  
                  coalesce(coalesce(a.website_id, d.website_id), e.website_id) website_id,  
                  coalesce(coalesce(a.website_url, d.website_url),e.website_url) website_url, 
                  coalesce(coalesce(a.info_date ,  d.info_date ), e.info_date ) info_date ,
                  nvl(a.tot_click_count,0)   tot_click_count           ,
                  nvl(d.tot_sale_count_by_cd,0)   tot_sale_count_by_cd      ,
                  nvl(e.tot_sale_count_by_pd,0)   tot_sale_count_by_pd      ,
                  nvl(d.tot_sale_amt_by_cd,0)   tot_sale_amt_by_cd        ,
                  nvl(e.tot_sale_amt_by_pd, 0)   tot_sale_amt_by_pd        ,
                  nvl(d.tot_adj_amt_by_cd , 0)   tot_adj_amt_by_cd        ,
                  nvl(e.tot_adj_amt_by_pd  , 0) tot_adj_amt_by_pd
  from cccom_dw.ztemp1_ovrvw_total_click_count a 
   full outer join cccom_dw.ztemp1_ovrvw_total_sale_multi_detail_cd d on d.affiliate_id = a.affiliate_id 
                                                and d.website_id = a.website_id
                                                and d.info_date = a.info_date
   full outer join cccom_dw.ztemp1_ovrvw_total_sale_multi_detail_pd e on e.affiliate_id = a.affiliate_id 
                                                and e.website_id = a.website_id
                                                and e.info_date = a.info_date
 );

end transaction;

