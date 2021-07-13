begin transaction; 

-- delete 3m data 
-- delete from cccom_dw.fact_arp_sale_commissions where process_date >= add_months(date_trunc('month', current_date),-3);

-- trucnate table 
truncate table cccom_dw.fact_arp_sale_commissions;


-- REX 

INSERT INTO cccom_dw.fact_arp_sale_commissions
(
  sale_id,
  click_id,
  click_date,
  process_date,
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
  payout_status,
  payout_date,
  commission_amt,
  adjustment_amt,
  cross_sale_ind,
  banner_program,
  adjustment_id,
  effective_start_date,
  effective_end_date,
  payment_id,
  affected_transactions,
  comments
)
(
select a.sale_id                ,
       a.click_id               ,
       a.click_date             ,
       a.process_date           ,
       a.affiliate_id           ,
       a.affiliate_name         ,
       a.website_id             ,
       a.website_url            ,
       a.card_id                ,
       a.card_title             ,
       a.merchant_id            ,
       a.merchant_name          ,
       a.user_variable          ,
       a.page_id                ,
       a.page_name              ,
       a.product_type_id        ,
       a.product_type_name      ,
       a.payout_status          ,
       a.payout_date            ,
       a.commission_amt         ,
       a.adjustment_amt         ,
       a.cross_sale_ind         ,
       a.banner_program         ,
       null    adjustment_id    ,       
       null    effective_start_date,   
       null    effective_end_date,  
       b.payout_id              ,
       null affected_transactions,
       null comments
  from cccom_dw.fact_arp_transaction_sales a 
       left outer join cccom_dw.map_trans_payout      b  on b.trans_id = a.sale_id 
  where a.process_date < '01-Mar-2019'
    and a.commission_amt > 0
    and a.process_date >= add_months(date_trunc('month', current_date),-18)
)  ;




-- RMS

INSERT INTO cccom_dw.fact_arp_sale_commissions
(
  sale_id,
  click_id,
  click_date,
  process_date,
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
  payout_status,
  payout_date,
  commission_amt,
  adjustment_amt,
  cross_sale_ind,
  banner_program,
  adjustment_id,
  effective_start_date,
  effective_end_date,
  payment_id,
  affected_transactions,
  comments
)
(
select a.sale_id                ,
       a.click_id               ,
       a.click_date             ,
       a.process_date           ,
       a.affiliate_id           ,
       a.affiliate_name         ,
       a.website_id             ,
       a.website_url            ,
       a.card_id                ,
       a.card_title             ,
       a.merchant_id            ,
       a.merchant_name          ,
       a.user_variable          ,
       a.page_id                ,
       a.page_name              ,
       a.product_type_id        ,
       a.product_type_name      ,
       a.payout_status          ,
       a.payout_date            ,
       a.commission_amt         ,
       a.adjustment_amt         ,
       a.cross_sale_ind         ,
       a.banner_program         ,
       b.adjustment_id    ,       
       b.effective_start_date,   
       b.effective_end_date,  
       b.payment_id              ,
       null affected_transactions,
       null comments
  from cccom_dw.fact_arp_transaction_sales a 
       left join cccom_dw.fact_sales_commission b on b.trans_id = a.sale_id   
  where a.process_date >= '01-Mar-2019'
    and (a.commission_amt > 0 or a.commission_amt < 0 )
    and a.process_date >= add_months(date_trunc('month', current_date),-18)
)  ;


end transaction;


