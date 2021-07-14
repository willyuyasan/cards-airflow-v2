truncate table cccom_dw.fact_arp_sale_commissions;

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
  adjustment_type,
  adjustment_sub_type,
  adjustment_status,
  effective_start_date,
  effective_end_date,
  adjustment_amount,
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
       null    adjustment_type  ,     
       null    adjustment_sub_type,    
       null    adjustment_status,      
       null    effective_start_date,   
       null    effective_end_date,  
       nvl(a.adjustment_amt, 0)        ,
       b.payout_id              ,
       null affected_transactions,
       null comments
  from cccom_dw.fact_arp_transaction_sales a 
       left outer join cccom_dw.map_trans_payout      b  on b.trans_id = a.sale_id 
  where a.process_date >= add_months(date_trunc('month', current_date),-18)
)  ;



