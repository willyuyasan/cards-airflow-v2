unload ('select sale_id,click_id,click_date,process_date,num_click_date,date_year,date_mon,date_week,date_dayofyear,affiliate_id,affiliate_name,website_id,website_url,card_id,	card_title,merchant_id,merchant_name,user_variable,page_id,page_name,product_type_id,product_type_name,payout_status,payout_date,commission_amt,cross_sale_ind,banner_program,load_date,adjustment_amt
from cccom_dw.fact_arp_transaction_sales')
to {{params["s3path"]}}
credentials 'aws_access_key_id={{params["s3AK"]}};aws_secret_access_key={{params["s3Sct"]}}'
DELIMITER '\t'
MAXFILESIZE 50 MB
GZIP
region 'us-west-2';
