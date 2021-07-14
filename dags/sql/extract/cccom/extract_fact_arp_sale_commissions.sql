unload ('select sale_id,click_id,click_date,process_date,affiliate_id,affiliate_name,website_id,website_url,card_id,card_title,merchant_id,merchant_name,user_variable,page_id,page_name,product_type_id,product_type_name,payout_status,payout_date,commission_amt,adjustment_amt,cross_sale_ind,banner_program,adjustment_id,effective_start_date,effective_end_date,payment_id,affected_transactions,comments,load_date
from cccom_dw.fact_arp_sale_commissions')
to {{params["s3path"]}}
credentials 'aws_access_key_id={{params["s3AK"]}};aws_secret_access_key={{params["s3Sct"]}}'
DELIMITER '\t'
MAXFILESIZE 50 MB
GZIP
region 'us-west-2';
