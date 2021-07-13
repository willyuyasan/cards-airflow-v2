unload ('select click_id,click_date,num_click_date,date_year,date_mon,date_week,date_dayofyear,affiliate_id,affiliate_name,website_id,website_url,card_id,card_title,product_type_id,product_type_name,merchant_id,merchant_name,page_id,page_name,user_variable,visitor_ip_address,http_referrer,user_agent,banner_program,load_date
from cccom_dw.fact_arp_transaction_clicks')
to {{params["s3path"]}}
credentials 'aws_access_key_id={{params["s3AK"]}};aws_secret_access_key={{params["s3Sct"]}}'
DELIMITER '\t'
MAXFILESIZE 50 MB
ADDQUOTES
GZIP
region 'us-west-2';
