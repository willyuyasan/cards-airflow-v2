unload ('select affiliate_id ,affiliate_name ,website_id ,website_url ,card_id ,card_title ,product_type_id ,product_type_name ,merchant_id ,merchant_name ,page_id ,page_name ,user_variable ,info_date ,tot_click_count ,tot_application_count_cd ,tot_application_count_pd ,tot_sale_count_by_cd ,tot_sale_count_by_pd ,tot_sale_amt_by_cd ,tot_sale_amt_by_pd ,load_date ,tot_adj_amt_by_cd ,tot_adj_amt_by_pd
from cccom_dw.fact_arp_performance')
to {{params["s3path"]}}
credentials 'aws_access_key_id={{params["s3AK"]}};aws_secret_access_key={{params["s3Sct"]}}'
DELIMITER '\t'
MAXFILESIZE 50 MB
ADDQUOTES
GZIP
region 'us-west-2';
