unload ('select  affiliate_id, 	affiliate_name, 	website_id, 	website_url, 	info_date, 	tot_click_count,	tot_sale_count_by_cd,	tot_sale_count_by_pd,	tot_sale_amt_by_cd,	tot_sale_amt_by_pd,	tot_adj_amt_by_cd,	tot_adj_amt_by_pd,	load_date
from cccom_dw.fact_arp_overview')
to {{params["s3path"]}}
credentials 'aws_access_key_id={{params["s3AK"]}};aws_secret_access_key={{params["s3Sct"]}}'
DELIMITER '\t'
MAXFILESIZE 50 MB
GZIP
ADDQUOTES
region 'us-west-2';
