unload ('select adjustment_id,adjustment_type_id,adjustment_type,adjustment_sub_type,adjustment_status,issuer_id,issuer_name,affiliate_id,affiliate_name,product_id,effective_start_date,effective_end_date,adjustment_amount,payment_id,affected_transactions,website_id,comments,load_date
from cccom_dw.fact_arp_adjustments')
to {{params["s3path"]}}
credentials 'aws_access_key_id={{params["s3AK"]}};aws_secret_access_key={{params["s3Sct"]}}'
DELIMITER '\t'
MAXFILESIZE 50 MB
GZIP
region 'us-west-2';
