unload ('select affiliate_id,affiliate_name,payout_id,payment_create_dt,payment_sent_dt,check_wire_info,payment_type,payment_type_desc,amount,memo,load_date,status
from cccom_dw.fact_arp_payment')
to {{params["s3path"]}}
credentials 'aws_access_key_id={{params["s3AK"]}};aws_secret_access_key={{params["s3Sct"]}}'
DELIMITER '\t'
MAXFILESIZE 50 MB
GZIP
region 'us-west-2';

