select log_id,
user_id,
user_name,
affiliate_id,
affiliate_name,
card_id,
replace(card_name,'\n',' ') as card_name,
issuer_id,
issuer_name,
ifnull(website_id,'') as website_id,
ifnull(old_rate,'') as old_rate,
ifnull(nullif(old_start_date,'0000-00-00'),'') as old_start_date,
ifnull(nullif(old_end_date,'0000-00-00'),'') as old_end_date,
ifnull(new_rate,'') as new_rate,
ifnull(nullif(new_start_date,'0000-00-00'),'') as new_start_date,
ifnull(nullif(new_end_date,'0000-00-00'),'') as new_end_date,
ifnull(payin_rate,'') as payin_rate,
rate_type,
ifnull(rate_id,'') as rate_id,
action,
date_created
from cccomus.partner_commission_rates_log;
