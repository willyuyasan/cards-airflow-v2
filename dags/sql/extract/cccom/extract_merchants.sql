select merchantid, replace((replace(merchantname, ', ', ',')),' - ','-' ) merchantname, 
merchantcardpage,
ifnull(default_payin_tier_id,'') as default_payin_tier_id, 
deleted
from cccomus.cms_merchants;
