select pcwm.card_website_map_id,
pcwm.website_id,
pcwm.card_id,
pcwm.status,
ifnull(pcwm.date_requested,'') as date_requested,
case pcwm.time_approved
  when '0000-00-00 00:00:00' then '' 
  else ifnull(pcwm.time_approved,'')
end as time_approved,
case pcwm.last_modified_time
  when '0000-00-00 00:00:00' then '' else pcwm.last_modified_time
end as last_modified_time
from cccomus.partner_card_website_map pcwm
where pcwm.last_modified_time >= last_day(now()) + interval 1 day - interval 1 month;
