select declined_application_id,
transaction_id,
upload_file_id,
case provider_process_time
  when '0000-00-00 00:00:00' then '' else provider_process_time
end as provider_process_time,
state,
ifnull(error_code,'') as error_code,
case update_time
  when '0000-00-00 00:00:00' then '' else update_time
end as update_time
from declined_applications
where ((provider_process_time >= (last_day(now()) + interval 1 day - interval 3 month) and provider_process_time < now())
or (update_time >= (last_day(now()) + interval 1 day - interval 3 month) and update_time < now()));