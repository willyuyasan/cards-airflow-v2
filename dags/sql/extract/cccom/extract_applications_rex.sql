select application_id,
ifnull(transaction_id,'') as transaction_id,
ifnull(upload_file_id,'') as upload_file_id,
ifnull(submission_date,'') as submission_date,
ifnull(state,'') as state,
last_updated
from cccomus.applications
where created_date >= (last_day(now()) + interval 1 day - interval 12 month);