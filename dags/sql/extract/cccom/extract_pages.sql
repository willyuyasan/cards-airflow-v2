select page_id, 
replace(page_name, char(9), ' ') as page_name,
page_type, 
ifnull(page_url, '') as page_url, 
insert_time, deleted
from cccomus.pages;
