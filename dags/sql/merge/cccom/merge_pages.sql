begin transaction;

update cccom_dw.dim_pages 
set page_name = s.page_name,
page_type = s.page_type,
page_url = s.page_url,
insert_time = s.insert_time,
deleted = s.deleted,
load_date = sysdate
from cccom_dw.stg_pages s
where s.page_id = dim_pages.page_id;

insert into cccom_dw.dim_pages(
page_key,
source_key,
page_id,
page_name,
page_type,
page_url,
insert_time,
deleted
)
select d.last_key + row_number() over (order by s.page_id),
1,
s.page_id,
s.page_name,
s.page_type,
s.page_url,
s.insert_time,
s.deleted
from (select nvl(max(page_key),100) last_key from cccom_dw.dim_pages) d
cross join cccom_dw.stg_pages s
where s.page_id not in (select d.page_id from cccom_dw.dim_pages d);

end transaction;
