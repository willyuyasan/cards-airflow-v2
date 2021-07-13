begin transaction;

update cccom_dw.dim_keywords 
set keyword_type = s.keyword_type,
keyword_text_id = s.keyword_text_id,
keyword_text = s.keyword_text,
insert_time = s.insert_time,
update_time = s.update_time,
deleted = s.deleted,
load_date = sysdate
from cccom_dw.stg_keywords s
where s.keyword_id = dim_keywords.keyword_id;

insert into cccom_dw.dim_keywords(
keyword_key,
source_key,
keyword_id,
keyword_type,
keyword_text_id,
keyword_text,
insert_time,
update_time,
deleted
)
select d.last_key + row_number() over (order by s.keyword_id),
1,
s.keyword_id,
s.keyword_type,
s.keyword_text_id,
s.keyword_text,
s.insert_time,
s.update_time,
s.deleted
from (select nvl(max(keyword_key),100) last_key from cccom_dw.dim_keywords) d
cross join cccom_dw.stg_keywords s
where s.keyword_id not in (select d.keyword_id from cccom_dw.dim_keywords d);

end transaction;
