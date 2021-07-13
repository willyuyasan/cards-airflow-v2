select k.keyword_id,
k.keyword_type,
k.keyword_text_id,
kt.keyword_text,
ifnull(k.insert_time,'') as insert_time,
ifnull(k.update_time,'') as update_time,
k.deleted
from cccomus.keywords k
join cccomus.keyword_text kt on (kt.keyword_text_id = k.keyword_text_id);
