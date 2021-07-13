begin transaction;

update cccom_dw.dim_banners
set affiliate_type = s.affiliate_type,
banner_type = s.banner_type,
name = s.name,
url = s.url,
deleted = s.deleted,
load_date = sysdate
from cccom_dw.stg_banners s
where s.banner_id = dim_banners.banner_id;

insert into cccom_dw.dim_banners (
  banner_key,
  banner_id,
  affiliate_type,
  banner_type,
  name,
  url,
  deleted
)
select last_key + row_number() over (order by s.banner_id),
s.banner_id,
s.affiliate_type,
s.banner_type,
s.name,
s.url,
s.deleted
from cccom_dw.stg_banners s
cross join (select nvl(max(banner_key),100) as last_key from cccom_dw.dim_banners)
where s.banner_id not in (select d.banner_id from cccom_dw.dim_banners d);

end transaction;
