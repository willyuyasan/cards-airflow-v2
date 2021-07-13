# Do not extract accountid (empty), data1 (empty), and commissiongiven (all 0)

select impressionid,
dateimpression,
bannerid,
affiliateid,
all_imps_count,
unique_imps_count
from impressions
where dateimpression >= (last_day(now()) + interval 1 day - interval 3 month)
and dateimpression < now();
