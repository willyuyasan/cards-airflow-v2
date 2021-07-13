begin transaction;

-- Delete existing slotting CPA from fact table
delete from cccom_dw.fact_cpa
where fact_cpa.bidding_cycle_id in (
  select distinct s.bidding_cycle_id from cccom_dw.stg_cpbt_cpa s
);

-- Delete slotting CPA for bidding cycle ID that no longer exists in CPBT
delete from cccom_dw.fact_cpa
where bidding_cycle_id >= (
  select min(s.bidding_cycle_id)
  from cccom_dw.stg_cpbt_cpa s
)
and bidding_cycle_id not in (
  select distinct(s.bidding_cycle_id)
  from cccom_dw.stg_cpbt_cpa s
);

-- Insert slotting CPA into fact table
insert into cccom_dw.fact_cpa (
  bidding_cycle_id,
  start_date,
  end_date,
  affiliate_id,
  page_id,
  card_id,
  bid_type,
  est_cpa,
  cpa,
  effective_bid
)
select distinct cpbt.bidding_cycle_id,
cpbt.placement_start_date,
cpbt.placement_end_date,
cpbt.site_import_id, -- affiliate
cpbt.category_import_id, -- page
cpbt.prod_internal_id, -- card
cpbt.bid_type,
cpbt.est_cpa,
cpbt.cpa,
cpbt.effective_bid
from cccom_dw.stg_cpbt_cpa cpbt
where cpbt.bidding_cycle_id not in (
  select distinct f.bidding_cycle_id
  from cccom_dw.fact_cpa f
  where f.bidding_cycle_id is not null
);

-- For cards that don't have a slotting CPA, get the slotting CPA
-- from cards with the same title
insert into cccom_dw.fact_cpa (
  bidding_cycle_id,
  start_date,
  end_date,
  affiliate_id,
  page_id,
  card_id,
  bid_type,
  cpa,
  effective_bid,
  est_cpa,
  orig_card_id
)
select fc.bidding_cycle_id,
fc.start_date,
fc.end_date,
fc.affiliate_id,
fc.page_id,
no_slot.card_id,
fc.bid_type,
max(fc.cpa),
max(fc.effective_bid),
max(fc.est_cpa),
listagg(fc.card_id,',') within group (order by fc.card_id)
from cccom_dw.fact_cpa fc
join cccom_dw.dim_products p2 on (p2.card_id = fc.card_id)
join (
  -- cards without slotting but same title as card with slotting
  select bc.bidding_cycle_id, p1.card_id, p1.card_title
  from cccom_dw.dim_products p1
  cross join (select distinct bidding_cycle_id from cccom_dw.fact_cpa where bidding_cycle_id is not null) bc
  where p1.active = 1
  -- same card title
  and exists (
    select 1
    from cccom_dw.fact_cpa fc0
    join cccom_dw.dim_products p0 on (p0.card_id = fc0.card_id)
    where fc0.bidding_cycle_id = bc.bidding_cycle_id
    and p0.card_title = p1.card_title
  )
  -- no slotting
  and not exists (
    select 1
    from cccom_dw.fact_cpa fc0
    join cccom_dw.dim_products p0 on (p0.card_id = fc0.card_id)
    where fc0.bidding_cycle_id = bc.bidding_cycle_id
    and p0.card_id = p1.card_id
  )
) no_slot on (no_slot.card_title = p2.card_title and no_slot.bidding_cycle_id = fc.bidding_cycle_id)
group by fc.bidding_cycle_id,
fc.start_date,
fc.end_date,
fc.affiliate_id,
fc.page_id,
no_slot.card_id,
fc.bid_type;

-- Get month-to-date sales and use it to estimate end-of-month sales
update cccom_dw.fact_cpa
set mtd_sales = t.num_sales,
mtd_sales_days = t.num_sales_days,
mtd_sales_daily_avg =
  case
    when t.num_sales_days = 0 then null
    else t.num_sales::float/t.num_sales_days
  end,
eom_est_sales =
  case
    when trunc(sysdate) < end_date + 1 and t.num_sales_days > 0 then
      t.num_sales + (datediff('day', trunc(sysdate), end_date + 1) * t.num_sales::float / t.num_sales_days)
    else
      t.num_sales
  end,
load_date = sysdate
from (
  select f.start_date,
  f.page_id,
  f.card_id,
  fp.card_title,
  f.bid_type,
  t0.num_sales,
  t0.num_sales_days
  from cccom_dw.fact_cpa f
  join cccom_dw.dim_products fp on (fp.card_id = f.card_id)
  join (
    -- Get total sales by month, page, and card title (not card ID)
    select date_trunc('month',provider_process_date) first_of_month, pg.page_id, p.card_title,
    count(distinct s.trans_id) num_sales,
    count(distinct trunc(s.provider_process_date)) num_sales_days
    from cccom_dw.fact_sales s
    join cccom_dw.fact_clicks c using (click_id)
    join cccom_dw.dim_trans_types tt using (trans_type_key)
    join cccom_dw.dim_products p on (p.product_key = s.product_key)
    join cccom_dw.dim_pages pg on (pg.page_key = s.exit_page_key)
    join cccom_dw.dim_websites w using (website_key)
    where provider_process_date >= '2017-01-01'
    and s.provider_process_date < sysdate
    and tt.trans_type in (4, 60, 95)
    and w.website_id in (5696, 4621, 39712)
    group by first_of_month, pg.page_id, p.card_title
  ) t0 on (t0.first_of_month = f.start_date and t0.page_id = f.page_id and t0.card_title = fp.card_title)
  where f.bid_type = 2
) t
where fact_cpa.start_date = t.start_date
and fact_cpa.page_id = t.page_id
and fact_cpa.card_id = t.card_id
and fact_cpa.bid_type = t.bid_type;

-- Use end-of-month sales estimate to calculate lump sum slotting CPA.
-- For day 6 or later, add 12% to the estimated end-of-month sales
update cccom_dw.fact_cpa
set eom_est_cpa =
  case
    when effective_bid is not null
        and trunc(sysdate) >= start_date
        and trunc(sysdate) <= end_date
        and extract('day' from sysdate) > 5 then
      ((eom_est_sales::float * 1.12 * cpa) + effective_bid)/(eom_est_sales::float * 1.12)
    when effective_bid is not null
        and trunc(sysdate) > end_date then
      ((eom_est_sales::float * 1.0 * cpa) + effective_bid)/(eom_est_sales::float * 1.0)
    else
      est_cpa
  end
where bid_type = 2;

-- Delete existing RAM CPA
delete from cccom_dw.fact_cpa
where payin_tier_id is not null;

-- Refresh the base CPA staging table
truncate table cccom_dw.stg_base_cpa;

insert into cccom_dw.stg_base_cpa (
  payin_tier_id,
  website_id,
  affiliate_id,
  card_id,
  start_date,
  end_date,
  amount
)
select wt.payin_tier_id,
  w.website_id,
  a.affiliate_id,
  p.card_id,
  greatest(wt.start_date, pt.start_date) as start_date,
  least(wt.end_date, pt.end_date) as end_date,
  pt.amount
  from cccom_dw.dim_website_payin_tiers wt
  join cccom_dw.dim_websites w on (w.website_key = wt.website_key)
  join cccom_dw.dim_affiliates a on (a.affiliate_key = w.affiliate_key)
  join cccom_dw.dim_product_payin_tiers pt on (pt.payin_tier_id = wt.payin_tier_id)
  join cccom_dw.dim_products p on (p.product_key = pt.product_key)
  where 1
  and w.website_id = 5696 -- only creditcards.com, not other websites we're using for slotting
  and wt.payin_tier_default = 0
  and wt.deleted = 0
  and pt.deleted = 0
  and nvl(wt.start_date, sysdate) <= sysdate
  and nvl(wt.end_date, sysdate) >= '2017-01-01'
  and nvl(pt.start_date, sysdate) <= sysdate
  and nvl(pt.end_date, sysdate) >= '2017-01-01'
union
select pt.payin_tier_id,
  null as website_id,
  '104000' as affiliate_id,
  p.card_id,
  pt.start_date,
  pt.end_date,
  pt.amount
  from cccom_dw.dim_product_payin_tiers pt
  join cccom_dw.dim_products p on (p.product_key = pt.product_key)
  join cccom_dw.dim_merchants m on (m.merchant_key = p.merchant_key)
  where pt.payin_tier_default = 1
  and pt.deleted = 0
  and nvl(pt.start_date, sysdate) <= sysdate
  and nvl(pt.end_date, sysdate) >= '2017-01-01'
  and m.merchant_key != -1;

-- Site-Product CPA from RAM
insert into cccom_dw.fact_cpa (
  bidding_cycle_id,
  payin_tier_id,
  start_date,
  end_date,
  affiliate_id,
  page_id,
  card_id,
  bid_type,
  est_cpa
)
select null,
payin_tier_id,
start_date,
end_date,
affiliate_id,
null,
card_id,
-1,
amount
from cccom_dw.stg_base_cpa b
where website_id = 5696;

-- Insert Product CPA from RAM into fact table if it doesn't already contain
-- a Site-Product CPA.
insert into cccom_dw.fact_cpa (
  bidding_cycle_id,
  payin_tier_id,
  start_date,
  end_date,
  affiliate_id,
  page_id,
  card_id,
  bid_type,
  est_cpa
)
select null,
payin_tier_id,
start_date,
end_date,
affiliate_id,
null,
card_id,
-1,
amount
from cccom_dw.stg_base_cpa b
where website_id is null
and (card_id, start_date) not in (
  select f.card_id, f.start_date
  from cccom_dw.fact_cpa f
  where f.payin_tier_id is not null
);

-- Delete CPBT Product CPA
delete from cccom_dw.fact_cpa
where payin_tier_id is null
and bidding_cycle_id is null;

-- Insert CPBT Product CPA into fact table if not in RAM. Use separate insert
-- statements to fill in gaps in dates.
insert into cccom_dw.fact_cpa (
  bidding_cycle_id,
  payin_tier_id,
  start_date,
  end_date,
  affiliate_id,
  page_id,
  card_id,
  bid_type,
  est_cpa
)
select null,
null,
'2017-01-01',
t.start_date - 1,
-1,
null,
internal_id,
-1,
base_cpa
from cccom_dw.stg_cpbt_product p
join (
  select f.card_id, min(f.start_date) start_date
  from cccom_dw.fact_cpa f
  where f.payin_tier_id is not null
  group by f.card_id
) t on (t.card_id = p.internal_id)
where t.start_date > '2017-01-01'
and active = 1;

insert into cccom_dw.fact_cpa (
  bidding_cycle_id,
  payin_tier_id,
  start_date,
  end_date,
  affiliate_id,
  page_id,
  card_id,
  bid_type,
  est_cpa
)
select null,
null,
t.end_date + 1,
trunc(sysdate),
-1,
null,
internal_id,
-1,
base_cpa
from cccom_dw.stg_cpbt_product p
join (
  select f.card_id, max(nvl(f.end_date, trunc(sysdate))) end_date
  from cccom_dw.fact_cpa f
  where f.payin_tier_id is not null
  group by f.card_id
) t on (t.card_id = p.internal_id)
where t.end_date < trunc(sysdate)
and active = 1;

insert into cccom_dw.fact_cpa (
  bidding_cycle_id,
  payin_tier_id,
  start_date,
  end_date,
  affiliate_id,
  page_id,
  card_id,
  bid_type,
  est_cpa
)
select null,
null,
'2017-01-01',
trunc(sysdate),
-1,
null,
internal_id,
-1,
base_cpa
from cccom_dw.stg_cpbt_product p
where p.internal_id not in (
  select f.card_id
  from cccom_dw.fact_cpa f
  where (f.payin_tier_id is not null or f.bidding_cycle_id is null)
)
and active = 1;

end transaction;