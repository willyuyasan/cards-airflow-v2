BEGIN transaction;

--update md5 vals in stage table
UPDATE cccom_dw.stg_payin_tiers_for_cpa
   SET md5_vals = MD5(payin_type::varchar || '/' || payin_tier_id::varchar || '/' || start_date::varchar || '/' || card_id::varchar || '/' ||COALESCE (orig_card_id::varchar,'') || '/' ||COALESCE (affiliate_id::varchar,''))
WHERE md5_vals = '';

--update any changes to end_date in fact table if it is changed in stage table
UPDATE cccom_dw.fact_cpa
   SET end_date = sc.end_date
FROM cccom_dw.stg_payin_tiers_for_cpa sc
WHERE sc.md5_vals = fact_cpa.md5_vals
AND   sc.end_date is not NULL
AND   fact_cpa.end_date is NULL;

--insert new md5s from stage table into fact table
INSERT INTO cccom_dw.fact_cpa
(
  payin_type,
  bidding_cycle_id,
  payin_tier_id,
  start_date,
  end_date,
  affiliate_id,
  page_id,
  card_id,
  bid_type,
  cpa,
  effective_bid,
  est_cpa,
  mtd_sales,
  mtd_sales_days,
  mtd_sales_daily_avg,
  eom_est_sales,
  eom_est_cpa,
  orig_card_id,
  md5_vals
)
SELECT payin_type,
       bidding_cycle_id,
       payin_tier_id,
       start_date,
       end_date,
       affiliate_id,
       page_id,
       card_id,
       bid_type,
       cpa,
       effective_bid,
       est_cpa,
       mtd_sales,
       mtd_sales_days,
       mtd_sales_daily_avg,
       eom_est_sales,
       eom_est_cpa,
       orig_card_id,
       md5_vals
FROM cccom_dw.stg_payin_tiers_for_cpa sc
WHERE sc.md5_vals not in (select nvl(md5_vals, '') from cccom_dw.fact_cpa);

END transaction;