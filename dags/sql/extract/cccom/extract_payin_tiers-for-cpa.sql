-- grabbing site specific payins
SELECT 'site_specific' AS payin_type,
       '' as bidding_cycle_id,
       pt.payin_tier_id,
       date(max(GREATEST(ca.start_time,wa.start_time))) AS start_date,
       IFNULL(date(min(LEAST(ca.end_time,wa.end_time))), '') AS end_date,
       IFNULL(affiliate_id, ''),
       '' as page_id,
       IFNULL(ca.card_id, ''),
       1 AS bid_type,
       IFNULL(ca.amount, '') as cpa,
       '' as effective_bid,
       IFNULL(ca.amount, '') AS est_cpa,
       '' as mtd_sales,
       '' as mtd_sales_days,
       '' as mtd_sales_daily_avg,
       '' as eom_est_sales,
       '' as eom_est_cpa,
       '' AS orig_card_id,
       '' as md5_vals
       FROM cccomus.payin_tiers pt
  JOIN cccomus.payin_tier_website_assignments wa ON pt.payin_tier_id = wa.payin_tier_id
  JOIN cccomus.payin_tier_card_assignments ca ON pt.payin_tier_id = ca.payin_tier_id
  JOIN cccomus.partner_websites pw ON wa.website_id = pw.website_id
  JOIN cccomus.cms_cards cards ON ca.card_id = cards.cardid
WHERE site_code = 'USEN'
AND   pt.default = 0
AND   ca.deleted = 0
AND   ca.start_time <= NOW()
AND   COALESCE(ca.end_time,NOW()) >= NOW()
AND   wa.deleted = 0
AND   wa.start_time <= NOW()
AND   COALESCE(wa.end_time,NOW()) >= NOW()
AND (affiliate_id != 100002
OR   wa.website_id in (4621, 37601, 39712))
GROUP BY  payin_type, pt.payin_tier_id, affiliate_id, ca.card_id, bid_type, ca.amount, orig_card_id
UNION ALL
-- grabbing default payins
SELECT 'default' AS payin_type,
       '' as bidding_cycle_id,
       pt.payin_tier_id,
       date(ca.start_time) AS start_date,
       IFNULL(date(ca.end_time), '') AS end_date,
       -1 AS affiliate_id,
       '' as page_id,
       IFNULL(ca.card_id, ''),
       1 AS bid_type,
       IFNULL(ca.amount, '') as cpa,
       '' as effective_bid,
       IFNULL(ca.amount, '') AS est_cpa,
       '' as mtd_sales,
       '' as mtd_sales_days,
       '' as mtd_sales_daily_avg,
       '' as eom_est_sales,
       '' as eom_est_cpa,
       '' AS orig_card_id,
       '' as md5_vals
       FROM cccomus.payin_tiers pt
  JOIN cccomus.payin_tier_card_assignments ca ON pt.payin_tier_id = ca.payin_tier_id
  JOIN cccomus.cms_cards cc ON cc.cardid = ca.card_id
  JOIN cccomus.cms_merchants cm ON cm.merchantid = cc.merchant
WHERE 1 = 1
AND   pt.default = 1
AND   ca.deleted = 0
AND   ca.start_time <= NOW()
AND   COALESCE(ca.end_time,NOW()) >= NOW()
UNION ALL
-- Grabbing test products
SELECT 'test' AS payin_type,
       '' as bidding_cycle_id,
       pt.payin_tier_id,
       date(ca.start_time) AS start_date,
       IFNULL(date(ca.end_time), '') AS end_date,
       '' as affiliate_id,
       '' as page_id,
       IFNULL(missing.card_id, ''),
       1 AS bid_type,
       IFNULL(ca.amount, '') as cpa,
       '' as effective_bid,
       IFNULL(ca.amount, '') AS est_cpa,
       '' as mtd_sales,
       '' as mtd_sales_days,
       '' as mtd_sales_daily_avg,
       '' as eom_est_sales,
       '' as eom_est_cpa,
       IFNULL(ca.card_id, '') AS orig_card_id,
       '' as md5_vals
       FROM cccomus.payin_tiers pt
  JOIN cccomus.payin_tier_card_assignments ca ON pt.payin_tier_id = ca.payin_tier_id
  JOIN cccomus.cms_cards cards ON ca.card_id = cards.cardid
  JOIN (SELECT cardid AS card_id,
               cardTitle
        FROM cccomus.cms_cards c
          LEFT JOIN cccomus.payin_tier_card_assignments pi ON pi.card_id = c.cardid
        WHERE pi.card_id IS NULL
        AND   site_code = 'USEN') missing ON cards.cardtitle = missing.cardtitle
WHERE 1 = 1
AND   pt.default = 1
AND   ca.deleted = 0
AND   ca.start_time <= NOW()
AND   COALESCE(ca.end_time,NOW()) >= NOW();