REPLACE INTO partner_website_conversion_rates
SELECT
     website_id,
     'Yesterday' AS period,
     SUM(clicks) AS clicks,
     SUM(sales) AS sales,
     SUM(commission) AS commission,
     SUM(adjustments) AS adjustments,
     SUM(total) AS total,
     CAST(
          SUM(total) / SUM(clicks) * 100 AS DECIMAL (18, 2)
     ) AS epc,
     CAST(
          SUM(sales) / SUM(clicks) * 100 AS DECIMAL (18, 2)
     ) AS conversion
FROM
     partner_website_daily_stats
WHERE transaction_date = ADDDATE(CAST(NOW() AS DATE), INTERVAL - 1 DAY) -- Yesterday
GROUP BY website_id
UNION
SELECT
     website_id,
     'This Week' AS period,
     SUM(clicks) AS clicks,
     SUM(sales) AS sales,
     SUM(commission) AS commission,
     SUM(adjustments) AS adjustments,
     SUM(total) AS total,
     CAST(
          SUM(total) / SUM(clicks) * 100 AS DECIMAL (18, 2)
     ) AS epc,
     CAST(
          SUM(sales) / SUM(clicks) * 100 AS DECIMAL (18, 2)
     ) AS conversion
FROM
     partner_website_daily_stats
WHERE WEEKOFYEAR(transaction_date) = WEEKOFYEAR(CAST(NOW() AS DATE))
     AND YEAR(transaction_date) = YEAR(CAST(NOW() AS DATE)) -- This Week
GROUP BY website_id
UNION
SELECT
     website_id,
     'Last Week' AS period,
     SUM(clicks) AS clicks,
     SUM(sales) AS sales,
     SUM(commission) AS commission,
     SUM(adjustments) AS adjustments,
     SUM(total) AS total,
     CAST(
          SUM(total) / SUM(clicks) * 100 AS DECIMAL (18, 2)
     ) AS epc,
     CAST(
          SUM(sales) / SUM(clicks) * 100 AS DECIMAL (18, 2)
     ) AS conversion
FROM
     partner_website_daily_stats
WHERE WEEKOFYEAR(transaction_date) = WEEKOFYEAR(CAST(NOW() AS DATE)) - 1
     AND YEAR(transaction_date) = YEAR(CAST(NOW() AS DATE)) -- Last Week
GROUP BY website_id
UNION
SELECT
     website_id,
     'This Month' AS period,
     SUM(clicks) AS clicks,
     SUM(sales) AS sales,
     SUM(commission) AS commission,
     SUM(adjustments) AS adjustments,
     SUM(total) AS total,
     CAST(
          SUM(total) / SUM(clicks) * 100 AS DECIMAL (18, 2)
     ) AS epc,
     CAST(
          SUM(sales) / SUM(clicks) * 100 AS DECIMAL (18, 2)
     ) AS conversion
FROM
     partner_website_daily_stats
WHERE MONTH(transaction_date) = MONTH(CAST(NOW() AS DATE))
     AND YEAR(transaction_date) = YEAR(CAST(NOW() AS DATE)) -- This Month
GROUP BY website_id
UNION
SELECT
     website_id,
     'Last Month' AS period,
     SUM(clicks) AS clicks,
     SUM(sales) AS sales,
     SUM(commission) AS commission,
     SUM(adjustments) AS adjustments,
     SUM(total) AS total,
     CAST(
          SUM(total) / SUM(clicks) * 100 AS DECIMAL (18, 2)
     ) AS epc,
     CAST(
          SUM(sales) / SUM(clicks) * 100 AS DECIMAL (18, 2)
     ) AS conversion
FROM
     partner_website_daily_stats
WHERE MONTH(transaction_date) = MONTH(ADDDATE(CAST(NOW() AS DATE), INTERVAL - 1 MONTH))
     AND YEAR(transaction_date) = YEAR(ADDDATE(CAST(NOW() AS DATE), INTERVAL - 1 MONTH)) -- Last Month
GROUP BY website_id
UNION
SELECT
     website_id,
     'Last 30 Days' AS period,
     SUM(clicks) AS clicks,
     SUM(sales) AS sales,
     SUM(commission) AS commission,
     SUM(adjustments) AS adjustments,
     SUM(total) AS total,
     CAST(
          SUM(total) / SUM(clicks) * 100 AS DECIMAL (18, 2)
     ) AS epc,
     CAST(
          SUM(sales) / SUM(clicks) * 100 AS DECIMAL (18, 2)
     ) AS conversion
FROM
     partner_website_daily_stats
WHERE transaction_date > ADDDATE(CAST(NOW() AS DATE), INTERVAL - 30 DAY) -- Last 30 Days
GROUP BY website_id ;
