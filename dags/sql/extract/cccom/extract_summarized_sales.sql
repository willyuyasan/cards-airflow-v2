SELECT
transaction_date,
product_id,
program_id,
product_type_id,
website_id,
affiliate_id,
category_fid,
user_variable,
SUM(sale_count_cd) AS sale_count_cd,
SUM(sale_count_pd) AS sale_count_pd,
SUM(sale_amount_cd) AS sale_amount_cd,
SUM(sale_amount_pd) AS sale_amount_pd,
SUM(adjustment_amount_cd) AS adjustment_amount_cd,
SUM(adjustment_amount_pd) AS adjustment_amount_pd
FROM
(
SELECT
DATE(click_date) AS transaction_date,
product_id,
program_id,
product_type_id,
website_id,
affiliate_id,
category_fid,
user_variable,
1 AS sale_count_cd,
0 AS sale_count_pd,
commission AS sale_amount_cd,
0 AS sale_amount_pd,
adjustments AS adjustment_amount_cd,
0 AS adjustment_amount_pd
FROM cccomus.transactions_sale_external
WHERE click_date BETWEEN #{mySummarizedStartDate} AND '#{mySummarizedDates}'
UNION ALL
SELECT
DATE(process_date) AS transaction_date,
product_id,
program_id,
product_type_id,
website_id,
affiliate_id,
category_fid,
user_variable,
0 AS sale_count_cd,
1 AS sale_count_pd,
0 AS sale_amount_cd,
commission AS sale_amount_pd,
0 AS adjustment_amount_cd,
adjustments AS adjustment_amount_pd
FROM cccomus.transactions_sale_external
WHERE process_date BETWEEN #{mySummarizedStartDate} AND '#{mySummarizedDates}'
) t
GROUP BY
transaction_date,
product_id,
program_id,
product_type_id,
website_id,
affiliate_id,
category_fid,
user_variable;
