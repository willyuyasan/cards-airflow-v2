SELECT
DATE(date_inserted) AS transaction_date,
product_id,
program_id,
product_type_id,
website_id,
affiliate_id,
exit_page_id as category_fid,
user_variable,
COUNT(1) AS click_count
FROM
cccomus.transactions_click_external
WHERE
date_inserted BETWEEN #{mySummarizedStartDate} AND NOW()
GROUP BY
transaction_date,
product_id,
program_id,
product_type_id,
website_id,
affiliate_id,
category_fid,
user_variable;
