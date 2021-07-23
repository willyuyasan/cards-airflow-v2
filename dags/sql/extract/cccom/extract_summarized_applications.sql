SELECT
transaction_date,
product_id,
program_id,
product_type_id,
website_id,
affiliate_id,
category_fid,
user_variable,
SUM(application_count_cd) AS application_count_cd,
SUM(application_count_pd) AS application_count_pd
FROM (
SELECT
DATE(t.date_inserted) AS transaction_date,
t.product_id,
t.program_id,
t.product_type_id,
t.website_id,
t.affiliate_id,
t.exit_page_id AS category_fid,
t.user_variable,
1 AS application_count_cd,
0 AS application_count_pd
FROM (SELECT transaction_id FROM cccomus.applications WHERE state = 'COMMITTED' AND created_date <= NOW()) a
JOIN cccomus.transactions_click_external t ON (t.tracking_id = a.transaction_id AND t.date_inserted BETWEEN #{mySummarizedStartDate} AND NOW())
UNION ALL
SELECT
DATE(a.submission_date) AS transaction_date,
t.product_id,
t.program_id,
t.product_type_id,
t.website_id,
t.affiliate_id,
t.exit_page_id AS category_fid,
t.user_variable,
0 AS application_count_cd,
1 AS application_count_pd
FROM (SELECT transaction_id, submission_date FROM cccomus.applications
WHERE state = 'COMMITTED' AND submission_date BETWEEN #{mySummarizedStartDate} AND NOW()) a
JOIN cccomus.transactions_click_external t ON (t.tracking_id = a.transaction_id)
) a
GROUP BY
transaction_date,
product_id,
program_id,
product_type_id,
website_id,
affiliate_id,
category_fid,
user_variable;
