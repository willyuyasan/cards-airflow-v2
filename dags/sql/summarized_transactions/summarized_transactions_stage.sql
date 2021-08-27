--TRUNCATE TABLE cccomus.summarized_transactions_stage;
DELETE FROM cccomus.summarized_transactions_stage;
INSERT INTO cccomus.summarized_transactions_stage (
  transaction_date,
  product_id,
  product_name,
  program_id,
  program_name,
  product_type_id,
  product_type_name,
  category_fid,
  category_name,
  website_id,
  affiliate_id,
  website_url,
  user_variable,
  click_count,
  application_count_cd,
  application_count_pd,
  sale_count_cd,
  sale_count_pd,
  sale_amount_cd,
  sale_amount_pd,
  adjustment_amount_cd,
  adjustment_amount_pd,
  hashed_key
)
SELECT DATE(t.transaction_date) AS transaction_date,
  t.product_id,
  IFNULL(c.cardTitle,'') AS product_name,
  t.program_id,
  IFNULL(pr.program_name, '') AS program_name,
  t.product_type_id,
  IFNULL(pt.product_type_name, 'Other') AS product_type_name,
  t.category_fid,
  IFNULL(pd.page_name, '') AS category_name,
  t.website_id,
  t.affiliate_id,
  IFNULL(pw.url,'') AS website_url,
  IFNULL(t.user_variable,'Other') AS user_variable,
  IFNULL(click_count,0),
  IFNULL(application_count_cd,0),
  IFNULL(application_count_pd,0),
  IFNULL(sale_count_cd,0),
  IFNULL(sale_count_pd,0),
  IFNULL(sale_amount_cd,0),
  IFNULL(sale_amount_pd,0),
  IFNULL(adjustment_amount_cd,0),
  IFNULL(adjustment_amount_pd,0),
  MD5(
    CONCAT(
      t.transaction_date, ',',
      t.product_id, ',',
      IFNULL(t.program_id,0), ',',
      t.product_type_id, ',',
      t.website_id, ',',
      t.affiliate_id, ',',
      t.category_fid, ',',
      IFNULL(t.user_variable,'Other')
    )
  )
FROM (
  SELECT transaction_date,
    product_id,
    IFNULL(program_id, 0) AS program_id,
    product_type_id,
    website_id,
    affiliate_id,
    category_fid,
    user_variable,
    SUM(click_count) AS click_count,
    SUM(application_count_cd) AS application_count_cd,
    SUM(application_count_pd) AS application_count_pd,
    SUM(sale_count_cd) AS sale_count_cd,
    SUM(sale_count_pd) AS sale_count_pd,
    SUM(sale_amount_cd) AS sale_amount_cd,
    SUM(sale_amount_pd) AS sale_amount_pd,
    SUM(adjustment_amount_cd) AS adjustment_amount_cd,
    SUM(adjustment_amount_pd) AS adjustment_amount_pd
  FROM (
    SELECT transaction_date,
      product_id,
      program_id,
      product_type_id,
      website_id,
      affiliate_id,
      category_fid,
      user_variable,
      click_count,
      0 AS sale_count_cd,
      0 AS sale_count_pd,
      0 AS sale_amount_cd,
      0 AS sale_amount_pd,
      0 AS adjustment_amount_cd,
      0 AS adjustment_amount_pd,
      0 AS application_count_cd,
      0 AS application_count_pd
    FROM cccomus.summarized_clicks_stage
    UNION ALL
    SELECT transaction_date,
      product_id,
      program_id,
      product_type_id,
      website_id,
      affiliate_id,
      category_fid,
      user_variable,
      0,
      sale_count_cd,
      sale_count_pd,
      sale_amount_cd,
      sale_amount_pd,
      adjustment_amount_cd,
      adjustment_amount_pd,
      0,
      0
    FROM cccomus.summarized_sales_stage
    UNION ALL
    SELECT transaction_date,
      product_id,
      program_id,
      product_type_id,
      website_id,
      affiliate_id,
      category_fid,
      user_variable,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      application_count_cd,
      application_count_pd
    FROM cccomus.summarized_applications_stage
) t1
GROUP BY transaction_date,
product_id,
IFNULL(program_id, 0),
product_type_id,
website_id,
affiliate_id,
category_fid,
user_variable
) t
LEFT JOIN cccomus.partner_websites pw USING (website_id)
LEFT JOIN cccomus.cms_programs pr USING (program_id)
LEFT JOIN cccomus.pages pd ON t.category_fid = pd.page_id
LEFT JOIN cccomus.cms_product_types pt USING (product_type_id)
LEFT JOIN cccomus.cms_cards c ON CONVERT(t.product_id USING latin1) = c.cardId
WHERE transaction_date BETWEEN {{params["START_DATE"]}} AND {{params["END_DATE"]}};
