INSERT INTO cccomus.summarized_issuer_performance (
     issuer_id,
     product_id,
     website_id,
     event_date,
     click_count,
     application_count,
     approval_count,
     revenue,
     cross_sale_count,
     cross_sale_revenue
) 
SELECT
     r.issuer_id,
     r.product_id,
     r.website_id,
     r.event_date,
     SUM(r.clicks) AS click_count,
     SUM(r.applications) AS application_count,
     SUM(r.approvals) AS approval_count,
     SUM(r.total_revenue) AS revenue,
     SUM(r.cross_sales) AS cross_sale_count,
     SUM(r.cross_sale_revenue) AS cross_sale_revenue 
FROM
     (
          (SELECT 
               p.issuer_id,
               product_id,
               website_id,
               DATE_FORMAT(sc.click_date, '%Y-%m-%d') AS event_date,
               COALESCE(SUM(transaction_count), 0) AS clicks,
               0 AS applications,
               0 AS approvals,
               0.0 AS total_revenue,
               0 AS cross_sales,
               0.0 AS cross_sale_revenue 
          FROM
               cccomus.summarized_clicks sc 
               INNER JOIN cms.programs p
                    ON p.program_id = sc.program_id 
          WHERE sc.click_date >= '{{ds}}' - INTERVAL 90 DAY        
          AND  sc.click_date  <  '{{ds}}' + INTERVAL 1 DAY
          GROUP BY p.issuer_id,
               sc.product_id,
               sc.website_id,
               event_date) 
          UNION
          (SELECT 
               p.issuer_id,
               sa.product_id,
               sa.website_id,
               DATE_FORMAT(
                    sa.app_submission_date,
                    '%Y-%m-%d'
               ) AS event_date,
               0 AS clicks,
               COUNT(1) AS applications,
               0 AS approvals,
               0.0 AS total_revenue,
               0 AS cross_sales,
               0.0 AS cross_sale_revenue 
          FROM
               cccomus.summarized_applications sa 
               INNER JOIN cms.programs p
                    ON p.program_id = sa.program_id 
          WHERE sa.app_submission_date >= '{{ds}}' - INTERVAL 90 DAY   
               AND sa.app_submission_date < '{{ds}}' + INTERVAL 1 DAY
          GROUP BY p.issuer_id,
               sa.product_id,
               sa.website_id,
               event_date) 
          UNION
          (SELECT 
               p.issuer_id,
               tse.product_id,
               tse.website_id,
               DATE_FORMAT(tse.process_date, '%Y-%m-%d') AS event_date,
               0 AS clicks,
               0 AS applications,
               COUNT(1) AS approvals,
               COALESCE(SUM(revenue), 0) AS total_revenue,
               0 AS cross_sales,
			   0 AS cross_sale_revenue
          FROM
               cccomus.transactions_sale_external tse 
               INNER JOIN cms.programs p
                    ON p.program_id = tse.program_id 
          WHERE tse.process_date >= '{{ds}}' - INTERVAL 90 DAY 
               AND tse.process_date < '{{ds}}' + INTERVAL 1 DAY
          GROUP BY p.issuer_id,
               tse.product_id,
               tse.website_id,
               event_date)
		  UNION
          (SELECT 
               p.issuer_id,
               tse.product_id,
               tse.website_id,
               DATE_FORMAT(tse.process_date, '%Y-%m-%d') AS event_date,
               0 AS clicks,
               0 AS applications,
               0 AS approvals,
               0 AS total_revenue,
               COUNT(1) AS cross_sales,
               COALESCE(SUM(revenue), 0) AS cross_sale_revenue
          FROM
               cccomus.transactions_sale_external tse
               INNER JOIN cms.programs p
                    ON p.program_id = tse.program_id
          WHERE tse.process_date >= '{{ds}}' - INTERVAL 90 DAY
               AND tse.process_date < '{{ds}}' + INTERVAL 1 DAY
			   AND LENGTH(tse.cross_sale) > 0
          GROUP BY p.issuer_id,
               tse.product_id,
               tse.website_id,
               event_date)
     ) AS r
GROUP BY r.issuer_id,
     r.product_id,
     r.website_id,
     r.event_date ;
