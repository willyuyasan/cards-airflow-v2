DELETE FROM partner_affiliate_sales_transactions_report
WHERE provider_process_date >= (CAST(NOW() AS DATE)) - INTERVAL 90 DAY
