SELECT
   reftrans AS ref_transID,
   affiliate_id,
   provider_process_date,
   SUM(commission) AS bonus_amount
FROM partner_affiliate_sales_transactions_report
WHERE trans_type in (70,102,105,106,107,108,120)
GROUP BY reftrans;
