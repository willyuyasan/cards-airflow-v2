delete from cccomus.partner_affiliate_sales_transactions_report_adjustments;

INSERT INTO partner_affiliate_sales_transactions_report_adjustments
SELECT
   reftrans,
   trans_type,
   SUM(commission)
FROM
   cccomus.partner_affiliate_sales_transactions_report
WHERE
   trans_type IN (105,107)
GROUP BY reftrans, trans_type;
