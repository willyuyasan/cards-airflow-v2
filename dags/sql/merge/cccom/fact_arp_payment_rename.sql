Begin Transaction;
ANALYZE affiliated_reporting.fact_payment_load;
ALTER TABLE affiliated_reporting.fact_payment RENAME TO fact_payment_temphold;
ALTER TABLE affiliated_reporting.fact_payment_load RENAME TO fact_payment;
ALTER TABLE affiliated_reporting.fact_payment_temphold RENAME TO fact_payment_load;

COMMIT;
