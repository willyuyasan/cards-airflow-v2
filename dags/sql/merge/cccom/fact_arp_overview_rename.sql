Begin Transaction;
ANALYZE affiliated_reporting.fact_overview_load;
ALTER TABLE affiliated_reporting.fact_overview RENAME TO fact_overview_temphold;
ALTER TABLE affiliated_reporting.fact_overview_load RENAME TO fact_overview;
ALTER TABLE affiliated_reporting.fact_overview_temphold RENAME TO fact_overview_load;

COMMIT;
