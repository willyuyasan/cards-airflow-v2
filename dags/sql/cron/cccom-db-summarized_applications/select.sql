SELECT
        apps.application_id,
        apps.transaction_id,
        apps.state,
        apps.submission_date                     AS app_submission_date,
        apps.last_updated                        AS app_updated_date,
        tr.dateinserted                          AS click_date,
        cards.cardId                             AS product_id,
        cards.cardTitle                          AS product_name,
        cmsp.program_id                          AS program_id,
        cmsp.program_name                        AS program_name,
        cards.merchant                           AS issuer_id,
        m.merchantname                           AS issuer_name,
        tr.affiliateid,
        ta.website_id,
        tr.data1
  FROM cccomus.applications                AS apps
       JOIN cccomus.transactions_recent    AS tr         ON apps.transaction_id = tr.transid
       JOIN cccomus.transactions_affiliate AS ta         ON apps.transaction_id = ta.transaction_id
       JOIN cccomus.cms_cards              AS cards      ON cards.cardId        = tr.bannerid
       JOIN cccomus.cms_merchants          AS m          ON cards.merchant      = m.merchantid
       JOIN cccomus.cms_programs           AS cmsp       ON cmsp.program_id     = cards.program_id
WHERE apps.last_updated > (CAST(NOW() AS DATE)) - INTERVAL 1 DAY
AND apps.state = 'COMMITTED';
