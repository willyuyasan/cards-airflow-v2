SELECT
             t.transid,
             t.affiliateid,
             IFNULL(ta.website_id, 0) AS website_id,
             IFNULL(t.data1, '') AS uv,
             IFNULL(t.bannerid, 0) AS card_id,
             IFNULL(t.exit, 0) as exit_page_id,
             IFNULL(t.dateinserted, t.dateestimated) AS click_date,
             IFNULL(t.providerprocessdate, t.dateestimated) AS provider_process_date,
             t.dateestimated AS estimated_date,
             t.transtype AS trans_type,
             CASE paf.in_house
				WHEN 1 THEN t.estimatedrevenue
				ELSE t.commission
			 END AS commission,
             t.data2 AS cross_sale,
             '1' AS committed,
             t.reftrans AS reftrans,
             t.providerstatus AS comment,
             t.recurringcommid AS referral_id,
             t.payoutstatus AS payoutstatus
        FROM transactions_recent t
   LEFT JOIN transactions_affiliate ta on ta.transaction_id = t.reftrans
	LEFT JOIN partner_affiliates paf on t.affiliateid = paf.affiliate_id
   WHERE (t.transtype in ( 4,5,6 ) AND t.providerprocessdate >= (CAST(NOW() AS DATE)) - INTERVAL 90 DAY)
       OR ( t.transtype in (70,100,102,105,106,107,108,120)
       AND t.commission != 0
       AND (t.dateadjusted >= '2013-05-01 00:00:00'OR t.dateadjusted IS NULL and t.dateestimated >= '2013-05-01 00:00:00')
       AND (t.providerprocessdate >= (CAST(NOW() AS DATE)) - INTERVAL 90 DAY OR t.providerprocessdate IS NULL and t.dateestimated >= (CAST(NOW() AS DATE)) - INTERVAL 90 DAY )
       );

