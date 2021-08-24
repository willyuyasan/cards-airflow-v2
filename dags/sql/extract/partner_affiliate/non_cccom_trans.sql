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
             '0' AS committed,
             t.reftrans AS reftrans,
             t.providerstatus AS comment,
             t.recurringcommid AS referral_id,
             t.payoutstatus AS payoutstatus
        FROM cccomus.transactions_upload t
   LEFT JOIN cccomus.transactions_affiliate ta on ta.transaction_id = t.reftrans
	LEFT JOIN cccomus.partner_affiliates paf on t.affiliateid = paf.affiliate_id
       WHERE (t.transtype in ( 4,5,6 ) OR ( t.transtype in (70,100,102,105,106,107,108)
       AND t.commission != 0 ))
       AND (t.providerprocessdate >= (CAST(NOW() AS DATE)) - INTERVAL 90 DAY OR t.providerprocessdate IS NULL
	    and t.dateestimated >= (CAST(NOW() AS DATE)) - INTERVAL 90 DAY);
