SELECT
    A.website_id,
    CAST(A.transaction_date AS CHAR (50)) AS transaction_date,
    SUM(COALESCE(A.clicks,0)) AS clicks,
    SUM(COALESCE(A.sales,0)) AS sales,
    SUM(COALESCE(A.commission,0)) AS commission,
    SUM(COALESCE(A.adjustments,0)) AS adjustments,
    SUM(COALESCE(A.commission,0)) + SUM(COALESCE(A.adjustments,0)) as total
FROM
    (
        SELECT
            website_id,
            DATE(date) AS transaction_date,
            SUM(totalclicks) AS clicks,
            NULL AS sales,
            NULL AS commission,
            NULL AS adjustments
        FROM
            click_summary
        WHERE
            date >= adddate( CAST(NOW() AS DATE) , INTERVAL -90 DAY)
        GROUP BY
            website_id,
            DATE(date)
        HAVING
            website_id > 0
        UNION
        SELECT
            website_id,
            DATE(date) AS transaction_date,
            NULL AS clicks,
            SUM(approvalSalesCount) AS sales,
            SUM(CAST(COALESCE(totalCommission,0) AS DECIMAL (18,2))) AS commission,
            NULL AS adjustments
        FROM
            commission_summary
        WHERE
            date >= adddate( CAST(NOW() AS DATE), INTERVAL -90 DAY)
        GROUP BY
            website_id,
            DATE(date)
        HAVING
            website_id > 0
        UNION
        SELECT
            ta.website_id,
            DATE(tr.providerprocessdate) AS transaction_date,
            NULL AS clicks,
            NULL AS Sales,
            NULL AS Commission,
            SUM(
                CASE paf.in_house
                    WHEN 1 THEN CAST(COALESCE(tr.estimatedrevenue,0) AS DECIMAL (18,2))
                    ELSE CAST(COALESCE(tr.commission,0) AS DECIMAL (18,2))
                END
            ) AS adjustments
        FROM
            transactions_recent tr
            LEFT OUTER JOIN transactions_affiliate ta
                ON tr.reftrans = ta.transaction_id
            LEFT OUTER JOIN partner_affiliates paf
                ON tr.affiliateid = paf.affiliate_id
        WHERE
            tr.providerprocessdate >= adddate( CAST(NOW() AS DATE), INTERVAL -90 DAY)
            AND tr.transtype IN (105,107)
        GROUP BY
            ta.website_id,
            DATE(tr.providerprocessdate)
    ) A
GROUP BY
    A.website_id,
    A.transaction_date;
