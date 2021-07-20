DELETE FROM partner_affiliate_bonus_summary
WHERE provider_process_date >= (CAST(NOW() AS DATE)) - INTERVAL 90 DAY
