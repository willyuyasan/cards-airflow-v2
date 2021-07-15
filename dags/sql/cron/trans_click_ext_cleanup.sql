DELETE FROM `transactions_click_external`
WHERE date_inserted < date_add(CURDATE(),INTERVAL -18 MONTH)
