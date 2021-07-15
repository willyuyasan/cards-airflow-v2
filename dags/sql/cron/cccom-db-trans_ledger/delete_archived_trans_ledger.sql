delete from transactions_ledger
where  insert_time < DATE_SUB( DATE( NOW() ), INTERVAL 60 DAY );
