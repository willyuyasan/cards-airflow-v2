DELETE FROM cccomus.summarized_applications
WHERE app_submission_date < (CAST(NOW() AS DATE)) - INTERVAL 18 MONTH;
