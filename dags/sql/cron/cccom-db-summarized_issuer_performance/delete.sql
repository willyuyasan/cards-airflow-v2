DELETE 
FROM
     cccomus.summarized_issuer_performance 
WHERE event_date >= '{{ds}}' - INTERVAL 90 DAY        
AND  event_date  <  '{{ds}}' + INTERVAL 1 DAY
