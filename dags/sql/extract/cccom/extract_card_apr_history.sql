replace into cms.card_apr_history
SELECT
    null                    AS card_apr_history_id,
    carddata.cardId         AS card_id,
    '%s' AS insert_time,
    carddata.regularApr     AS regular_apr,
    case
        when cards.active = 0 then 'Inactive'
        else        'Active'
    end                     AS card_status
FROM cms.rt_cards AS cards
JOIN cms.cs_carddata AS carddata
    USING (cardid)
WHERE  cards.deleted != 1
