select  id adjustment_id,
        type adjustment_type,
        sub_type adjustment_sub_type,
        status adjustment_status,
        issuer_id issuer_id,
        affiliate_id affiliate_id,
        product_id product_id,
        effective_start_dt effective_start_date,
        effective_end_dt effective_end_date,
        amount adjustment_amount,
        payment_id  payment_id,
        affected_transactions affected_transactions,
        website_id website_id,
        deleted deleted,
        regexp_replace("comments", E'[\\n\\r\\f\\u000B\\u0085\\u2028\\u2029]+', ' ', 'g' ) "comments",
        create_dt create_date,
        create_by create_by,
        update_dt update_date,
        update_by update_by
from    transactions.adjustments
where   sub_type <> 'SLOTTING_CPA'
-- cutover date logic: effective_start_dt (from transactions.adjustments table) >= 01/01/2019 - from RMS
and     effective_end_dt >= '2019-02-01'
and  (  create_dt >= date_trunc('month', now()::date) - interval '3 month'
      or
        update_dt >= date_trunc('month', now()::date) - interval '3 month'
    );