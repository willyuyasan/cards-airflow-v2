
begin transaction;

    delete from cccom_dw.fact_rms_adjustments
    where adjustment_id in (
      select adjustment_id from cccom_dw.stg_rms_adjustments);

    insert into cccom_dw.fact_rms_adjustments(
            adjustment_id,
            adjustment_type,
            adjustment_sub_type,
            adjustment_status,
            issuer_id,
            affiliate_key,
            product_key,
            effective_start_date,
            effective_end_date,
            adjustment_amount,
            payment_id,
            affected_transactions,
            website_key,
            comments,
            create_date,
            create_by,
            update_date,
            update_by
    )
    select  a.adjustment_id,
            a.adjustment_type,
            a.adjustment_sub_type,
            a.adjustment_status,
            a.issuer_id,
            nvl(da.affiliate_key,-1),
            nvl(dp.product_key, -1),
            a.effective_start_date,
            a.effective_end_date,
            a.adjustment_amount,
            a.payment_id,
            a.affected_transactions,
            nvl(dw.website_key,-1),
            a.comments,
            a.create_date,
            a.create_by,
            a.update_date,
            a.update_by
    from    cccom_dw.stg_rms_adjustments a
    left join cccom_dw.dim_affiliates da
    on (a.affiliate_id = da.affiliate_id)
    left join cccom_dw.dim_products dp
    on (a.product_id = dp.card_id)
    left join cccom_dw.dim_websites dw
    on (a.website_id = dw.website_id)
    where not a.deleted;

end transaction;