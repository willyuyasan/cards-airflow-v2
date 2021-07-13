
begin transaction;

    delete from cccom_dw.fact_sales_commission
    where tec_id in (
      select t.tec_id from cccom_dw.stg_rms_transaction_commission t);

    insert into cccom_dw.fact_sales_commission
    (   tec_id,
        click_id,
        click_date,
        trans_id,
        commission,
        effective_start_date,
        effective_end_date,
        adjustment_id,
        payment_id)
    select  tec_id,
            click_id,
            click_date,
            trans_id,
            commission,
            effective_start_date,
            effective_end_date,
            adjustment_id,
            payment_id
    from  cccom_dw.stg_rms_transaction_commission ssc
    where not deleted;

end transaction ;
