
truncate table cccom_dw.fact_arp_payment;

INSERT INTO cccom_dw.fact_arp_payment
    (
          affiliate_id,
          affiliate_name,
          payout_id,
          payment_create_dt,
          payment_sent_dt,
          check_wire_info,
          payment_type,
          payment_type_desc,
          amount,
          status,
          memo
    )
    (
      select b.affiliate_id affiliate_id,
             b.company_name affiliate_name,
             a.payout_id payout_id,
             a.process_date payment_crete_dt,
             a.approval_date payment_sent_dt,
             decode(a.reference , 'n' , null , a.reference ) check_wire_info,
             a.payment_type_id payment_type,
             a.payment_type_name payment_type_desc,
             a.amount amount,
             a.status,
             null memo
        from cccom_dw.fact_payouts a
             join cccom_dw.dim_affiliates b on b.affiliate_key = a.affiliate_key
       where a.approval_date >= add_months(current_date, - 18 )
   );
