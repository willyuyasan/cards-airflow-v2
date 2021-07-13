
truncate table cccom_dw.fact_arp_adjustments;

INSERT INTO cccom_dw.fact_arp_adjustments
(
  adjustment_id,
  adjustment_type_id,
  adjustment_type,
  adjustment_sub_type,
  adjustment_status,
  issuer_id,
  issuer_name,
  affiliate_id,
  affiliate_name,
  product_id,
  effective_start_date,
  effective_end_date,
  adjustment_amount,
  payment_id,
  affected_transactions,
  website_id,
  comments
)
(
select null ,
       b.trans_type,
       b.label,
       null adjustment_sub_type,
       'APPROVED' adjustment_status,
       null issuer_id,
       null issuer_name,
       d.affiliate_id ,
       d.company_name  affiliate_name,
       e.card_id product_id,
       f.approval_date effective_start_date,
       f.process_date  effective_end_date,
       a.commission adjustment_amount,
       c.payout_id  payment_id ,
       null affected_transactions,
       null website_id,
       null comments
  from cccom_dw.fact_sales a
       join cccom_dw.dim_trans_types b     on b.trans_type_key = a.trans_type_key
                                          and b.trans_type  in ( 102,106, 108, 120)
       join cccom_dw.map_trans_payout c    on c.trans_id       = a.trans_id
       join cccom_dw.dim_affiliates   d    on d.affiliate_key  = a.affiliate_key
       join cccom_dw.dim_products     e    on e.product_key    = a.product_key
       join cccom_dw.fact_payouts     f    on f.payout_id      = c.payout_id
 where f.approval_date >= add_months(date_trunc('month', current_date),-18)
 and f.approval_date < '01-Feb-2019'
);


