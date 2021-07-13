select  te.id transaction_event_id,
        coalesce(t.tracking_id,'') tracking_id,
        t.origination_dt origination_datetime,
        coalesce(te.event_type_code,'') event_type_code,
        coalesce(t.issuer_id,-1) issuer_id,
        t.issuer_name issuer_name,
        coalesce(t.website_id,'') website_id,
        case 
          when t.business_id = 2 then  '127714'
          else coalesce(t.affiliate_id,'') 
        end affiliate_id,
        t.affiliate_type affiliate_type,
        coalesce(t.referring_website_id,-1) referring_website_id,
        coalesce(t.referring_affiliate_id,'') referring_affiliate_id,
        -- transactions_events
        te.provider_event_dt provider_event_date,
        te.issuer_process_dt issuer_process_date,
        coalesce(te.product_id,'') product_id,
        coalesce(te.order_id,'') order_id,
        coalesce(te.product_sku,'') product_sku,
        t.data_provider_id provider_id,
        coalesce(t.data_provider_name,'') provider_channel,
        coalesce(substr(te.provider_file_id,1,150),'') provider_file_id,
        coalesce(te.provider_file_pos,-1) provider_file_pos,
        te.create_dt te_create_date,
        coalesce (te.product_name) product_name,
        coalesce (te.clicked_product_id) clicked_product_id,
        -- transaction_event_Revenue
        sum(coalesce(ter.stated,0)) stated_revenue,
        sum(coalesce(ter.expected,0)) expected_revenue,
        sum(coalesce(ter.actual ,0)) actual_revenue,
        min(ter.effective_start_dt) ter_effective_start_dt,
        max(ter.effective_end_dt) ter_effective_end_dt,
        sum(coalesce (ter.expected_referral_revenue,0)) expected_referral_revenue,
        -- transaction_event_commission
        sum(coalesce(tec.amount,0)) commission,
        min(tec.effective_start_dt) tec_effective_start_date,
        max(tec.effective_end_dt) tec_effective_end_date,
        max(tec.adjustment_id) tec_adjustment_id,
        sum(case when tec.adjustment_id is not null then
                coalesce(tec.amount,0)
                else 0 end) adjustment_amt
from    transactions.transactions t
join    transactions.transaction_events te
on    ( t.id = te.transaction_id)
left join transactions.transaction_event_revenue ter
on ( te.id = ter.transaction_event_id)
left join transactions.transaction_event_commission tec
on ( te.id = tec.transaction_event_id)
where 1 = 1
and not t.deleted
and not te.deleted
and not coalesce(ter.deleted,False)
and not coalesce(tec.deleted,False)
and te.event_type_code in ('APP','APR','REV')
and coalesce(ter.actual,0) <> 0
and (t.create_dt >=  date_trunc('month', now()::date) - interval '3 month'
  or t.update_dt >= date_trunc('month', now()::date) - interval '3 month'
  or te.create_dt >= date_trunc('month', now()::date) - interval '3 month'
  or te.update_dt >= date_trunc('month', now()::date) - interval '3 month'
  or ter.create_dt >= date_trunc('month', now()::date) - interval '3 month'
  or ter.update_dt >= date_trunc('month', now()::date) - interval '3 month'
  or tec.create_dt >= date_trunc('month', now()::date) - interval '3 month'
  or tec.update_dt >= date_trunc('month', now()::date) - interval '3 month')
group by te.id,
        coalesce(t.tracking_id,''),
        t.origination_dt ,
        coalesce(te.event_type_code,'') ,
        coalesce(t.issuer_id,-1) ,
        t.issuer_name ,
        coalesce(t.website_id,'') ,
        case 
          when t.business_id = 2 then  '127714'
          else coalesce(t.affiliate_id,'') 
        end,
        t.affiliate_type ,
        coalesce(t.referring_website_id,-1),
        coalesce(t.referring_affiliate_id,''),
        -- transactions_events
        te.provider_event_dt ,
        te.issuer_process_dt ,
        coalesce(te.product_id,'') ,
        coalesce(te.order_id,'') ,
        coalesce(te.product_sku,'') ,
        t.data_provider_id ,
        coalesce(t.data_provider_name,'') ,
        coalesce(te.provider_file_id,'') ,
        coalesce(te.provider_file_pos,-1) ,
        te.create_dt,
        coalesce (te.product_name),
        coalesce (te.clicked_product_id);
