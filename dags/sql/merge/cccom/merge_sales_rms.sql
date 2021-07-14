-- amex new campaign started 05/04/2019 , signature : provider_file_id starts with 1425453_*

begin transaction;

    delete from cccom_dw.fact_sales
    where trans_id in (
      select t.transaction_event_id::varchar from cccom_dw.stg_rms_transactions t);

    insert into cccom_dw.fact_sales(
            trans_id,
            source_key,
            click_id,
            click_date,
            trans_type_key,
            trans_type,
            event_type_code,
            payout_status,
            order_id,
            product_key,
            affiliate_key,
            commission,
            ip,
            recurring_comm_id,
            cross_sale_product_key,
            campaign_id,
            keyword_key,
            exit_page_key,
            page_position_key,
            provider_event_date_key,
            provider_event_date,
            provider_process_date_key,
            provider_process_date,
            merchant_name,
            issuer_name,
            provider_id,
            quantity,
            provider_channel,
            estimated_revenue,
            date_estimated,
            external_visit_id,
            source_table_id,
            load_date,
            referrer_url,
            click_datetime,
            provider_process_datetime,
            provider_action_name,
            count_as_sale,
            count_as_sale_comments,
            product_sku,
            expected_revenue,
            stated_revenue,
            adjustment_id,
            adjustment_amt,
            referring_website_id,
            referring_affiliate_id,
            product_name,
            clicked_product_id,
            expected_referral_revenue,
            estimated_data_filename
    )
select      t.transaction_event_id::varchar    trans_id,
            1    source_key,
            t.tracking_id    click_id,
            t.origination_date    click_date,
            nvl(dtt.trans_type_key,-1)    trans_type_key,
            Case t.event_type_code
              when 'APR' then 4
              when 'APP' then 5
              when 'REV' then 130
              else -1
            end trans_type,
            t.event_type_code event_type_code,
            1    payout_status,
            nvl(t.order_id,'')    order_id,
            nvl(t.product_key,-1)    product_key,
            nvl(da.affiliate_key,-1)    affiliate_key,
            nvl(t.commission,0)    commission,
            nvl(fc.ip_address,'-1')    ip,
            nvl(fc.cardmatch_offer_id,'')    recurring_comm_id,
            case when fc.product_key <> t.product_key then
                fc.product_key
            else
               case when t.expected_revenue > 0 and 
                         t.stated_revenue   > 0 then
                   case when t.expected_revenue <> t.stated_revenue then
                      fc.product_key
                   else     
                      null
                   end 
               else
                  null
               end          
            end  cross_sale_product_key,     
            fc.campaign_id    campaign_id,
            nvl(fc.keyword_key,-1)    keyword_key,
            nvl(fc.exit_page_key,-1)    exit_page_key,
            nvl(fc.page_position_key,-1)    page_position_key,
            to_number(to_char(date(t.provider_event_date),'YYYYMMDD'),'99999999') provider_event_date_key,
            t.provider_event_date    provider_event_date,
            to_number(to_char(date(nvl(t.issuer_process_date,sysdate)),'YYYYMMDD'),'99999999')  provider_process_date_key,
            nvl(t.issuer_process_date,sysdate)    provider_process_date,
            nvl(t.issuer_id,-1)    merchant_name,
            nvl(t.issuer_name,'') issuer_name,
            nvl(t.provider_id,-1)    provider_id,
            1    quantity,
            nvl(t.provider_channel,'')    provider_channel,
            nvl(t.actual_revenue,0)    estimated_revenue,
            t.te_create_date    date_estimated,
            nvl(fc.external_visit_id,'')    external_visit_id,
            1    source_table_id,
            sysdate    load_date,
            fc.referrer_url    referrer_url,
            t.origination_date    click_datetime,
            nvl(t.issuer_process_date,sysdate)    provider_process_datetime,
            ''    provider_action_name,
            case when (fc.product_key != t.product_key
                    or (sum(nvl(t.expected_revenue,0))
                        over(partition by t.tracking_id, t.order_id)
                      <> sum(nvl(t.stated_revenue,0))
                          over(partition by t.tracking_id, t.order_id)
                       and nvl(t.expected_revenue,0) <> 0
                       and nvl(t.stated_revenue,0) <> 0))
                and sum(nvl(t.actual_revenue,0))
                    over(partition by t.tracking_id, t.order_id) < t.exclude_revenue then
                0
            else
                1
            end count_as_sale, -- sales exclusion rule
            case when (fc.product_key != t.product_key
                    or (sum(nvl(t.expected_revenue,0))
                        over(partition by t.tracking_id, t.order_id)
                      <> sum(nvl(t.stated_revenue,0))
                          over(partition by t.tracking_id, t.order_id)
                       and nvl(t.expected_revenue,0) <> 0
                       and nvl(t.stated_revenue,0) <> 0))
                and sum(nvl(t.actual_revenue,0))
                    over(partition by t.tracking_id, t.order_id) < t.exclude_revenue then
                t.exclusion_type
            else
                ''
            end   count_as_sale_comments,
            t.product_sku    product_sku,
            t.expected_revenue expected_revenue,
            t.stated_revenue stated_revenue,
            t.tec_adjustment_id adjustment_id,
            t.tec_adjustment_amt adjustment_amt,
            t.referring_website_id referring_website_id,
            t.referring_affiliate_id referring_affiliate_id,
            t.product_name product_name,
            t.clicked_product_id clicked_product_id,
            t.expected_referral_revenue expected_referral_revenue,
            t.provider_file_id estimated_data_filename
    from (  select  row_number() over (partition by t.tracking_id, t.transaction_event_id  order by ex.rule_id desc) rule_order,
                    ex.rule_id, 
                    ex.revenue exclude_revenue, 
                    ex.exclusion_type,
                    dp.merchant_key, 
                    dp.product_key,
                    t.*
              from (  select s.*
                        from cccom_dw.stg_rms_transactions s
                             left join cccom_dw.fact_sales f on (s.transaction_event_id::varchar = f.trans_id)
                       where f.trans_id is null
                         and s.event_type_code in ( 'APP','APR','REV' ) 
                         and ( s.event_type_code in ('APP') or 
                             ( s.event_type_code in ('REV') and s.tracking_id <> '')  or
      	                     ( s.event_type_code  =  'APR' and s.provider_file_id not like '1425453%') )              
                   ) t
                   left join cccom_dw.dim_products dp on (dp.card_id = t.product_id)
                   left join cccom_dw.ctl_sales_exclusions_rms ex on (nvl(ex.merchant_key::int, dp.merchant_key) = dp.merchant_key
                                                                 and  nvl(ex.product_key::int, dp.product_key) = dp.product_key)
            union all
            select 1 rule_order,
                   1 , 
                   0 exclude_revenue, 
                   '',
                   dp.merchant_key, 
                   dp.product_key,
                   t.*
             from ( select s.*
                      from cccom_dw.stg_rms_transactions s
                           left join cccom_dw.fact_sales f on (s.transaction_event_id::varchar = f.trans_id)
                     where f.trans_id is null
                       and s.event_type_code    in ( 'APR' ) 
                       and s.provider_file_id like '1425453%'              
                  ) t
                  left join cccom_dw.dim_products dp on (dp.card_id = t.product_id)
          ) t
    join cccom_dw.dim_trans_types dtt on ( dtt.trans_type = (Case t.event_type_code
                                                               when 'APR' then 4
                                                               when 'APP' then 5
                                                               when 'REV' then 130
                                                               else -1
                                                             end ) )
    left join cccom_dw.dim_affiliates da on (da.affiliate_id = t.affiliate_id)
    left join cccom_dw.fact_clicks fc on (t.tracking_id = fc.click_id
        and t.origination_date = fc.click_date)
    where rule_order = 1;

    delete from cccom_dw.fact_sales
    where provider_process_datetime >= (select min(nvl(s.issuer_process_date,sysdate)) from cccom_dw.stg_rms_transactions s)
    and trans_id not in (select stg.transaction_event_id::varchar from cccom_dw.stg_rms_transactions stg)
    -- cutover date logic - we are not going to insert RMS make good and prepayments into fact_sales
    -- trans_type_key:110 - 102/make good
    -- trans_type_key:117 - 120/Prepayment
    and event_type_code is not null -- this is to avoid deleting 102/120 adjustments from REX (12/01/2018 to 12/31/2018)
    and provider_process_date >= '01-Feb-2019';

end transaction;