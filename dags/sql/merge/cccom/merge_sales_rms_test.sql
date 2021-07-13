begin transaction;

    delete from cccom_dw.fact_sales_rms
    where trans_id in (
      select t.transaction_event_id from cccom_dw.stg_rms_transactions_test t);

    insert into cccom_dw.fact_sales_rms(
            trans_id,
            source_key,
            click_id,
            click_date,
            trans_type_key,
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
            estimated_data_filename
    )
select      t.transaction_event_id    trans_id,
            1    source_key,
            t.tracking_id    click_id,
            t.origination_date    click_date,
            nvl(det.event_type_id,-1)    trans_type_key,
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
            ''    provider_action_name, -- #TODO: Check the action name logic with Mark / Daniel
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
            t.provider_file_id  estimated_data_filename
    from (  select    row_number() over (partition by t.tracking_id, t.transaction_event_id order by ex.rule_id desc) rule_order,
                      ex.rule_id, ex.revenue exclude_revenue, ex.exclusion_type,
                      dp.merchant_key, dp.product_key,
                      t.*
            from  (  select s.*
                    from cccom_dw.stg_rms_transactions_test s
                    left join cccom_dw.fact_sales_rms f
                    on (s.transaction_event_id = f.trans_id)
                    where f.trans_id is null ) t
                    left join cccom_dw.dim_products dp
                    on (dp.card_id = t.product_id)
                    left join cccom_dw.ctl_sales_exclusions_rms ex
                    on (nvl(ex.merchant_key::int, dp.merchant_key) = dp.merchant_key
                      and nvl(ex.product_key::int, dp.product_key) = dp.product_key)
          ) t
    join cccom_dw.dim_event_types det on (t.event_type_code = det.code)
    left join cccom_dw.dim_affiliates da on (da.affiliate_id = t.affiliate_id)
    left join cccom_dw.fact_clicks fc on (t.tracking_id = fc.click_id
    and t.origination_date = fc.click_date)
    where rule_order = 1;

    delete from cccom_dw.fact_sales_rms
    where provider_process_datetime >= (select min(nvl(s.issuer_process_date,sysdate)) from cccom_dw.stg_rms_transactions_test s)
    and trans_id not in (select stg.transaction_event_id from cccom_dw.stg_rms_transactions_test stg);

end transaction;