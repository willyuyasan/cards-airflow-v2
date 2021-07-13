-- truncate the table  
truncate table cccom_dw.fact_arp_transaction_clicks;


-- populate the table 
INSERT INTO cccom_dw.fact_arp_transaction_clicks
    (
          click_id,
          click_date,
          num_click_date,
          date_year,
          date_mon,
          date_week,
          date_dayofyear,
          affiliate_id,
          affiliate_name,
          website_id,
          website_url,
          card_id,
          card_title,
          product_type_id,
          product_type_name,
          merchant_id,
          merchant_name,
          page_id,
          page_name,
          user_variable,
          visitor_ip_address,
          http_referrer,
          banner_program
    )
    (
        select a.click_id                                              click_id                ,
               a.click_date                                            click_date              , 
               to_number(to_char(a.click_date,'YYYYMMDD'),'99999999')  num_click_date          ,
               date_part(year     , a.click_date)                      date_year               , 
               date_part(mon      , a.click_date)                      date_mon                , 
               date_part(week     , a.click_date)                      date_week               ,
               date_part(dayofyear, a.click_date)                      date_dayofyear          ,
               b.affiliate_id                                          affiliate_id            , 
               b.company_name                                          affiliate_name          ,
               c.website_id                                            website_id              ,
               c.url                                                   website_url             ,
               d.card_id                                               card_id                 ,
               d.card_title                                            card_title              ,
               e.product_type_id                                       product_type_id         ,
               e.product_type_name                                     product_type_name       ,
               f.merchant_id                                           merchant_id             ,
               f.merchant_name                                         merchant_name           ,
               g.page_id                                               page_id                 ,
               g.page_name                                             page_name               ,
               a.user_variable                                         user_variable           ,
               floor(   ( a.ip_address + 2147483647 ) / 16777216 )      || '.' ||
               floor(  (( a.ip_address + 2147483647 ) - ((floor(( a.ip_address + 2147483647 ) / 16777216)) * 16777216 ))  / 65536 )   || '.' ||
               floor( ((( a.ip_address + 2147483647 ) - ((floor(( a.ip_address + 2147483647 ) / 16777216)) * 16777216 )) - (( floor((( a.ip_address + 2147483647 ) - (( floor(( a.ip_address + 2147483647 ) / 16777216 )) * 16777216 )) / 65536 )) * 65536 )) / 256 )      || '.' ||
               floor((((( a.ip_address + 2147483647 ) - ((floor(( a.ip_address + 2147483647 ) / 16777216)) * 16777216 )) - (( floor((( a.ip_address + 2147483647 ) - (( floor(( a.ip_address + 2147483647 ) / 16777216 )) * 16777216 )) / 65536 )) * 65536 )) - (( floor(((( a.ip_address + 2147483647 ) - (( floor(( a.ip_address + 2147483647 ) / 16777216 )) * 16777216 )) - (( floor((( a.ip_address + 2147483647 ) - (( floor(( a.ip_address + 2147483647 ) / 16777216 )) * 16777216 )) / 65536 )) * 65536 )) / 256 )) * 256 )) / 1 )
                                                                       visitor_ip_address      ,
               a.referrer_url                                          http_referrer           ,
               case 
                 when c.website_id      in ( 5696, 40060 ) and 
                      b.affiliate_id    <> '104000'        and 
                      b.affiliate_type   = 'PARTNER'       then
                        TRUE
                 else
                        FALSE 
               end                                                     banner_program         
          from cccom_dw.fact_clicks                                   a
               join cccom_dw.dim_affiliates                           b on b.affiliate_key     = a.affiliate_key      
               join cccom_dw.dim_websites                             c on c.website_key       = a.website_key
               join cccom_dw.dim_products                             d on d.product_key       = a.product_key
               join cccom_dw.dim_product_types                        e on e.product_type_key  = d.product_type_key
               join cccom_dw.dim_merchants                            f on f.merchant_key      = d.merchant_key
               left outer join cccom_dw.dim_pages                     g on g.page_key          = a.exit_page_key
         where a.click_date >= add_months(date_trunc('month', current_date),-18)                                                        
    );
    
    
    
    
    
    -- trunate the table 
truncate table cccom_dw.fact_arp_transaction_applications;

-- populate the table 
INSERT INTO cccom_dw.fact_arp_transaction_applications
      (
              application_id,
              click_id,
              click_date,
              process_date,
              num_click_date,
              date_year,
              date_mon,
              date_week,
              date_dayofyear,
              affiliate_id,
              affiliate_name,
              website_id,
              website_url,
              card_id,
              card_title,
              product_type_id,
              product_type_name,
              merchant_id,
              merchant_name,
              page_id,
              page_name,
              user_variable
      )
      (
        select a.application_id                                        application_id          ,
               a.click_id                                              click_id                ,
               b.click_date                                            click_date              ,
               a.submission_date                                       process_date            ,
               to_number(to_char(b.click_date,'YYYYMMDD'),'99999999')  num_click_date          ,
               date_part(year     , b.click_date)                      date_year               , 
               date_part(mon      , b.click_date)                      date_mon                , 
               date_part(week     , b.click_date)                      date_week               ,
               date_part(dayofyear, b.click_date)                      date_dayofyear          ,
               c.affiliate_id                                          affiliate_id            , 
               c.company_name                                          affiliate_name          ,
               d.website_id                                            website_id              ,
               d.url                                                   website_url             ,
               e.card_id                                               card_id                 ,
               e.card_title                                            card_title              ,
               h.product_type_id                                       product_type_id         ,
               h.product_type_name                                     product_type_name       ,
               f.merchant_id                                           merchant_id             ,
               f.merchant_name                                         merchant_name           ,
               g.page_id                                               page_id                 ,
               g.page_name                                             page_name               ,
               b.user_variable                                         user_variable           
          from cccom_dw.fact_applications                             a
               join cccom_dw.fact_clicks                              b on b.click_id          = a.click_id
               join cccom_dw.dim_affiliates                           c on c.affiliate_key     = b.affiliate_key      
               join cccom_dw.dim_websites                             d on d.website_key       = b.website_key
               join cccom_dw.dim_products                             e on e.product_key       = b.product_key
               join cccom_dw.dim_merchants                            f on f.merchant_key      = e.merchant_key
               join cccom_dw.dim_product_types                        h on h.product_type_key  = e.product_type_key
               left outer join cccom_dw.dim_pages                     g on g.page_key          = b.exit_page_key
         where b.click_date >= add_months(date_trunc('month', current_date),-18)                                                          
      );
         
         
         
         
         
 -- truncate the table 
truncate table cccom_dw.fact_arp_transaction_sales;

-- populate tha table 
INSERT INTO cccom_dw.fact_arp_transaction_sales
    (
          sale_id,
          click_id,
          click_date,
          process_date,
          num_click_date,
          date_year,
          date_mon,
          date_week,
          date_dayofyear,
          affiliate_id,
          affiliate_name,
          website_id,
          website_url,
          card_id,
          card_title,
          merchant_id,
          merchant_name,
          user_variable,
          page_id,
          page_name,
          product_type_id,
          product_type_name,
          payout_status,
          payout_date,
          commission_amt,
          adjustment_amt,        
          banner_program
    )
    (
        select a.trans_id                                                              sale_id                 ,
               a.click_id                                                              click_id                ,
               b.click_date                                                            click_date              ,
               a.provider_process_date                                                 process_date            ,
               to_number(to_char(a.provider_process_date,'YYYYMMDD'),'99999999')       num_proces_date         ,
               date_part(year     , a.provider_process_date)                           date_year               , 
               date_part(mon      , a.provider_process_date)                           date_mon                , 
               date_part(week     , a.provider_process_date)                           date_week               ,
               date_part(dayofyear, a.provider_process_date)                           date_dayofyear          ,
               c.affiliate_id                                                          affiliate_id            , 
               c.company_name                                                          affiliate_name          ,
               d.website_id                                                            website_id              ,
               d.url                                                                   website_url             ,
               e.card_id                                                               card_id                 ,
               e.card_title                                                            card_title              ,
               f.merchant_id                                                           merchant_id             ,
               f.merchant_name                                                         merchant_name           ,
               b.user_variable                                                         user_variable           ,
               g.page_id                                                               page_id                 ,
               g.page_name                                                             page_name               ,
               h.product_type_id                                                       product_type_id         ,
               h.product_type_name                                                     product_type_name       ,
               a.payout_status                                                         payout_status           ,
               trunc(a.payout_date)                                                    payout_date             ,
               a.commission                                                            commission_amount       ,
               a.adjustment_amt                                                        adjustment_amt          ,
               case 
                 when d.website_id      in ( 5696, 40060 ) and 
                      c.affiliate_id    <> '104000'        and 
                      c.affiliate_type   = 'PARTNER'       then
                        TRUE
                 else
                        FALSE
               end                                                                     banner_program         
          from cccom_dw.fact_sales                                    a
               join cccom_dw.fact_clicks                              b on b.click_id          = a.click_id
               join cccom_dw.dim_affiliates                           c on c.affiliate_key     = b.affiliate_key      
               join cccom_dw.dim_websites                             d on d.website_key       = b.website_key
               join cccom_dw.dim_products                             e on e.product_key       = b.product_key
               join cccom_dw.dim_merchants                            f on f.merchant_key      = e.merchant_key
               left outer join cccom_dw.dim_pages                     g on g.page_key          = b.exit_page_key
               join cccom_dw.dim_product_types                        h on h.product_type_key  = e.product_type_key               
         where a.provider_process_date >= add_months(date_trunc('month', current_date),-18)
           and a.trans_type IN ( 4,5,6,105,106,107 )       
     );
        
        
        



-- query to get   tot_click_count
  create temp table temp1_perf_total_click_count as
           select a.affiliate_id                                            affiliate_id                , 
                  a.affiliate_name                                          affiliate_name              ,
                  a.website_id                                              website_id                  ,
                  a.website_url                                             website_url                 ,
                  a.card_id                                                 card_id                     ,
                  a.card_title                                              card_title                  ,
                  a.product_type_id                                         card_type_id                ,
                  a.product_type_name                                       card_type_name              ,
                  a.merchant_id                                             merchant_id                 ,
                  a.merchant_name                                           merchant_name               ,
                  a.page_id                                                 category_id                 ,
                  a.page_name                                               category_name               ,
                  a.user_variable                                           user_variable               ,
                  trunc(a.click_date)                                       info_date                   ,
                  count(*)                                                  tot_click_count             
             from cccom_dw.fact_arp_transaction_clicks                   a
            where 
                  a.click_date >= add_months(date_trunc('month', current_date),-18)  
         group by a.affiliate_id                                         ,
                  a.affiliate_name                                       ,
                  a.website_id                                           ,
                  a.website_url                                          ,
                  a.card_id                                              ,
                  a.card_title                                           ,
                  a.product_type_id                                      ,
                  a.product_type_name                                    ,
                  a.merchant_id                                          ,
                  a.merchant_name                                        ,
                  a.page_id                                              ,
                  a.page_name                                            ,
                  a.user_variable                                        ,
                  trunc(a.click_date)                                    ;
                  

  -- query to get   tot_application_count_cd
  create temp table temp1_perf_total_application_count_cd as
           select a.affiliate_id                                            affiliate_id                , 
                  a.affiliate_name                                          affiliate_name              ,
                  a.website_id                                              website_id                  ,
                  a.website_url                                             website_url                 ,
                  a.card_id                                                 card_id                     ,
                  a.card_title                                              card_title                  ,
                  a.product_type_id                                         card_type_id                ,
                  a.product_type_name                                       card_type_name              ,
                  a.merchant_id                                             merchant_id                 ,
                  a.merchant_name                                           merchant_name               ,
                  a.page_id                                                 category_id                 ,
                  a.page_name                                               category_name               ,
                  a.user_variable                                           user_variable               ,
                  trunc(a.click_date)                                       info_date                   ,
                  count(*)                                                  tot_application_count_cd      
             from cccom_dw.fact_arp_transaction_applications             a
            where 
                  a.click_date >= add_months(date_trunc('month', current_date),-18)  
         group by a.affiliate_id                                         ,
                  a.affiliate_name                                       ,
                  a.website_id                                           ,
                  a.website_url                                          ,
                  a.card_id                                              ,
                  a.card_title                                           ,
                  a.product_type_id                                      ,
                  a.product_type_name                                    ,
                  a.merchant_id                                          ,
                  a.merchant_name                                        ,
                  a.page_id                                              ,
                  a.page_name                                            ,
                  a.user_variable                                        ,
                  trunc(a.click_date)                                    ;
                  
-- select count(*) from temp1_perf_total_application_count_cd ;

-- query to get   tot_application_count_pd 
  create temp table temp1_perf_total_application_count_pd as 
           select a.affiliate_id                                            affiliate_id                , 
                  a.affiliate_name                                          affiliate_name              ,
                  a.website_id                                              website_id                  ,
                  a.website_url                                             website_url                 ,
                  a.card_id                                                 card_id                     ,
                  a.card_title                                              card_title                  ,
                  a.product_type_id                                         card_type_id                ,
                  a.product_type_name                                       card_type_name              ,
                  a.merchant_id                                             merchant_id                 ,
                  a.merchant_name                                           merchant_name               ,
                  a.page_id                                                 category_id                 ,
                  a.page_name                                               category_name               ,
                  a.user_variable                                           user_variable               ,
                  trunc(a.process_date)                                     info_date                   ,
                  count(*)                                                  tot_application_count_pd      
             from cccom_dw.fact_arp_transaction_applications             a
            where 
                  a.click_date >= add_months(date_trunc('month', current_date),-18)  
         group by a.affiliate_id                                         ,
                  a.affiliate_name                                       ,
                  a.website_id                                           ,
                  a.website_url                                          ,
                  a.card_id                                              ,
                  a.card_title                                           ,
                  a.product_type_id                                      ,
                  a.product_type_name                                    ,
                  a.merchant_id                                          ,
                  a.merchant_name                                        ,
                  a.page_id                                              ,
                  a.page_name                                            ,
                  a.user_variable                                        ,
                  trunc(a.process_date)                                  ;
                  
-- select count(*) from temp1_perf_total_application_count_pd;

-- query to get   tot_sale_count_by_cd, tot_sale_amt_by_cd , tot_adj_amt_by_cd 
  create temp table temp1_perf_total_sale_multi_detail_cd as
           select a.affiliate_id                                            affiliate_id                , 
                  a.affiliate_name                                          affiliate_name              ,
                  a.website_id                                              website_id                  ,
                  a.website_url                                             website_url                 ,
                  a.card_id                                                 card_id                     ,
                  a.card_title                                              card_title                  ,
                  a.product_type_id                                         card_type_id                ,
                  a.product_type_name                                       card_type_name              ,
                  a.merchant_id                                             merchant_id                 ,
                  a.merchant_name                                           merchant_name               ,
                  a.page_id                                                 category_id                 ,
                  a.page_name                                               category_name               ,
                  a.user_variable                                           user_variable               ,
                  trunc(a.click_date)                                       info_date                   ,
                  count(*)                                                  tot_sale_count_by_cd        ,
                  sum(a.commission_amt)                                     tot_sale_amt_by_cd          ,
                  sum(a.adjustment_amt)                                     tot_adj_amt_by_cd     
             from cccom_dw.fact_arp_transaction_sales                    a
            where 
                  a.click_date >= add_months(date_trunc('month', current_date),-18)  
         group by a.affiliate_id                                         ,
                  a.affiliate_name                                       ,
                  a.website_id                                           ,
                  a.website_url                                          ,
                  a.card_id                                              ,
                  a.card_title                                           ,
                  a.product_type_id                                      ,
                  a.product_type_name                                    ,
                  a.merchant_id                                          ,
                  a.merchant_name                                        ,
                  a.page_id                                              ,
                  a.page_name                                            ,
                  a.user_variable                                        ,
                  trunc(a.click_date)                                    ;
                  
-- select count(*) from temp1_perf_total_sale_multi_detail_cd;

-- query to get   tot_sale_count_by_pd, tot_sale_amt_by_pd , tot_adj_amt_by_pd 
  create temp table temp1_perf_total_sale_multi_detail_pd as
           select a.affiliate_id                                            affiliate_id                , 
                  a.affiliate_name                                          affiliate_name              ,
                  a.website_id                                              website_id                  ,
                  a.website_url                                             website_url                 ,
                  a.card_id                                                 card_id                     ,
                  a.card_title                                              card_title                  ,
                  a.product_type_id                                         card_type_id                ,
                  a.product_type_name                                       card_type_name              ,
                  a.merchant_id                                             merchant_id                 ,
                  a.merchant_name                                           merchant_name               ,
                  a.page_id                                                 category_id                 ,
                  a.page_name                                               category_name               ,
                  a.user_variable                                           user_variable               ,
                  trunc(a.process_date)                                     info_date                   ,
                  count(*)                                                  tot_sale_count_by_pd        ,
                  sum(a.commission_amt)                                     tot_sale_amt_by_pd          ,
                  sum(a.adjustment_amt)                                     tot_adj_amt_by_pd     
             from cccom_dw.fact_arp_transaction_sales                    a
            where 
                  a.process_date >= add_months(date_trunc('month', current_date),-18)  
         group by a.affiliate_id                                         ,
                  a.affiliate_name                                       ,
                  a.website_id                                           ,
                  a.website_url                                          ,
                  a.card_id                                              ,
                  a.card_title                                           ,
                  a.product_type_id                                      ,
                  a.product_type_name                                    ,
                  a.merchant_id                                          ,
                  a.merchant_name                                        ,
                  a.page_id                                              ,
                  a.page_name                                            ,
                  a.user_variable                                        ,
                  trunc(a.process_date)                                  ;
                  
-- select count(*) from temp1_perf_total_sale_multi_detail_pd;

-- query to create temp table for populating the permanent table .this is being done since there may be some
-- settings in Finserv prod instane that query size must be under 3000 char.



CREATE temp TABLE temp1_perm_performance_tab1
       DISTSTYLE even
       compound sortkey(affiliate_id,website_id,card_id,card_type_id,merchant_id,category_id,info_date,user_variable) as       
       (
          select    coalesce(coalesce(a.affiliate_id,b.affiliate_id),c.affiliate_id)       affiliate_id,
                    coalesce(coalesce(a.affiliate_name,b.affiliate_name),c.affiliate_name) affiliate_name,
                    coalesce(coalesce(a.website_id,b.website_id),c.website_id)             website_id,
                    coalesce(coalesce(a.website_url,b.website_url),c.website_url)          website_url,
                    coalesce(coalesce(a.card_id,b.card_id),c.card_id)                      card_id,
                    coalesce(coalesce(a.card_title,b.card_title),c.card_title)             card_title,
                    coalesce(coalesce(a.card_type_id,b.card_type_id),c.card_type_id)       card_type_id,
                    coalesce(coalesce(a.card_type_name,b.card_type_name),c.card_type_name) card_type_name,
                    coalesce(coalesce(a.merchant_id,b.merchant_id),c.merchant_id)          merchant_id,
                    coalesce(coalesce(a.merchant_name,b.merchant_name),c.merchant_name)    merchant_name,
                    coalesce(coalesce(a.category_id,b.category_id),c.category_id)          category_id,
                    coalesce(coalesce(a.category_name,b.category_name),c.category_name)    category_name,
                    coalesce(coalesce(a.user_variable,b.user_variable),c.user_variable)    user_variable,
                    coalesce(coalesce(a.info_date,b.info_date),c.info_date)                info_date,
                    nvl(a.tot_click_count,0)                                               tot_click_count,
                    nvl(b.tot_application_count_cd,0)                                      tot_application_count_cd,
                    nvl(c.tot_application_count_pd,0)                                      tot_application_count_pd
             from   temp1_perf_total_click_count             a 
                  full outer join temp1_perf_total_application_count_cd    b    on  b.affiliate_id  = a.affiliate_id 
                                                                    and  b.website_id    = a.website_id
                                                                    and  b.card_id       = a.card_id 
                                                                    and  b.card_type_id  = a.card_type_id
                                                                    and  b.merchant_id   = a.merchant_id
                                                                    and  b.category_id   = a.category_id
                                                                    and  b.user_variable = a.user_variable
                                                                    and  b.info_date     = a.info_date
                  full outer join temp1_perf_total_application_count_pd    c    on  c.affiliate_id  = a.affiliate_id 
                                                                    and  c.website_id    = a.website_id
                                                                    and  c.card_id       = a.card_id 
                                                                    and  c.card_type_id  = a.card_type_id
                                                                    and  c.merchant_id   = a.merchant_id
                                                                    and  c.category_id   = a.category_id
                                                                    and  c.user_variable = a.user_variable
                                                                    and  c.info_date     = a.info_date
        );

                                                                    

create temp table temp1_perm_performance_tab2
       diststyle even
       compound sortkey(affiliate_id,website_id,card_id,card_type_id,merchant_id,category_id,info_date,user_variable) as
       (
          select    coalesce(coalesce(a.affiliate_id,d.affiliate_id), e.affiliate_id)        affiliate_id,
                    coalesce(coalesce(a.affiliate_name, d.affiliate_name), e.affiliate_name) affiliate_name,
                    coalesce(coalesce(a.website_id, d.website_id), e.website_id)             website_id,
                    coalesce(coalesce(a.website_url, d.website_url),e.website_url)           website_url,
                    coalesce(coalesce(a.card_id, d.card_id), e.card_id)                      card_id,
                    coalesce(coalesce(a.card_title, d.card_title), e.card_title)             card_title,
                    coalesce(coalesce(a.card_type_id, d.card_type_id), e.card_type_id)       card_type_id,
                    coalesce(coalesce(a.card_type_name, d.card_type_name), e.card_type_name) card_type_name,
                    coalesce(coalesce(a.merchant_id,d.merchant_id), e.merchant_id)           merchant_id,
                    coalesce(coalesce(a.merchant_name, d.merchant_name), e.merchant_name)    merchant_name,
                    coalesce(coalesce(a.category_id, d.category_id), e.category_id)          category_id,
                    coalesce(coalesce(a.category_name, d.category_name), e.category_name)    category_name,
                    coalesce(coalesce(a.user_variable, d.user_variable), e.user_variable)    user_variable,
                    coalesce(coalesce(a.info_date, d.info_date), e.info_date)                info_date,
                    nvl(d.tot_sale_count_by_cd,0)                                            tot_sale_count_by_cd,
                    nvl(e.tot_sale_count_by_pd,0)                                            tot_sale_count_by_pd,
                    nvl(d.tot_sale_amt_by_cd,0)                                              tot_sale_amt_by_cd,
                    nvl(e.tot_sale_amt_by_pd,0)                                              tot_sale_amt_by_pd,
                    nvl(d.tot_adj_amt_by_cd,0)                                               tot_adj_amt_by_cd,
                    nvl(e.tot_adj_amt_by_pd,0)                                               tot_adj_amt_by_pd
             from   temp1_perf_total_click_count             a 
                  full outer join temp1_perf_total_sale_multi_detail_cd    d    on  d.affiliate_id  = a.affiliate_id 
                                                                    and  d.website_id    = a.website_id
                                                                    and  d.card_id       = a.card_id 
                                                                    and  d.card_type_id  = a.card_type_id
                                                                    and  d.merchant_id   = a.merchant_id
                                                                    and  d.category_id   = a.category_id
                                                                    and  d.user_variable = a.user_variable
                                                                    and  d.info_date     = a.info_date
                  full outer join temp1_perf_total_sale_multi_detail_pd    e    on  e.affiliate_id  = a.affiliate_id 
                                                                    and  e.website_id    = a.website_id
                                                                    and  e.card_id       = a.card_id 
                                                                    and  e.card_type_id  = a.card_type_id
                                                                    and  e.merchant_id   = a.merchant_id
                                                                    and  e.category_id   = a.category_id
                                                                    and  e.user_variable = a.user_variable
                                                                    and  e.info_date     = a.info_date
         );


truncate table cccom_dw.fact_arp_performance;

-- populate the final table 
  INSERT INTO cccom_dw.fact_arp_performance
       (
           affiliate_id             ,
           affiliate_name           ,
           website_id               ,
           website_url              ,
           card_id                  ,
           card_title               ,
           product_type_id          ,
           product_type_name        ,
           merchant_id              ,
           merchant_name            ,
           page_id                  ,
           page_name                ,
           user_variable            ,
           info_date                ,
           tot_click_count          ,
           tot_application_count_cd ,
           tot_application_count_pd ,
           tot_sale_count_by_cd     ,
           tot_sale_count_by_pd     ,
           tot_sale_amt_by_cd       ,
           tot_sale_amt_by_pd       ,
           tot_adj_amt_by_cd        ,
           tot_adj_amt_by_pd  
       )
       (
          select    coalesce(a.affiliate_id,b.affiliate_id)             affiliate_id,
                    coalesce(a.affiliate_name,b.affiliate_name)         affiliate_name,
                    coalesce(a.website_id,b.website_id)                 website_id,
                    coalesce(a.website_url,b.website_url)               website_url,
                    coalesce(a.card_id,b.card_id)                       card_id,
                    coalesce(a.card_title,b.card_title)                 card_title,
                    coalesce(a.card_type_id,b.card_type_id)             card_type_id,
                    coalesce(a.card_type_name,b.card_type_name)         card_type_name,
                    coalesce(a.merchant_id,b.merchant_id)               merchant_id,
                    coalesce(a.merchant_name,b.merchant_name)           merchant_name,
                    coalesce(a.category_id,b.category_id)               category_id,
                    coalesce(a.category_name,b.category_name)           category_name,
                    coalesce(a.user_variable,b.user_variable)           user_variable,
                    coalesce(a.info_date,b.info_date)                   info_date,
                    nvl(a.tot_click_count,0)                            tot_click_count,
                    nvl(a.tot_application_count_cd,0)                   tot_application_count_cd,
                    nvl(a.tot_application_count_pd,0)                   tot_application_count_pd,
                    nvl(b.tot_sale_count_by_cd,0)                       tot_sale_count_by_cd,
                    nvl(b.tot_sale_count_by_pd,0)                       tot_sale_count_by_pd,
                    nvl(b.tot_sale_amt_by_cd,0)                         tot_sale_amt_by_cd,
                    nvl(b.tot_sale_amt_by_pd,0)                         tot_sale_amt_by_pd,
                    nvl(b.tot_adj_amt_by_cd,0)                          tot_adj_amt_by_cd,
                    nvl(b.tot_adj_amt_by_pd,0)                          tot_adj_amt_by_pd
             from      temp1_perm_performance_tab1             a 
                  full outer join temp1_perm_performance_tab2             b     on  b.affiliate_id  = a.affiliate_id 
                                                                    and  b.website_id    = a.website_id
                                                                    and  b.card_id       = a.card_id 
                                                                    and  b.card_type_id  = a.card_type_id
                                                                    and  b.merchant_id   = a.merchant_id
                                                                    and  b.category_id   = a.category_id
                                                                    and  b.info_date     = a.info_date
                                                                    and  b.user_variable = a.user_variable
       );
   





-- query to get   tot_click_count
  create temp table temp1_ovrvw_total_click_count as
           select a.affiliate_id                                            affiliate_id                , 
                  a.affiliate_name                                          affiliate_name              ,
                  a.website_id                                              website_id                  ,
                  a.website_url                                             website_url                 ,
                  trunc(a.click_date)                                       info_date                   ,
                  count(*)                                                  tot_click_count             
             from cccom_dw.fact_arp_transaction_clicks                   a
            where a.click_date >= add_months(date_trunc('month', current_date),-18)                                                     
         group by a.affiliate_id                                         ,
                  a.affiliate_name                                       ,
                  a.website_id                                           ,
                  a.website_url                                          ,
                  trunc(a.click_date)                                    ;

-- select count(*) from temp1_ovrvw_total_click_count;

-- query to get   tot_sale_count_by_cd, tot_sale_amt_by_cd , tot_adj_amt_by_cd 
  create temp table temp1_ovrvw_total_sale_multi_detail_cd as
           select a.affiliate_id                                            affiliate_id                , 
                  a.affiliate_name                                          affiliate_name              ,
                  a.website_id                                              website_id                  ,
                  a.website_url                                             website_url                 ,
                  trunc(a.click_date)                                       info_date                   ,
                  count(*)                                                  tot_sale_count_by_cd        ,
                  sum(a.commission_amt)                                     tot_sale_amt_by_cd          ,
                  sum(a.adjustment_amt)                                     tot_adj_amt_by_cd     
             from cccom_dw.fact_arp_transaction_sales                    a
            where a.click_date  >= add_months(date_trunc('month', current_date),-18)                                                      
         group by a.affiliate_id                                         ,
                  a.affiliate_name                                       ,
                  a.website_id                                           ,
                  a.website_url                                          ,
                  trunc(a.click_date)                                    ;
                  
-- select count(*) from temp1_ovrvw_total_sale_multi_detail_cd;

-- query to get   tot_sale_count_by_pd, tot_sale_amt_by_pd , tot_adj_amt_by_pd 
  create temp table temp1_ovrvw_total_sale_multi_detail_pd as
           select a.affiliate_id                                            affiliate_id                , 
                  a.affiliate_name                                          affiliate_name              ,
                  a.website_id                                              website_id                  ,
                  a.website_url                                             website_url                 ,
                  trunc(a.process_date)                                     info_date                   ,
                  count(*)                                                  tot_sale_count_by_pd        ,
                  sum(a.commission_amt)                                     tot_sale_amt_by_pd          ,
                  sum(a.adjustment_amt)                                     tot_adj_amt_by_pd     
             from cccom_dw.fact_arp_transaction_sales                    a
            where a.process_date  >= add_months(date_trunc('month', current_date),-18)                                                     
         group by a.affiliate_id                                         ,
                  a.affiliate_name                                       ,
                  a.website_id                                           ,
                  a.website_url                                          ,
                  trunc(a.process_date)                                  ;
                  
-- select count(*) from temp1_ovrvw_total_sale_multi_detail_pd;

truncate table cccom_dw.fact_arp_overview;

-- populate the table
INSERT INTO cccom_dw.fact_arp_overview
     (
         affiliate_id         ,
         affiliate_name       ,
         website_id           ,
         website_url          ,
         info_date            ,
         tot_click_count      ,
         tot_sale_count_by_cd ,
         tot_sale_count_by_pd ,
         tot_sale_amt_by_cd   ,
         tot_sale_amt_by_pd   ,
         tot_adj_amt_by_cd    ,
         tot_adj_amt_by_pd  
     )
     (
          select  coalesce(coalesce(a.affiliate_id,d.affiliate_id), e.affiliate_id)          affiliate_id,   
                  coalesce(coalesce(a.affiliate_name, d.affiliate_name), e.affiliate_name)   affiliate_name,   
                  coalesce(coalesce(a.website_id, d.website_id), e.website_id)               website_id,   
                  coalesce(coalesce(a.website_url, d.website_url),e.website_url)             website_url,     
                  coalesce(coalesce(a.info_date ,  d.info_date ), e.info_date )              info_date,
                  nvl(a.tot_click_count,0)   tot_click_count           ,
                  nvl(d.tot_sale_count_by_cd,0)   tot_sale_count_by_cd      ,
                  nvl(e.tot_sale_count_by_pd,0)   tot_sale_count_by_pd      ,
                  nvl(d.tot_sale_amt_by_cd,0)   tot_sale_amt_by_cd        ,
                  nvl(e.tot_sale_amt_by_pd, 0)   tot_sale_amt_by_pd        ,
                  nvl(d.tot_adj_amt_by_cd , 0)   tot_adj_amt_by_cd        ,
                  nvl(e.tot_adj_amt_by_pd  , 0) tot_adj_amt_by_pd
             from      temp1_ovrvw_total_click_count             a 
                  full outer join temp1_ovrvw_total_sale_multi_detail_cd    d    on  d.affiliate_id = a.affiliate_id 
                                                                     and  d.website_id   = a.website_id
                                                                     and  d.info_date    = a.info_date
                  full outer join temp1_ovrvw_total_sale_multi_detail_pd    e    on  e.affiliate_id = a.affiliate_id 
                                                                     and  e.website_id   = a.website_id
                                                                     and  e.info_date    = a.info_date
     );


