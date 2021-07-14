select 1,
ifnull(transid,'') as transid,
ifnull(dateinserted,'') as dateinserted,
ifnull(transtype,'') as transtype,
ifnull(payoutstatus,'') as payoutstatus,
ifnull(datepayout,'') as datepayout,
ifnull(orderid,'') as orderid,
ifnull(bannerid,'') as bannerid,
ifnull(affiliateid,'') as affliateid,
ifnull(campcategoryid,'') as campcategoryid,
ifnull(parenttransid,'') as parenttransid,
ifnull(commission,'') as commission,
ifnull(ip,'') as ip,
ifnull(recurringcommid,'') as recurringcommid,
ifnull(data2,'') as data2,
ifnull(channel,'') as channel,
ifnull(episode,'') as episode,
ifnull(t.exit,'') as exit_page_id,
ifnull(page_position,'') as page_position,
#providerorderid,
#providertype,
ifnull(providereventdate,'') as providereventdate,
ifnull(providerprocessdate,'') as providerprocessdate,
ifnull(merchantname,'') as merchantname,
ifnull(providerid,'') as providerid,
ifnull(quantity,'') as quantity,
ifnull(providerchannel,'') as providerchannel,
ifnull(estimatedrevenue,'') as estimatedrevenue,
ifnull(dateestimated,'') as dateestimated,
ifnull(replace(providerstatus,'\r\n', ' '),'') as providerstatus,
ifnull(providercorrected,'') as providercorrected,
ifnull(providerwebsiteid,'') as providerwebsiteid,
ifnull(providerwebsitename,'') as providerwebsitename,
ifnull(provideractionid,'') as provideractionid,
ifnull(reftrans,'') as reftrans,
ifnull(dateadjusted,'') as dateadjusted,
#currref,
#prevref,
#thirdref,
ifnull(external_visit_id,'') as external_visit_id,
ifnull(refinceptiondate,'') as refinceptiondate,
ifnull(replace(refererurl, char(9), ' '),'') as referrerurl,
ifnull(replace(provideractionname,'\r\n', ' '),'') as provideractionname,
ifnull(productid,'') as productid,
ifnull(t.estimateddatafilename,'') as estimateddatafilename
from cccomus.transactions_recent t
where transtype != 1
and providerprocessdate >= (last_day(now()) + interval 1 day - interval 4 month)
and providerprocessdate < now()
#cutover logc: pull transactions data prior to 01-Dec-2018 from REX
# Make Good (trans_type = 102) / Pre-payments (trans_type = 120): datepayout < 01/01-2019 - from REX
and ((ifnull(t.transtype,'') not in (102,120)
    and date(ifnull(t.providerprocessdate,'2200-01-01')) < '2019-02-01')
  or (ifnull(t.transtype,'') in (102,120)
    and date(ifnull(t.datepayout,'2200-01-01')) < '2019-03-01'))
union all
select 2,
ifnull(transid,'') as transid,
ifnull(dateinserted,'') as dateinserted,
ifnull(transtype,'') as transtype,
ifnull(payoutstatus,'') as payoutstatus,
ifnull(datepayout,'') as datepayout,
ifnull(orderid,'') as orderid,
ifnull(bannerid,'') as bannerid,
ifnull(affiliateid,'') as affiliateid,
ifnull(campcategoryid,'') as campcategoryid,
ifnull(parenttransid,'') as parenttransid,
ifnull(commission,'') as commission,
ifnull(ip,'') as ip,
ifnull(recurringcommid,'') as recurringcommid,
ifnull(data2,'') as data2,
ifnull(channel,'') as channel,
ifnull(episode,'') as episode,
ifnull(t.exit,'') as exit_page_id,
ifnull(page_position,'') as page_position,
#providerorderid,
#providertype,
ifnull(providereventdate,'') as providereventdate,
ifnull(providerprocessdate,'') as providerprocessdate,
ifnull(merchantname,'') as merchantname,
ifnull(providerid,'') as providerid,
ifnull(quantity,'') as quantity,
ifnull(providerchannel,'') as providerchannel,
ifnull(estimatedrevenue,'') as estimatedrevenue,
ifnull(dateestimated,'') as dateestimated,
ifnull(replace(providerstatus,'\r\n', ' '),'') as providerstatus,
ifnull(providercorrected,'') as providercorrected,
ifnull(providerwebsiteid,'') as providerwebsiteid,
ifnull(providerwebsitename,'') as providerwebsitename,
ifnull(provideractionid,'') as provideractionid,
ifnull(reftrans,'') as reftrans,
ifnull(dateadjusted,'') as dateadjusted,
#currref,
#prevref,
#thirdref,
ifnull(external_visit_id,'') as external_visit_id,
ifnull(refinceptiondate,'') as refinceptiondate,
ifnull(replace(refererurl, char(9), ' '),'') as referrerurl,
ifnull(replace(provideractionname,'\r\n', ' '),'') as provideractionname,
ifnull(productid,'') as productid,
ifnull(estimateddatafilename,'') as estimateddatafilename
from cccomus.transactions_upload t
where providerprocessdate >= (last_day(now()) + interval 1 day - interval 4 month)
and providerprocessdate < now()
#cutover logc: pull transactions data prior to 01-Dec-2018 from REX
# Make Good (trans_type = 102) / Pre-payments (trans_type = 120): datepayout < 01/01-2019 - from REX
and ((ifnull(t.transtype,'') not in (102,120)
    and date(ifnull(t.providerprocessdate,'2200-01-01')) < '2019-02-01')
  or (ifnull(t.transtype,'') in (102,120)
    and date(ifnull(t.datepayout,'2200-01-01')) < '2019-03-01'));