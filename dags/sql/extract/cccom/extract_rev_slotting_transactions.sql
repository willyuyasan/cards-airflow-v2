select
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
ifnull(t.exit_page,'') as exit_page_id,
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
sale_id
from cccomus.rev_slotting_transactions t
