
begin transaction;

    delete from cccom_dw.fact_dummy_card_group
    where keycheck in (
      select t.keycheck from cccom_dw.stg_dummy_card_group t);

    insert into cccom_dw.fact_dummy_card_group
    ( keycheck,
	card_group_id ,
	name ,
	deleted ,
	parent_card_group_id ,
	parent_name ,
	parent_deleted )
    select
    keycheck,
	card_group_id ,
	name ,
	deleted ,
	parent_card_group_id ,
	parent_name ,
	parent_deleted
    from  cccom_dw.stg_dummy_card_group ssc
    where not deleted;

end transaction ;
