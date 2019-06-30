CREATE OR REPLACE LUA SCRIPT etl.dawa_import(entity, target_schema_name, target_table_name) RETURNS TABLE AS

    import('etl.query_wrapper','qw')
	import('etl.scd2_wrapper','sw')

    wrapper = qw.new('etl.job_log', 'etl.job_details', target_schema_name..'.'..target_table_name)



    suc, res = wrapper:query([[
            select REST_SERVICES.DAWA_TXID('');
    ]])


     -- If table contains date value, create a custom log message
    if not suc then
        wrapper:log('ERROR', target_schema_name .. '.' .. target_table_name .. ' could not load TXID from dawa')
    end

	if not res[1][1] then
        wrapper:log('ERROR', target_schema_name .. '.' .. target_table_name .. ' retrieved a empty txid')
    end

	if suc then
		wrapper:set_param('TXID', res[1][1])
		suc1, res1 = wrapper:query([[
            select ETL_UDFS.DAWA_POSTNUMRE(0, :TXID);
    	]])
	end


    return wrapper:finish()
/

EXECUTE SCRIPT etl.load_dawa_postnummer();




