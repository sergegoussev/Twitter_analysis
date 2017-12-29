CREATE DEFINER=`root`@`localhost` PROCEDURE `'__specify_db_name__'_summary_procedure`()
/*This creates the summary procedure. It should be scheduled to run at some regular 
interval - like every 1 hour*/
BEGIN
	UPDATE sample_summary SET num_tweets=(SELECT 
												COUNT(tmast.twtid)
											FROM 
											twt_master tmast
											INNER JOIN sample_twts_stream1 tstr
											ON tmast.twtid=tstr.twtid) WHERE id=1;
	UPDATE sample_summary SET num_users=(SELECT 
												COUNT(*) 
											FROM sample_users_stream1) WHERE id=1;
	UPDATE sample_summary SET num_rts=(SELECT 
											COUNT(trt.twtid)
										FROM 
										twt_rtmaster trt
										INNER JOIN sample_twts_stream1 tstr
										ON trt.twtid=tstr.twtid) WHERE id=1;
	UPDATE sample_summary SET date_end=(SELECT 
											MAX(twt_createdat) 
										FROM twt_master tmast
											INNER JOIN sample_twts_stream1 tstr
											ON tmast.twtid=tstr.twtid) WHERE id=1;
END