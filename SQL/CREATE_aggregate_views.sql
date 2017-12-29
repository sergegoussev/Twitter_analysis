/***
Create_aggeregate_views.sql v2.0, release data 2017-12-27
@SergeGoussev
---------------
This view create script works with Create_twitter_schema.sql v2.xa

Only input is the name of the database

NOTE: view acronyms:
-VAG - view aggreated
-VAD - view administrative

***/
USE `__specify_db_name__`;

/*----------aggregated views---------------*/
/*First create the view of aggegated tweet and user info data -- OLAP overview of tweets
NOTE: hashtags, urls, and conversation data is excluded for this first one
*/
DROP VIEW VAG_Tweets;
CREATE VIEW VAG_Tweets AS
SELECT 
	tt.twtid,
	tt.userid AS user_id,
    tu.username AS user_handle,
    tu.givenname AS user_name,
    tu.user_lang,
    tg.geo AS user_geo,
    tun.numfriends AS user_numfriends,
	tun.numfollowers user_numfollowers,
	tun.time_stamp AS userinfo_last_updated,
	tt.twttext,
	tt.twt_createdat,
	tt.twt_lang,
    tn.favcount,
	tn.rtcount
FROM twt_master tt
	INNER JOIN twt_numbers tn
ON tt.twtid=tt.twtid
	LEFT JOIN users_master tu
ON tt.userid=tu.userid
	LEFT JOIN users_geo tg
ON tt.userid=tg.userid
	LEFT JOIN users_numbers tun
ON tt.userid=tun.userid;

/*----------administrative views---------------*/
/*Create a view to determine the number of tweets captured per day since sample start
*/
DROP VIEW VAD_TotalDailyRate;
CREATE VIEW VAD_TotalDailyRate AS
SELECT
	DATE(twt_createdat) AS dat,
    COUNT(twtid)
FROM (
	SELECT 
		twt_createdat,
		twtid
	FROM twt_master
	UNION 
	SELECT 
		rt_createdat,
		twtid 
	FROM twt_rtmaster
	) AS twts_union
WHERE DATE(twt_createdat) > (
							SELECT 
								DATE(date_start)
							FROM sample_summary
								WHERE id=1
							)
GROUP BY dat;