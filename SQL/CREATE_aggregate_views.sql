/***
Create_aggeregate_views.sql v2.0, release data 2017-12-27
@SergeGoussev
---------------
This view create script works with Create_twitter_schema.sql v2.xa

Only input is the name of the database

NOTE: view acronyms:
-VA - view aggreated -- used for presenting data as OLTP (limited, if need in full, use scheduled AN tables created through procedures)
-VD - view administrative -- used for reporting, dashboard analysis (or on web-app)
-VDS - view administrative supporting -- these are the ones that present info used by other admin views
***/
USE ON_politics;

/*----------aggregated views---------------*/
/*First create the view of aggegated tweet and user info data -- OLAP overview of tweets
NOTE: hashtags, urls, and conversation data is excluded for this first one
*/
DROP VIEW VG_Tweets;

CREATE VIEW VG_Tweets AS
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
	ON tt.twtid=tn.twtid
	
	LEFT JOIN users_master tu
	ON tt.userid=tu.userid
	
	LEFT JOIN users_geo tg
	ON tt.userid=tg.userid
	
	LEFT JOIN users_numbers tun
	ON tt.userid=tun.userid;


/*view of teh most retweeted tweets captured within the whole db
NOTE: This acts as a way to tell which rt trees can be analyzed  */
DROP VIEW VG_Most_rted_twts;

CREATE VIEW VG_Most_rted_twts AS
SELECT 
	rttwtid AS twtid,
	username,
	twttext,
	COUNT(twt_rtmaster.twtid) AS num_times_rted_in_sample
FROM twt_rtmaster
	INNER JOIN twt_master
	ON twt_rtmaster.rttwtid=twt_master.twtid
	
	INNER JOIN users_master
	ON twt_master.userid=users_master.userid
    
GROUP BY rttwtid
ORDER BY num_times_rted_in_sample DESC;

/*view to aggregate all quotes data */
DROP VIEW VG_quotes;

CREATE VIEW VG_quotes AS
SELECT
	tm1.twtid,
	tm1.userid,
	um1.username,
	tm1.twttext,
	tm1.twt_lang,
	tm1.twt_createdat,
	tm2.twtid AS quoted_twtid,
	tm2.userid AS quoted_userid,
	um2.username AS quoted_username,
	tm2.twttext AS quoted_twttext,
	tm2.twt_lang AS quoted_lang,
	tm2.twt_createdat AS quoted_twt_createdat
FROM twt_qtmaster qt
	INNER JOIN twt_master tm1
	ON qt.twtid=tm1.twtid
	
	LEFT JOIN users_master um1
	ON tm1.userid=um1.userid
	
	INNER JOIN twt_master tm2
	ON qt.qttwtid=tm2.twtid
	
	LEFT JOIN users_master um2
	ON tm2.userid=um2.userid;


/*----------administrative views---------------*/
/*Create a view to determine the number of tweets captured per day since sample start
*/
DROP VIEW VD_TotalDailyRate;

CREATE VIEW VD_TotalDailyRate AS
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

/*this view creates a view that is constantly updated with today's twtids
NOTE: investigate later, probably not the most 
*/
DROP VIEW VDS_todays_twtids;

CREATE VIEW VDS_todays_twtids AS
SELECT
	tm.twtid
FROM twt_master tm

WHERE DATE(tm.twt_createdat) = CURDATE();

/*As above, constantly updated list of userids and twtids of today's mentions
NOTE: its not dependent on the above but similar. Called meta as its still metadata
*/
DROP VIEW VDS_todays_mention_meta;

CREATE VIEW VDS_todays_mention_meta AS
SELECT
	mentions.userid,
	todays_mentions.twtid AS twtid
FROM (
	SELECT
		tm.twtid
	FROM twt_master tm
	WHERE DATE(tm.twt_createdat) = CURDATE()
	) AS todays_mentions

	INNER JOIN meta_mentionsmaster mentions
	ON todays_mentions.twtid=mentions.twtid;
    
/*--View to describe which users are the most mentioned today--
NOTE: this view is dependent on VADG vies to function, must be created consecutievely*/
DROP VIEW VD_todays_top_mentioned_users;

CREATE VIEW VD_todays_top_mentioned_users AS
SELECT
	users.username,
	users.givenname,
	COUNT(mentions.twtid)/(
							SELECT 
								COUNT(*) 
							FROM VDS_todays_mention_meta
							)*100 AS mention_proportions
FROM VDS_todays_mention_meta mentions
	
    INNER JOIN users_master users
    ON users.userid=mentions.userid
    
GROUP BY users.username 
ORDER BY mention_proportions DESC

LIMIT 10;

