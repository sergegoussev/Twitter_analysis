CREATE DEFINER=`root`@`localhost` PROCEDURE `'__specify_db_name__'_friendship_index_calc`()
BEGIN
/**
This Stored Procedure will iterate as often as selected and create an index of importance for all users in the 
database 

The logic is that friendships will be collected for users based on three indicators:
1. Number of friendships (those with LOTS of friends are too resource intensive to get too often)
2. How active users are (based on number of tweets per week)--the more active, the more relevant recent frienship indicators
3. Length of time since downloaded--the longer, the more important to re-download
--
**/
	#first drop the old table with data 
	DROP TABLE IF EXISTS users_getFriendships_index;
    
    #(re)create the table from scratch
	CREATE TABLE users_getFriendships_index (
		userid VARCHAR(18),
		fr_index DECIMAL(7,6)
		);
		
    #now insert into the new table--the friendship index
	INSERT INTO 
		users_getfriendships_index (userid, fr_index)
	SELECT
		t_calc.userid,
		t_calc.friend_category*0.25 +
			t_calc.tweet_freq*0.25 + 
			t_calc.staleness*0.5 AS fr_index
	FROM (
		SELECT 
			tm.userid,
			tm.username,
			IF(tn.numfriends<1000,6,
				IF(tn.numfriends<5000,5,
					IF(tn.numfriends<15000,4,
						IF(tn.numfriends<40000,3,
							IF(tn.numfriends<100000,2,1)))))/6
						AS friend_category,
			IF(tt.num_tweets_per_week>=7,4,
				IF(tt.num_tweets_per_week>=5,3,
					IF(tt.num_tweets_per_week>=3,2,1)))/4 AS tweet_freq,
			IF(ttime.tstamp>4,5,
				IF(ttime.tstamp>3,4,
					IF(ttime.tstamp>2,3,
						IF(ttime.tstamp>1,2,1))))/5 AS staleness
		FROM users_master tm
			INNER JOIN users_numbers tn
		ON tm.userid=tn.userid
			INNER JOIN (
				SELECT 
					userid, 
					COUNT(twtid)/(
						SELECT 
							(week(NOW())-week(date_start)) AS num_tweets_since_start
						FROM on_politics.sample_summary WHERE id=1
						) AS num_tweets_per_week /*AS numtwtscaptured*/
				FROM on_politics.twt_master 
				GROUP BY userid
				) tt
		ON tt.userid=tm.userid
			LEFT JOIN (
				SELECT 
					userid,
					WEEK(NOW())-WEEK(MAX(time_stamp)) AS tstamp
				FROM on_politics.links_raw
					GROUP BY userid) ttime
		ON tt.userid=ttime.userid) t_calc
	ORDER BY fr_index DESC;
END