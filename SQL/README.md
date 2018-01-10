<h5>Summary of SQL scripts</h5>

<p>Based on MySQL 5.7. Designed to be run on localhost, modify as needed.</p>

<h6>RUN in the following order:</h6>
1. Create_twitter_schema.sql
	- this creates the db schema 
2. Create_aggeregate_views.sql 
	- creates necessary views
3. Create teh summary procedure.sql
	- once created, use event scheduler to make a regular call, every hour is recommended
4. Friendship_index_procedure.sql
	- due to rate limits with the Twitter REST API to collect friends/followers of users, a quite limited range of friends can be collected. The friendship index script creates a new table that calculates an importance index -- friends of users at the top of this index will be downloaded first. This creates a more dynamic environment for analysis later.
