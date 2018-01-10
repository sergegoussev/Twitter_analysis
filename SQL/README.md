<h5>Summary of SQL scripts</h5>

<p>Based on MySQL 5.7. Designed to be run on localhost, modify as needed.</p>

<h6>RUN in the following order:</h6>
<ol>
	<li>Create_twitter_schema.sql
		<ul><li>Creates the database and schema in the MySQL server environment</li></ul>
        </li>
	<li>Create_aggeregate_views.sql
		<ul><li>Create the neccessary views to support the database for analysis</li></ul>
	</li>
	<li>Streaming_summary_procedure.sql
		<ul><li>This creates a stored procedure to provide a summary of a specific sample</li></ul>
	</li>
	<li>Friendship_index_procedure.sql
		<ul><li>due to rate limits with the Twitter REST API to collect friends/followers of users, a quite limited range of friends can be collected. The friendship index script creates a new table that calculates an importance index -- friends of users at the top of this index will be downloaded first. This creates a more dynamic environment for analysis later.</li></ul>
	</li>
</ol>
