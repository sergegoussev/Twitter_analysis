/***
Create_twitter_schema.sql v2.2, release data 2017-10-29
@SergeGoussev
---------------

This script creates the main DB schema for Twitter Analysis: 
Input: 
 - Specify the name of the DB you want. 
***/

/*---NOTE: specify the of desired DB in the three DROP, CREATE and USE commands-----*/
/*--1.DB creation*/
DROP DATABASE `__specify_db_name__`;
CREATE DATABASE IF NOT EXISTS `__specify_db_name__`;
USE `__specify_db_name__`;

/*2. First create the table to specify all the events that will be captured by this DB. 
Note: Can be used to capture REST SEARCH and STREAMING samples*/
CREATE TABLE IF NOT EXISTS sample_summary(
	id MEDIUMINT AUTO_INCREMENT,
    sample_name VARCHAR(50) UNIQUE,
    keywords TEXT,
    date_start DATETIME DEFAULT NULL,
    date_end DATETIME DEFAULT NULL,
    sample_type TEXT,
    sample_modularity DECIMAL(8,7) DEFAULT NULL,
    done VARCHAR(1) DEFAULT NULL,
    num_tweets BIGINT(20) DEFAULT NULL,
    num_rts BIGINT(20) DEFAULT NULL,
    num_users BIGINT(20) DEFAULT NULL,
    PRIMARY KEY (id)
    );

/*3. Create all tables to do with the users and their data*/
CREATE TABLE IF NOT EXISTS users_Master (
	userid VARCHAR(18) PRIMARY KEY,
    username TEXT,
    givenname TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
    acc_createdat DATETIME,
    user_lang VARCHAR(7)
    );
CREATE TABLE IF NOT EXISTS users_Numbers (
	userid VARCHAR(18) PRIMARY KEY,
    numfriends INT,
    numfollowers BIGINT,
    time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
CREATE TABLE IF NOT EXISTS users_Geo (
	userid VARCHAR(18) PRIMARY KEY,
    geo TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
    );
CREATE TABLE IF NOT EXISTS users_Bio (
	userid VARCHAR(18) PRIMARY KEY,
    bio TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
    );
CREATE TABLE IF NOT EXISTS users_getFriendships_index (
	userid VARCHAR(18),
	fr_index DECIMAL(3,2)
	)
    
/*4. Create all tables having to do with tweets that can come in*/
CREATE TABLE IF NOT EXISTS twt_Master (
	twtid VARCHAR(18) PRIMARY KEY,
    userid VARCHAR(18),
    twttext TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
    twt_createdat DATETIME,
    twt_lang VARCHAR(7)
    );
CREATE TABLE IF NOT EXISTS twt_numbers (
	twtid VARCHAR(18) PRIMARY KEY,
    favcount INTEGER,
    rtcount INTEGER,
    time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
CREATE TABLE IF NOT EXISTS twt_rtMaster (
	twtid VARCHAR(18) PRIMARY KEY,
    userid VARCHAR(18),
    rt_createdat DATETIME,
    rttwtid VARCHAR(18)
    );
CREATE TABLE IF NOT EXISTS twt_qtMaster (
	twtid VARCHAR(18) PRIMARY KEY,
    qttwtid VARCHAR(18)
    );

/*5. Create tables for tweet metadata*/
CREATE TABLE IF NOT EXISTS meta_hashMaster2 (
	hashid INT AUTO_INCREMENT,
    hashtag TEXT,
	PRIMARY KEY (hashid)
    );
CREATE TABLE IF NOT EXISTS meta_hashMaster1 (
	twtid VARCHAR(18) NOT NULL,
    hashid INT,
	FOREIGN KEY (hashid) REFERENCES meta_hashMaster2(hashid)
    );
CREATE TABLE IF NOT EXISTS meta_urlMaster2 (
	urlid INT AUTO_INCREMENT,
    url TEXT,
	PRIMARY KEY (urlid)
    );
CREATE TABLE IF NOT EXISTS meta_urlMaster1 (
	twtid VARCHAR(18) NOT NULL,
    urlid INT,
	FOREIGN KEY (urlid) REFERENCES meta_urlMaster2(urlid)
    );
CREATE TABLE IF NOT EXISTS meta_twt_coordinates (
	twtid VARCHAR(18) PRIMARY KEY,
    geolat FLOAT(10,6),
    geolon FLOAT(10,6)
    );
CREATE TABLE IF NOT EXISTS meta_mentionsMaster (
	twtid VARCHAR(18),
    userid VARCHAR(18),
	PRIMARY KEY (twtid, userid)
    );
CREATE TABLE IF NOT EXISTS meta_repliesMaster (
	twtid VARCHAR(18) PRIMARY KEY,
    inreplytotwtid VARCHAR(18)
    );

/*6. Create table for the friendship links*/
CREATE TABLE IF NOT EXISTS links_Master (
	userid1 VARCHAR(18),
    userid2 VARCHAR(18),
    time_stamp TIMESTAMP,
	PRIMARY KEY(userid1, userid2)
	);
CREATE TABLE IF NOT EXISTS links_raw (
	userid VARCHAR(18),
	json_friends JSON,
	time_stamp TIMESTAMP
	);