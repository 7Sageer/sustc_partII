-- User Table
CREATE TABLE users (
    mid BIGINT PRIMARY KEY,
    name VARCHAR(255),
    sex VARCHAR(50),
    birthday DATE,
    level INT,
    sign TEXT,
    identity VARCHAR(50),
    coin INT
);

-- AuthInfo Table
CREATE TABLE auth_info (
    mid BIGINT PRIMARY KEY,
    password VARCHAR(255),
    qq VARCHAR(255),
    wechat VARCHAR(255)
);


-- Video Table
CREATE TABLE videos (
    bv VARCHAR(50) PRIMARY KEY,
    title VARCHAR(255),
    ownerMid BIGINT,
    commitTime TIMESTAMP,
    reviewTime TIMESTAMP,
    publicTime TIMESTAMP,
    duration INT,
    description TEXT,
    isPublic BOOLEAN,
    reviewer BIGINT
);

-- UserVideoInteraction Table



-- UserVideoWatch Table
CREATE TABLE user_video_watch (
    mid BIGINT,
    bv VARCHAR(50),
    watch_time FLOAT,
    PRIMARY KEY (mid, bv)
);

-- UserRelationships Table
CREATE TABLE user_relationships (
    followerMid BIGINT,
    followingMid BIGINT,
    PRIMARY KEY (followerMid, followingMid)
);

-- Danmu Table
CREATE TABLE danmus (
    id SERIAL PRIMARY KEY,
    bv VARCHAR(50),
    mid BIGINT,
    time FLOAT,
    content TEXT,
    postTime TIMESTAMP
);

-- DanmuLike Table
CREATE TABLE danmu_like (
    danmuId INT,
    mid BIGINT,
    PRIMARY KEY (danmuId, mid)
);

CREATE TABLE video_interactions_aggregates (
    bv VARCHAR(50) PRIMARY KEY,
    like_count INT,
    coin_count INT,
    fav_count INT
);

CREATE TABLE video_stats (
    bv VARCHAR(50) PRIMARY KEY,
    like_rate FLOAT,
    coin_rate FLOAT,
    fav_rate FLOAT
);

CREATE TABLE video_aggregates (
    bv VARCHAR(50) PRIMARY KEY,
    avg_finish FLOAT
);



CREATE OR REPLACE FUNCTION recommend_videos_for_user(authMid BIGINT, pageSize INT, pageNum INT)
RETURNS TABLE(bv VARCHAR(50)) AS $$
BEGIN
    RETURN QUERY
    SELECT v.bv
    FROM videos v
    JOIN LATERAL (
        SELECT uvw.bv
        FROM user_video_watch uvw
        JOIN user_relationships ur ON uvw.mid = ur.followingMid AND ur.followerMid = authMid
        WHERE uvw.bv NOT IN (
            SELECT uvw_inner.bv FROM user_video_watch uvw_inner WHERE uvw_inner.mid = authMid
        )
        AND uvw.bv = v.bv
        GROUP BY uvw.bv
    ) AS friend_videos ON true
    JOIN users u ON v.ownerMid = u.mid
    GROUP BY v.bv, u.level, v.publicTime
    ORDER BY COUNT(friend_videos.bv) DESC, u.level DESC, v.publicTime DESC
    LIMIT pageSize OFFSET (pageNum - 1) * pageSize;
END;
$$ LANGUAGE plpgsql;








create function update_video_aggregates() returns void
    language plpgsql
as
$$
DECLARE
    v_record RECORD;
BEGIN
    FOR v_record IN SELECT v.bv, AVG(uvw.watch_time) / NULLIF(v.duration, 0) AS avg_finish
                    FROM videos v
                    JOIN user_video_watch uvw ON v.bv = uvw.bv
                    GROUP BY v.bv
    LOOP
        -- 更新或插入到 video_aggregates 表
        INSERT INTO video_aggregates(bv, avg_finish)
        VALUES (v_record.bv, v_record.avg_finish)
        ON CONFLICT (bv) DO UPDATE
        SET avg_finish = v_record.avg_finish;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION update_video_interactions_aggregates()
RETURNS VOID AS $$
BEGIN
    -- 从user_video_interaction表中聚合数据
    WITH aggregated_data AS (
        SELECT
            bv,
            COUNT(*) FILTER (WHERE is_liked) AS like_count,
            COUNT(*) FILTER (WHERE is_coined) AS coin_count,
            COUNT(*) FILTER (WHERE is_favorited) AS fav_count
        FROM
            user_video_interaction
        GROUP BY bv
    )
    -- 将聚合数据插入video_interactions_aggregates表，如果存在则更新
    INSERT INTO video_interactions_aggregates (bv, like_count, coin_count, fav_count)
    SELECT bv, like_count, coin_count, fav_count
    FROM aggregated_data
    ON CONFLICT (bv) DO UPDATE
    SET like_count = EXCLUDED.like_count,
        coin_count = EXCLUDED.coin_count,
        fav_count = EXCLUDED.fav_count;
END;
$$ LANGUAGE plpgsql;



create or replace function update_video_stats() returns void
    language plpgsql
as
$$
DECLARE
    v_record RECORD;
BEGIN
    FOR v_record IN
        SELECT
            v.bv,
            CASE
                WHEN uvw.view_count > 0 THEN COALESCE(via.like_count, 0)::FLOAT / uvw.view_count
                ELSE 0
            END AS like_rate,
            CASE
                WHEN uvw.view_count > 0 THEN COALESCE(via.coin_count, 0)::FLOAT / uvw.view_count
                ELSE 0
            END AS coin_rate,
            CASE
                WHEN uvw.view_count > 0 THEN COALESCE(via.fav_count, 0)::FLOAT / uvw.view_count
                ELSE 0
            END AS fav_rate
        FROM
            videos v
            JOIN video_interactions_aggregates via ON v.bv = via.bv
            LEFT JOIN (
                SELECT
                    bv,
                    COUNT(DISTINCT mid) AS view_count
                FROM
                    user_video_watch
                GROUP BY
                    bv
            ) uvw ON v.bv = uvw.bv
    LOOP
        -- 更新或插入到 video_stats 表
        INSERT INTO video_stats(bv, like_rate, coin_rate, fav_rate)
        VALUES (v_record.bv, v_record.like_rate, v_record.coin_rate, v_record.fav_rate)
        ON CONFLICT (bv) DO UPDATE
        SET like_rate = v_record.like_rate,
            coin_rate = v_record.coin_rate,
            fav_rate = v_record.fav_rate;
    END LOOP;
END;

$$;

CREATE OR REPLACE FUNCTION recommendFriends(auth BIGINT, pageSize INT, pageNum INT)
RETURNS TABLE(mid BIGINT) AS $$
BEGIN
    -- Check for invalid auth, pageSize, or pageNum
    IF auth IS NULL OR pageSize <= 0 OR pageNum <= 0 THEN
        RETURN QUERY SELECT NULL;
        RETURN;
    END IF;

    RETURN QUERY
    WITH user_followings AS (
        SELECT followingMid
        FROM user_relationships
        WHERE followerMid = auth
    ),
    potential_friends AS (
        SELECT ur.followerMid, COUNT(*) AS common_followings, u.level
        FROM user_relationships ur
        JOIN user_followings uf ON ur.followingMid = uf.followingMid
        JOIN users u ON ur.followerMid = u.mid
        WHERE ur.followerMid <> auth
        GROUP BY ur.followerMid, u.level
    )
    SELECT pf.followerMid
    FROM potential_friends pf
    ORDER BY pf.common_followings DESC, pf.level DESC, pf.followerMid
    OFFSET (pageNum - 1) * pageSize
    LIMIT pageSize;

END;
$$ LANGUAGE plpgsql;


