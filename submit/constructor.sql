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
CREATE TABLE user_video_interaction (
    mid BIGINT,
    bv VARCHAR(50),
    is_liked BOOLEAN DEFAULT FALSE,
    is_coined BOOLEAN DEFAULT FALSE,
    is_favorited BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (mid, bv)
);


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


create function recommend_videos_for_user(current_user_id bigint, pagesize integer, pagenum integer)
    returns TABLE(bv character varying)
    language plpgsql
as
$$
DECLARE
    friend_count INT;
BEGIN
    -- 检查互关数量
    SELECT COUNT(*) INTO friend_count
    FROM user_relationships a
    JOIN user_relationships b ON a.followerMid = b.followingMid AND a.followingMid = b.followerMid
    WHERE a.followingMid = current_user_id;

    -- 如果有互关，执行特定推荐逻辑
    IF friend_count > 0 THEN
        RETURN QUERY
        SELECT vid.bv
        FROM videos vid
        JOIN users usr ON vid.ownerMid = usr.mid
        WHERE vid.bv IN (
            SELECT uvw.bv
            FROM user_video_watch uvw
            JOIN user_relationships ur ON uvw.mid = ur.followerMid OR uvw.mid = ur.followingMid
            WHERE (ur.followerMid = current_user_id OR ur.followingMid = current_user_id)
            AND uvw.mid != current_user_id
        )
        AND vid.bv NOT IN (
            SELECT uvw.bv FROM user_video_watch uvw WHERE uvw.mid = current_user_id
        )
        ORDER BY usr.level DESC, vid.publicTime DESC
        LIMIT pageSize OFFSET pageSize * (pageNum - 1);

    -- 如果没有互关，执行一般推荐逻辑
    ELSE
        RETURN QUERY
        SELECT vid.bv
        FROM videos vid
        JOIN users usr ON vid.ownerMid = usr.mid
        WHERE vid.bv NOT IN (
            SELECT uvw.bv FROM user_video_watch uvw WHERE uvw.mid = current_user_id
        )
        ORDER BY usr.level DESC, vid.publicTime DESC
        LIMIT pageSize OFFSET pageSize * (pageNum - 1);
    END IF;
END;
$$;

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

create function update_video_interactions_aggregates() returns void
    language plpgsql
as
$$
DECLARE
    v_record RECORD;
BEGIN
    FOR v_record IN SELECT v.bv, COUNT(uvi.is_liked) AS like_count, COUNT(uvi.is_coined) AS coin_count, COUNT(uvi.is_favorited) AS fav_count
                    FROM videos v
                    JOIN user_video_interaction uvi ON v.bv = uvi.bv
                    GROUP BY v.bv
    LOOP
        -- 更新或插入到 video_interactions_aggregates 表
        INSERT INTO video_interactions_aggregates(bv, like_count, coin_count, fav_count)
        VALUES (v_record.bv, v_record.like_count, v_record.coin_count, v_record.fav_count)
        ON CONFLICT (bv) DO UPDATE
        SET like_count = v_record.like_count,
            coin_count = v_record.coin_count,
            fav_count = v_record.fav_count;
    END LOOP;
END;
$$;


create function update_video_stats() returns void
    language plpgsql
as
$$
DECLARE
    v_record RECORD;
BEGIN
    FOR v_record IN SELECT v.bv, COALESCE(via.like_count, 0) / NULLIF(via.like_count + vaa.avg_finish, 0) AS like_rate,
                    COALESCE(via.coin_count, 0) / NULLIF(via.coin_count + vaa.avg_finish, 0) AS coin_rate,
                    COALESCE(via.fav_count, 0) / NULLIF(via.fav_count + vaa.avg_finish, 0) AS fav_rate
                    FROM videos v
                    JOIN video_aggregates vaa ON v.bv = vaa.bv
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
