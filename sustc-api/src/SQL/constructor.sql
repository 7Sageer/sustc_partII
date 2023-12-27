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
    mid BIGINT PRIMARY KEY REFERENCES users(mid),
    password VARCHAR(255),
    qq VARCHAR(255),
    wechat VARCHAR(255)
);


-- Video Table
CREATE TABLE videos (
    bv VARCHAR(50) PRIMARY KEY,
    title VARCHAR(255),
    ownerMid BIGINT REFERENCES users(mid),
    commitTime TIMESTAMP,
    reviewTime TIMESTAMP,
    publicTime TIMESTAMP,
    duration INT,
    description TEXT,
    isPublic BOOLEAN,
    reviewer BIGINT REFERENCES users(mid)
);

-- UserVideoInteraction Table
CREATE TABLE user_video_interaction (
    mid BIGINT REFERENCES users(mid),
    bv VARCHAR(50) REFERENCES videos(bv),
    is_liked BOOLEAN DEFAULT FALSE,
    is_coined BOOLEAN DEFAULT FALSE,
    is_favorited BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (mid, bv)
);


-- UserVideoWatch Table
CREATE TABLE user_video_watch (
    mid BIGINT REFERENCES users(mid),
    bv VARCHAR(50) REFERENCES videos(bv),
    watch_time FLOAT,
    PRIMARY KEY (mid, bv)
);

-- UserRelationships Table
CREATE TABLE user_relationships (
    followerMid BIGINT REFERENCES users(mid),
    followingMid BIGINT REFERENCES users(mid),
    PRIMARY KEY (followerMid, followingMid)
);

-- Danmu Table
CREATE TABLE danmus (
    id SERIAL PRIMARY KEY,
    bv VARCHAR(50) REFERENCES videos(bv),
    mid BIGINT REFERENCES users(mid),
    time FLOAT,
    content TEXT,
    postTime TIMESTAMP
);

-- DanmuLike Table
CREATE TABLE danmu_like (
    danmuId INT REFERENCES danmus(id),
    mid BIGINT REFERENCES users(mid),
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


CREATE OR REPLACE FUNCTION recommend_friends_for_user(current_user_id BIGINT, pagesize INTEGER, pagenum INTEGER)
RETURNS TABLE(mid BIGINT) AS $$
BEGIN
    RETURN QUERY
    SELECT u.mid
    FROM users u
    JOIN (
        SELECT ur.followingMid
        FROM user_relationships ur
        WHERE ur.followerMid = current_user_id
    ) AS current_user_followings ON u.mid = current_user_followings.followingMid
    LEFT JOIN (
        SELECT ur.followerMid
        FROM user_relationships ur
        WHERE ur.followingMid = current_user_id
    ) AS current_user_followers ON u.mid = current_user_followers.followerMid
    WHERE current_user_followers.followerMid IS NULL
    GROUP BY u.mid, u.level
    ORDER BY COUNT(u.mid) DESC, u.level DESC, u.mid
    LIMIT pagesize OFFSET (pagenum - 1) * pagesize;
END; $$
LANGUAGE plpgsql;


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
                    JOIN video_interactions_aggregates via ON v.bv = via.bv
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
