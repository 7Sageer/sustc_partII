package io.sustc.service.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.crypto.Mac;
import javax.sql.DataSource;
import java.sql.Connection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.dto.UserRecord.Identity;
import io.sustc.service.VideoService;
import io.sustc.service.impl.Tools.Authenticate;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@Transactional
public class VideoServiceImpl implements VideoService {

    @Autowired
    private DataSource dataSource;

    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        try (Connection conn = dataSource.getConnection();) {
            if (Authenticate.videoAuthenticate(req, auth, conn) == 0) {
                auth.setMid(Authenticate.getMid(auth, conn));
                // generate an uuid by using UUID.randomUUID().toString()
                String bv = UUID.randomUUID().toString();
                String sql = "INSERT INTO videos (bv, ownermid, title, description, duration, committime, ispublic) VALUES (?, ?, ?, ?, ?, ?, ?);";
                PreparedStatement ps = conn.prepareStatement(sql);
                ps.setString(1, bv);
                ps.setLong(2, auth.getMid());
                ps.setString(3, req.getTitle());
                ps.setString(4, req.getDescription());
                ps.setFloat(5, req.getDuration());
                if (req.getPublicTime() == null)
                    return null;
                else
                    ps.setTimestamp(6, req.getPublicTime());
                ps.setBoolean(7, false);
                ps.executeUpdate();
                log.info("Successfully post video: {}", bv);
                Global.need_to_update = true;
                return bv;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        try (Connection conn = dataSource.getConnection();) {
            conn.setAutoCommit(false);

            Identity identity = Authenticate.authenticate(auth, conn);
            auth.setMid(Authenticate.getMid(auth, conn));
            if (identity == null)
                return false;
            String sql = "SELECT ownermid FROM videos WHERE bv = ?;";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, bv);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                log.error("Delete video failed: bv not found");
                return false;
            }
            if (rs.getLong("ownermid") == auth.getMid() || identity == Identity.SUPERUSER) {
                String watch = "DELETE FROM user_video_watch WHERE bv = ?;";
                PreparedStatement watchps = conn.prepareStatement(watch);
                watchps.setString(1, bv);
                watchps.executeUpdate();
                String interaction = "DELETE FROM user_video_interaction WHERE bv = ?;";
                PreparedStatement interactionps = conn.prepareStatement(interaction);
                interactionps.setString(1, bv);
                interactionps.executeUpdate();
                String danmulike = "DELETE FROM danmu_like WHERE danmuid IN (SELECT id FROM danmus WHERE bv = ?);";
                PreparedStatement danmulikeps = conn.prepareStatement(danmulike);
                danmulikeps.setString(1, bv);
                danmulikeps.executeUpdate();
                String danmu = "DELETE FROM danmus WHERE bv = ?;";
                PreparedStatement danmups = conn.prepareStatement(danmu);
                danmups.setString(1, bv);
                danmups.executeUpdate();
                sql = "DELETE FROM videos WHERE bv = ?;";
                ps = conn.prepareStatement(sql);
                ps.setString(1, bv);
                ps.executeUpdate();
                log.info("Successfully delete video: {}", bv);
                Global.need_to_update = true;
                conn.commit();
                return true;
            } else {
                log.error("Delete video failed: permission denied: ownermid is {} and authmid is {}",
                        rs.getLong("ownermid"), auth.getMid());
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        try (Connection conn = dataSource.getConnection();) {

            if (Authenticate.videoAuthenticate(req, auth, conn) == 0) {
                auth.setMid(Authenticate.getMid(auth, conn));
                try {
                    PostVideoReq oldreq = getVideoReq(auth, bv, conn);
                    if (oldreq == null) {
                        log.error("Update video failed: bv not found");
                        return false;
                    }
                    if (oldreq.getDuration() != req.getDuration()) {
                        log.error("Update video failed: duration cannot be changed");
                        return false;
                    } else if (oldreq.getDescription() == req.getDescription() && oldreq.getTitle() == req.getTitle()
                            && oldreq.getPublicTime() == req.getPublicTime()) {
                        log.error("Update video failed: no change");
                        return false;
                    }
                    String sql = "UPDATE videos SET title = ?, description = ?, duration = ?, committime = ?, ispublic = ? , publictime = ? WHERE bv = ?;";
                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.setString(1, req.getTitle());
                    ps.setString(2, req.getDescription());
                    ps.setFloat(3, req.getDuration());
                    ps.setTimestamp(4, req.getPublicTime());

                    if (req.getPublicTime() == null) {
                        log.error("Post video failed: publicTime is null");
                        return false;
                    } else {
                        ps.setTimestamp(6, req.getPublicTime());
                    }

                    ps.setString(7, bv);
                    ps.setBoolean(5, false);
                    ps.executeUpdate();
                    log.info("Successfully update video: {}", bv);
                    return true;
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            } else {
                log.error("Update video failed: authentication failed");
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        if (keywords == null || keywords.isEmpty()) {
            log.error("Search video failed: keywords is null or empty");
            return null;
        }

        try (Connection conn = dataSource.getConnection()) {
            if (Authenticate.authenticate(auth, conn) == null) {
                log.error("Search video failed: authentication failed");
                return null;
            }

            auth.setMid(Authenticate.getMid(auth, conn));
            String[] keywordArray = keywords.split("\\s+");

            String sql = "SELECT v.bv, v.title, v.description, u.name as ownerName "
                       + "FROM videos v "
                       + "JOIN users u ON v.ownermid = u.mid";

            Map<String, Integer> countMap = new HashMap<>();
            Map<String, Integer> viewMap = new HashMap<>();

            for (String keyword : keywordArray) {
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ResultSet rs = ps.executeQuery();
                    while (rs.next()) {
                        String bv = rs.getString("bv");
                        String ownerName = rs.getString("ownerName");
                        int matches = countMatches(rs, keyword, ownerName);
                        if (matches > 0) {
                            int currentCount = countMap.getOrDefault(bv, 0);
                            countMap.put(bv, currentCount + matches);
                            viewMap.putIfAbsent(bv, getViewCount(bv, conn));
                        }
                    }
                }
            }

            List<String> sortedBvs = sortResults(countMap, viewMap);
            return paginateResults(sortedBvs, pageSize, pageNum);
        } catch (SQLException e) {
            log.error("Search video failed: SQL exception", e);
            return null;
        }
    }

    private int countMatches(ResultSet rs, String keyword, String ownerName) throws SQLException {
        int matches = 0;
        Pattern pattern = Pattern.compile(Pattern.quote(keyword), Pattern.CASE_INSENSITIVE);

        matches += findMatches(pattern, rs.getString("title"));
        matches += findMatches(pattern, rs.getString("description"));
        if (ownerName != null) {
            matches += findMatches(pattern, ownerName);
        }

        return matches;
    }

    private int findMatches(Pattern pattern, String text) {
        if(text == null) {
            return 0;
        }
        Matcher matcher = pattern.matcher(text);
        int matches = 0;
        while (matcher.find()) {
            matches++;
        }
        return matches;
    }

    private List<String> sortResults(Map<String, Integer> countMap, Map<String, Integer> viewMap) {
        return countMap.entrySet().stream()
                .sorted((entry1, entry2) -> {
                    int countCompare = entry2.getValue().compareTo(entry1.getValue());
                    if (countCompare != 0)
                        return countCompare;
                    return viewMap.get(entry2.getKey()).compareTo(viewMap.get(entry1.getKey()));
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    private List<String> paginateResults(List<String> sortedBvs, int pageSize, int pageNum) {
        int start = (pageNum - 1) * pageSize;
        int end = Math.min(start + pageSize, sortedBvs.size());
        if (start < 0 || start > sortedBvs.size() || pageSize <= 0) {
            log.error("Invalid pagination parameters");
            return Collections.emptyList();
        }
        return new ArrayList<>(sortedBvs.subList(start, end));
    }

    @Override
    public double getAverageViewRate(String bv) {
        try (Connection conn = dataSource.getConnection();) {
            if (!checkVideoExists(bv, conn)) {
                log.error("Get average view rate failed: bv not found");
                return -1;
            }
            String sql = "SELECT * FROM user_video_watch WHERE bv = ?;";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, bv);
            ResultSet rs = ps.executeQuery();
            int count = 0;
            double sum = 0;
            while (rs.next()) {
                count++;
                sum += rs.getDouble("watch_time");
            }
            if (count == 0) {
                log.error("Get average view rate failed: bv not found");
                return -1;
            }
            sql = "SELECT * FROM videos WHERE bv = ?;";
            ps = conn.prepareStatement(sql);
            ps.setString(1, bv);
            rs = ps.executeQuery();
            rs.next();
            double duration = rs.getDouble("duration");
            log.info("Successfully get average view rate: {}", sum / (duration * count));
            return sum / (duration * count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        try (Connection conn = dataSource.getConnection();) {

            if (!checkVideoExists(bv, conn)) {
                log.error("Get hotspot failed: bv not found");
                return null;
            }

            String video = "SELECT duration FROM videos WHERE bv = ?;";
            PreparedStatement videops = conn.prepareStatement(video);
            videops.setString(1, bv);
            ResultSet videors = videops.executeQuery();
            videors.next();
            int duration = (int) videors.getFloat("duration");

            String danmu = "SELECT time FROM danmus WHERE bv = ?;";
            PreparedStatement danmups = conn.prepareStatement(danmu);
            danmups.setString(1, bv);
            ArrayList<Integer> Scores = new ArrayList<>(duration / 10 + 1);
            for (int i = 0; i < duration / 10 + 1; i++) {
                Scores.add(0);
            }
            ResultSet danmurs = danmups.executeQuery();

            while (danmurs.next()) {
                Float time = danmurs.getFloat("time");
                Scores.set((int) (time / 10), Scores.get((int) (time / 10)) + 1);
            }
            // find the max value in Scores
            int maxScore = 0;
            boolean allSameScore = true;
            for (int score : Scores) {
                if (score > maxScore) {
                    maxScore = score;
                    allSameScore = false;
                } else if (score != maxScore && maxScore != 0) {
                    allSameScore = false;
                }
            }

            if (allSameScore) {
                return new HashSet<>();
            }

            Set<Integer> result = new HashSet<>();
            for (int i = 0; i < Scores.size(); i++) {
                if (Scores.get(i) == maxScore) {
                    result.add(i);
                }
            }

            if (result.isEmpty()) {
                log.error("Get hotspot failed: no danmu");
                return null;
            }

            // log.info("Successfully get hotspot: {}", result);
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        try (Connection conn = dataSource.getConnection();) {

            Identity identity = Authenticate.authenticate(auth, conn);
            auth.setMid(Authenticate.getMid(auth, conn));
            if (identity != Identity.SUPERUSER) {
                log.error("Review video failed: permission denied");
                return false;
            }
            if (!checkVideoExists(bv, conn)) {
                log.error("Review video failed: bv not found");
                return false;
            }
            if (isUserVideoOwner(auth, bv, conn)) {
                log.error("Review video failed: user is the owner of the video");
                return false;
            }
            String sql = "SELECT * FROM videos WHERE bv = ?;";
            PreparedStatement checkps = conn.prepareStatement(sql);
            checkps.setString(1, bv);
            ResultSet rs = checkps.executeQuery();
            if (!rs.next()) {
                log.error("Review video failed: bv not found");
                return false;
            }
            if (rs.getBoolean("ispublic")) {
                log.error("Review video failed: video has been reviewed");
                return false;
            }

            sql = "UPDATE videos SET ispublic = true, reviewtime = now() WHERE bv = ?;";
            PreparedStatement updateps = conn.prepareStatement(sql);
            updateps.setString(1, bv);
            updateps.executeUpdate();
            log.info("Successfully review video: {}", bv);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;

        }
    }

    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        try (Connection conn = dataSource.getConnection();) {

            Identity identity = Authenticate.authenticate(auth, conn);
            auth.setMid(Authenticate.getMid(auth, conn));
            if (identity == null || !checkVideoExists(bv, conn)) {
                log.error("Coin video failed: bv not found or user not authenticated");
                return false;
            }
            if (isUserVideoOwner(auth, bv, conn)) {
                log.error("Coin video failed: user is the owner of the video");
                return false;
            }
            // if (!canUserViewVideo(auth, bv, conn)) {
            // log.error("Coin video failed: user cannot view the video");
            // return false;
            // }

            String coinnum = "SELECT coin FROM users WHERE mid = ?;";
            PreparedStatement coinps = conn.prepareStatement(coinnum);
            coinps.setLong(1, auth.getMid());
            ResultSet coinrs = coinps.executeQuery();
            coinrs.next();
            int coin = coinrs.getInt("coin");
            if (coin <= 0) {
                log.error("Coin video failed: user has no coin");
                return false;
            }

            Boolean isCoined = getUserVideoInteractionStatus(auth, bv, "is_coined", conn);
            if (isCoined == null) {
                String sql = "INSERT INTO user_video_interaction (mid, bv, is_favorited, is_coined, is_liked) VALUES (?, ?, ?, ?, ?);";
                PreparedStatement ps = conn.prepareStatement(sql);
                ps.setLong(1, auth.getMid());
                ps.setString(2, bv);
                ps.setBoolean(3, false);
                ps.setBoolean(4, true);
                ps.setBoolean(5, false);
                ps.executeUpdate();
                sql = "UPDATE users SET coin = ? WHERE mid = ?;";
                ps = conn.prepareStatement(sql);
                ps.setInt(1, coin - 1);
                ps.setLong(2, auth.getMid());
                ps.executeUpdate();
            } else if (isCoined == true) {
                log.error("Coin video failed: user has already coined the video");
                return false;
            } else {
                updateUserVideoInteraction(auth, bv, "is_coined", true, conn);
                String sql = "UPDATE users SET coin = ? WHERE mid = ?;";
                PreparedStatement ps = conn.prepareStatement(sql);
                ps.setInt(1, coin - 1);
                ps.setLong(2, auth.getMid());
                ps.executeUpdate();
            }
            log.info("Successfully coin video: {}", bv);
            Global.need_to_update = true;
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        try (Connection conn = dataSource.getConnection();) {
            Identity identity = Authenticate.authenticate(auth, conn);
            auth.setMid(Authenticate.getMid(auth, conn));
            if (identity == null || !checkVideoExists(bv, conn)) {
                log.error("Like video failed: bv not found or user not authenticated");
                return false;
            }
            if (isUserVideoOwner(auth, bv, conn)) {
                log.error("Like video failed: user is the owner of the video");
                return false;
            }
            // if (!canUserViewVideo(auth, bv, conn)) {
            // log.error("Like video failed: user cannot view the video");
            // return false;
            // }
            // if(!isUserWatched(auth, bv, conn)){
            // log.error("Like video failed: user has not watched the video: {} {}",
            // auth.getMid(), bv);
            // return false;
            // }

            Boolean isLiked = getUserVideoInteractionStatus(auth, bv, "is_liked", conn);
            Global.need_to_update = true;
            if (isLiked != null) {
                updateUserVideoInteraction(auth, bv, "is_liked", !isLiked, conn);
                return !isLiked;
            } else {
                String sql = "INSERT INTO user_video_interaction (mid, bv, is_favorited, is_coined, is_liked) VALUES (?, ?, ?, ?, ?);";
                PreparedStatement ps = conn.prepareStatement(sql);
                ps.setLong(1, auth.getMid());
                ps.setString(2, bv);
                ps.setBoolean(3, false);
                ps.setBoolean(4, false);
                ps.setBoolean(5, true);
                ps.executeUpdate();
                return true;
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        try (Connection conn = dataSource.getConnection();) {
            Identity identity = Authenticate.authenticate(auth, conn);
            auth.setMid(Authenticate.getMid(auth, conn));
            if (identity == null || !checkVideoExists(bv, conn)) {
                log.error("Collect video failed: bv not found or user not authenticated");
                return false;
            }
            if (isUserVideoOwner(auth, bv, conn)) {
                log.error("Collect video failed: user is the owner of the video");
                return false;
            }
            // if (!canUserViewVideo(auth, bv, conn)) {
            // log.error("Collect video failed: user cannot view the video");
            // return false;
            // }
            // if(!isUserWatched(auth, bv, conn)){
            // log.error("Collect video failed: user has not watched the video");
            // return false;
            // }
            Global.need_to_update = true;
            Boolean isFavorited = getUserVideoInteractionStatus(auth, bv, "is_favorited", conn);
            if (isFavorited != null) {
                updateUserVideoInteraction(auth, bv, "is_favorited", !isFavorited, conn);
                // log.info("Successfully change collect video status: {} {} {}", auth.getMid(),
                // bv, !isFavorited);
                return !isFavorited;
            } else {
                String sql = "INSERT INTO user_video_interaction (mid, bv, is_favorited, is_coined, is_liked) VALUES (?, ?, ?, ?, ?);";
                PreparedStatement ps = conn.prepareStatement(sql);
                ps.setLong(1, auth.getMid());
                ps.setString(2, bv);
                ps.setBoolean(3, true);
                ps.setBoolean(4, false);
                ps.setBoolean(5, false);
                ps.executeUpdate();
                // log.info("Successfully collect video: {}", bv);
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean checkVideoExists(String bv, Connection conn) throws SQLException {
        String sql = "SELECT * FROM videos WHERE bv = ?;";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, bv);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private Boolean getUserVideoInteractionStatus(AuthInfo auth, String bv, String column, Connection conn)
            throws SQLException {
        String sql = "SELECT * FROM user_video_interaction WHERE bv = ? AND mid = ?;";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, bv);
            ps.setLong(2, auth.getMid());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    // log.info("Successfully get user video interaction status: {} {} {}",
                    // auth.getMid(), bv, column);
                    return rs.getBoolean(column);
                } else {
                    return null;
                }
            }
        }
    }

    private void updateUserVideoInteraction(AuthInfo auth, String bv, String column, boolean status, Connection conn)
            throws SQLException {
        String sql = "UPDATE user_video_interaction SET " + column + " = ? WHERE bv = ? AND mid = ?;";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setBoolean(1, status);
            ps.setString(2, bv);
            ps.setLong(3, auth.getMid());
            ps.executeUpdate();
        }
    }

    private PostVideoReq getVideoReq(AuthInfo auth, String bv, Connection conn) throws SQLException {
        String sql = "SELECT * FROM videos WHERE bv = ?;";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, bv);
        ResultSet rs = ps.executeQuery();
        if (!rs.next()) {
            log.error("Get video info failed: bv not found");
            return null;
        }
        PostVideoReq req = new PostVideoReq();
        req.setTitle(rs.getString("title"));
        req.setDescription(rs.getString("description"));
        req.setDuration(rs.getFloat("duration"));
        req.setPublicTime(rs.getTimestamp("committime"));
        return req;
    }

    private boolean isUserVideoOwner(AuthInfo auth, String bv, Connection conn) throws SQLException {
        String sql = "SELECT ownermid FROM videos WHERE bv = ?;";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, bv);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("ownermid") == auth.getMid();
                }
                return false;
            }
        }
    }

    // private boolean canUserViewVideo(AuthInfo auth, String bv, Connection conn)
    // throws SQLException {
    // String sql = "SELECT * FROM videos WHERE bv = ?;";
    // try (PreparedStatement ps = conn.prepareStatement(sql)) {
    // Identity identity = Authenticate.authenticate(auth, conn);
    // ps.setString(1, bv);
    // try (ResultSet rs = ps.executeQuery()) {
    // if (rs.next()) {
    // // get public time
    // Timestamp publicTime = rs.getTimestamp("committime");
    // if (System.currentTimeMillis() < publicTime.getTime()) {
    // return identity == Identity.SUPERUSER || isUserVideoOwner(auth, bv, conn);
    // }

    // return rs.getBoolean("ispublic") || identity == Identity.SUPERUSER;
    // }
    // return false;
    // }
    // }
    // }

    private int getViewCount(String bv, Connection conn) throws SQLException {
        String sql = "SELECT COUNT(mid) as view_count FROM user_video_watch WHERE bv = ?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, bv);
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getInt("view_count");
            }
            return 0;
        }
    }

    // private boolean isUserWatched (AuthInfo auth, String bv, Connection conn)
    // throws SQLException {
    // String sql = "SELECT * FROM user_video_watch WHERE bv = ? AND mid = ?;";
    // try (PreparedStatement ps = conn.prepareStatement(sql)) {
    // ps.setString(1, bv);
    // ps.setLong(2, auth.getMid());
    // try (ResultSet rs = ps.executeQuery()) {
    // if(rs.next()){
    // return true;
    // }
    // return false;
    // }
    // }
    // }

}
