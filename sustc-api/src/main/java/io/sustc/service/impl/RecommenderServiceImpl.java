package io.sustc.service.impl;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import io.sustc.dto.AuthInfo;
import io.sustc.service.impl.Tools.Authenticate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Transactional
@Service
public class RecommenderServiceImpl implements io.sustc.service.RecommenderService {
    @Autowired
    DataSource dataSource;

    @Override
    public List<String> recommendNextVideo(String bv) {
        try(Connection conn = dataSource.getConnection();) {
            
            String sql = "SELECT v.bv, COUNT(uvw.mid) AS common_viewers " +
                    "FROM user_video_watch uvw " +
                    "JOIN videos v ON uvw.bv = v.bv " +
                    "WHERE uvw.mid IN ( " +
                    "    SELECT mid " +
                    "    FROM user_video_watch " +
                    "    WHERE bv = ? " +
                    ") " +
                    "AND uvw.bv != ? " +
                    "GROUP BY v.bv " +
                    "ORDER BY common_viewers DESC " +
                    "LIMIT 5;";

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, bv);
            ps.setString(2, bv);
            ps.execute();

            ResultSet rs = ps.getResultSet();
            List<String> result = new ArrayList<String>();
            while (rs.next()) {
                result.add(rs.getString("bv"));
            }
            if (result.size() == 0) {
                log.info("No similar videos found, or the video does not exist");
                return null;
            }
            log.info("Successfully get the result of recommendNextVideo");
            return result;
        } catch (Exception e) {
            log.error("Failed to get the result of recommendNextVideo");
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        if(Global.need_to_update == true){
            //call update_video_aggregates() update_video_interactions_aggregates() update_video_stats();
            try (Connection conn = dataSource.getConnection();){
                try (CallableStatement callableStatement = conn.prepareCall("{CALL update_video_aggregates()}")) {
                    callableStatement.execute();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                try (CallableStatement callableStatement = conn.prepareCall("{CALL update_video_interactions_aggregates()}")) {
                    callableStatement.execute();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                try (CallableStatement callableStatement = conn.prepareCall("{CALL update_video_stats()}")) {
                    callableStatement.execute();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                Global.need_to_update = false;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            Global.need_to_update = false;
        }
        if (pageSize <= 0 || pageNum <= 0) {
            log.error("Invalid pageSize or pageNum");
            return null;
        }

        List<String> recommendations = new ArrayList<>();
        String sql = "SELECT " +
                "v.bv, " +
                "COALESCE(vs.like_rate, 0) + COALESCE(vs.coin_rate, 0) + COALESCE(vs.fav_rate, 0) + COALESCE(va.avg_finish, 0) + COALESCE(danmu_avg, 0) AS total_score "
                +
                "FROM " +
                "videos v " +
                "LEFT JOIN " +
                "video_stats vs ON v.bv = vs.bv " +
                "LEFT JOIN " +
                "video_aggregates va ON v.bv = va.bv " +
                "LEFT JOIN ( " +
                "    SELECT " +
                "        bv, " +
                "        COUNT(*) / NULLIF(COUNT(DISTINCT mid), 0) AS danmu_avg " +
                "    FROM " +
                "        danmus " +
                "    GROUP BY " +
                "        bv " +
                ") danmu_data ON v.bv = danmu_data.bv " +
                "ORDER BY " +
                "    total_score DESC " +
                "OFFSET ? LIMIT ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            int offset = (pageNum - 1) * pageSize;
            pstmt.setInt(1, offset);
            pstmt.setInt(2, pageSize);

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    recommendations.add(rs.getString("bv"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        log.info("Successfully get the result of generalRecommendations");
        return recommendations;
    }

    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
        try (Connection conn = dataSource.getConnection()) {
            if (Authenticate.authenticate(auth, conn) == null) {
                log.error("Invalid auth");
                return null;
            } else {
                auth.setMid(Authenticate.getMid(auth, conn));
                String sql = "SELECT recommend_videos_for_user(?,?,?);";
                PreparedStatement ps = conn.prepareStatement(sql);
                ps.setLong(1, auth.getMid());
                ps.setInt(2, pageSize);
                ps.setInt(3, pageNum);
                ps.execute();
                ResultSet rs = ps.getResultSet();
                List<String> result = new ArrayList<String>();
                while (rs.next()) {
                    result.add(rs.getString("recommend_videos_for_user"));
                }
                if (result.size() == 0) {
                    log.info("No similar videos found, or the video does not exist");
                    return null;
                }
                log.info("Successfully get the result of recommendVideosForUser");
                return result;
            }
        } catch (Exception e) {
            log.error("Failed to get the result of recommendVideosForUser : " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        try (Connection conn = dataSource.getConnection()) {
            if (Authenticate.authenticate(auth, conn) == null) {
                log.error("Invalid auth");
                return null;
            } else {
                auth.setMid(Authenticate.getMid(auth, conn));
                String sql = "SELECT recommendFriends(?,?,?);";

                PreparedStatement pstmt = conn.prepareStatement(sql);

                pstmt.setLong(1, auth.getMid());
                pstmt.setInt(2, pageSize);
                pstmt.setInt(3, pageNum);
                ResultSet rs = pstmt.executeQuery();
                List<Long> recommendedUserIds = new ArrayList<>();
                while (rs.next()) {
                    recommendedUserIds.add(rs.getLong("recommendedUserId"));
                }
                if (recommendedUserIds.size() == 0) {
                    log.info("No similar users found, or the user does not exist");
                    return null;
                }

                try (CallableStatement callableStatement = conn.prepareCall("{CALL update_video_aggregates()}")) {
                    callableStatement.execute();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                log.info("Successfully get the result of recommendFriends");
                return recommendedUserIds;
            }
        } catch (Exception e) {
            log.error("Failed to get the result of recommendFriends");
            e.printStackTrace();
            return null;
        }
    }

}
