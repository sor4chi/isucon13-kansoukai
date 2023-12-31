package main

import (
	"database/sql"
	"errors"
	"net/http"
	"sort"
	"strconv"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
)

type LivestreamStatistics struct {
	Rank           int64 `json:"rank"`
	ViewersCount   int64 `json:"viewers_count"`
	TotalReactions int64 `json:"total_reactions"`
	TotalReports   int64 `json:"total_reports"`
	MaxTip         int64 `json:"max_tip"`
}

type LivestreamRankingEntry struct {
	LivestreamID int64
	Score        int64
}
type LivestreamRanking []LivestreamRankingEntry

func (r LivestreamRanking) Len() int      { return len(r) }
func (r LivestreamRanking) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r LivestreamRanking) Less(i, j int) bool {
	if r[i].Score == r[j].Score {
		return r[i].LivestreamID < r[j].LivestreamID
	} else {
		return r[i].Score < r[j].Score
	}
}

type UserStatistics struct {
	Rank              int64  `json:"rank"`
	ViewersCount      int64  `json:"viewers_count"`
	TotalReactions    int64  `json:"total_reactions"`
	TotalLivecomments int64  `json:"total_livecomments"`
	TotalTip          int64  `json:"total_tip"`
	FavoriteEmoji     string `json:"favorite_emoji"`
}

type UserRankingEntry struct {
	Username string
	Score    int64
}
type UserRanking []UserRankingEntry

func (r UserRanking) Len() int      { return len(r) }
func (r UserRanking) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r UserRanking) Less(i, j int) bool {
	if r[i].Score == r[j].Score {
		return r[i].Username < r[j].Username
	} else {
		return r[i].Score < r[j].Score
	}
}

func getUserStatisticsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	username := c.Param("username")
	// ユーザごとに、紐づく配信について、累計リアクション数、累計ライブコメント数、累計売上金額を算出
	// また、現在の合計視聴者数もだす

	user, ok := userModelByNameCache.Get(username)
	if !ok {
		return echo.NewHTTPError(http.StatusBadRequest, "not found user that has the given username")
	}

	var ranking UserRanking

	query := `
	SELECT u.name, COUNT(r.id) AS reactions, IFNULL(SUM(l2.tip), 0) AS total_tips
	FROM users u
	LEFT JOIN livestreams l ON u.id = l.user_id
	LEFT JOIN reactions r ON l.id = r.livestream_id
	LEFT JOIN livecomments l2 ON l.id = l2.livestream_id
	GROUP BY u.id
	`
	var entries []*struct {
		Username  string `db:"name"`
		Reactions int64  `db:"reactions"`
		TotalTips int64  `db:"total_tips"`
	}
	if err := dbConn.SelectContext(ctx, &entries, query); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get users: "+err.Error())
	}

	for _, entry := range entries {
		ranking = append(ranking, UserRankingEntry{
			Username: entry.Username,
			Score:    entry.Reactions + entry.TotalTips,
		})
	}

	sort.Sort(ranking)

	var rank int64 = 1
	for i := len(ranking) - 1; i >= 0; i-- {
		entry := ranking[i]
		if entry.Username == username {
			break
		}
		rank++
	}

	// リアクション数
	var totalReactions int64
	query = `SELECT COUNT(*) FROM users u
    INNER JOIN livestreams l ON l.user_id = u.id
    INNER JOIN reactions r ON r.livestream_id = l.id
    WHERE u.name = ?
	`
	if err := dbConn.GetContext(ctx, &totalReactions, query, username); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count total reactions: "+err.Error())
	}

	// ライブコメント数、チップ合計
	var totalLivecomments int64
	var totalTip int64
	livestreams, ok := livestreamModelByUserIDCache.Get(user.ID)
	if !ok {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams")
	}

	livestreamIDs := make([]int64, len(livestreams))
	for i := range livestreams {
		livestreamIDs[i] = livestreams[i].ID
	}

	query, args, err := sqlx.In("SELECT * FROM livecomments WHERE livestream_id IN (?)", livestreamIDs)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to build query: "+err.Error())
	}
	query = dbConn.Rebind(query)
	var livecomments []*LivecommentModel
	if err := dbConn.SelectContext(ctx, &livecomments, query, args...); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livecomments: "+err.Error())
	}

	for _, livecomment := range livecomments {
		totalTip += livecomment.Tip
		totalLivecomments++
	}

	// 合計視聴者数
	var viewersCount int64

	query, args, err = sqlx.In("SELECT COUNT(*) FROM livestream_viewers_history WHERE livestream_id IN (?)", livestreamIDs)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to build query: "+err.Error())
	}
	query = dbConn.Rebind(query)
	if err := dbConn.GetContext(ctx, &viewersCount, query, args...); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestream_view_history: "+err.Error())
	}

	// お気に入り絵文字
	var favoriteEmoji string
	query = `
	SELECT r.emoji_name
	FROM users u
	INNER JOIN livestreams l ON l.user_id = u.id
	INNER JOIN reactions r ON r.livestream_id = l.id
	WHERE u.name = ?
	GROUP BY emoji_name
	ORDER BY COUNT(*) DESC, emoji_name DESC
	LIMIT 1
	`
	if err := dbConn.GetContext(ctx, &favoriteEmoji, query, username); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to find favorite emoji: "+err.Error())
	}

	stats := UserStatistics{
		Rank:              rank,
		ViewersCount:      viewersCount,
		TotalReactions:    totalReactions,
		TotalLivecomments: totalLivecomments,
		TotalTip:          totalTip,
		FavoriteEmoji:     favoriteEmoji,
	}
	return c.JSON(http.StatusOK, stats)
}

func getLivestreamStatisticsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		return err
	}

	id, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}
	livestreamID := int64(id)

	// ランク算出
	var ranking LivestreamRanking
	query := `
	SELECT l.id, COUNT(r.id) AS reactions, IFNULL(SUM(l2.tip), 0) AS total_tips
	FROM livestreams l
	LEFT JOIN reactions r ON l.id = r.livestream_id
	LEFT JOIN livecomments l2 ON l.id = l2.livestream_id
	GROUP BY l.id
	`
	var entries []*struct {
		LivestreamID int64 `db:"id"`
		Reactions    int64 `db:"reactions"`
		TotalTips    int64 `db:"total_tips"`
	}
	if err := dbConn.SelectContext(ctx, &entries, query); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams: "+err.Error())
	}

	for _, entry := range entries {
		ranking = append(ranking, LivestreamRankingEntry{
			LivestreamID: entry.LivestreamID,
			Score:        entry.Reactions + entry.TotalTips,
		})
	}
	sort.Sort(ranking)

	var rank int64 = 1
	for i := len(ranking) - 1; i >= 0; i-- {
		entry := ranking[i]
		if entry.LivestreamID == livestreamID {
			break
		}
		rank++
	}

	type Stats struct {
		ViewersCount   int64 `db:"viewers_count"`   // 視聴者数
		MaxTip         int64 `db:"max_tip"`         // 最大チップ額
		TotalReactions int64 `db:"total_reactions"` // リアクション数
		TotalReports   int64 `db:"total_reports"`   // スパム報告数
	}

	var stats Stats
	if err := dbConn.GetContext(ctx, &stats, `
	SELECT
		(SELECT COUNT(*) FROM livestreams l INNER JOIN livestream_viewers_history h ON h.livestream_id = l.id WHERE l.id = ?) AS viewers_count,
		(SELECT IFNULL(MAX(tip), 0) FROM livestreams l INNER JOIN livecomments l2 ON l2.livestream_id = l.id WHERE l.id = ?) AS max_tip,
		(SELECT COUNT(*) FROM livestreams l INNER JOIN reactions r ON r.livestream_id = l.id WHERE l.id = ?) AS total_reactions,
		(SELECT COUNT(*) FROM livestreams l INNER JOIN livecomment_reports r ON r.livestream_id = l.id WHERE l.id = ?) AS total_reports
	`, livestreamID, livestreamID, livestreamID, livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get stats: "+err.Error())
	}

	return c.JSON(http.StatusOK, LivestreamStatistics{
		Rank:           rank,
		ViewersCount:   stats.ViewersCount,
		MaxTip:         stats.MaxTip,
		TotalReactions: stats.TotalReactions,
		TotalReports:   stats.TotalReports,
	})
}
