package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-json-experiment/json"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/v4"
)

type ReserveLivestreamRequest struct {
	Tags         []int64 `json:"tags"`
	Title        string  `json:"title"`
	Description  string  `json:"description"`
	PlaylistUrl  string  `json:"playlist_url"`
	ThumbnailUrl string  `json:"thumbnail_url"`
	StartAt      int64   `json:"start_at"`
	EndAt        int64   `json:"end_at"`
}

type LivestreamViewerModel struct {
	UserID       int64 `db:"user_id" json:"user_id"`
	LivestreamID int64 `db:"livestream_id" json:"livestream_id"`
	CreatedAt    int64 `db:"created_at" json:"created_at"`
}

type LivestreamModel struct {
	ID           int64  `db:"id" json:"id"`
	UserID       int64  `db:"user_id" json:"user_id"`
	Title        string `db:"title" json:"title"`
	Description  string `db:"description" json:"description"`
	PlaylistUrl  string `db:"playlist_url" json:"playlist_url"`
	ThumbnailUrl string `db:"thumbnail_url" json:"thumbnail_url"`
	StartAt      int64  `db:"start_at" json:"start_at"`
	EndAt        int64  `db:"end_at" json:"end_at"`
}

type Livestream struct {
	ID           int64  `json:"id"`
	Owner        User   `json:"owner"`
	Title        string `json:"title"`
	Description  string `json:"description"`
	PlaylistUrl  string `json:"playlist_url"`
	ThumbnailUrl string `json:"thumbnail_url"`
	Tags         []Tag  `json:"tags"`
	StartAt      int64  `json:"start_at"`
	EndAt        int64  `json:"end_at"`
}

type LivestreamTagModel struct {
	ID           int64 `db:"id" json:"id"`
	LivestreamID int64 `db:"livestream_id" json:"livestream_id"`
	TagID        int64 `db:"tag_id" json:"tag_id"`
}

type ReservationSlotModel struct {
	ID      int64 `db:"id" json:"id"`
	Slot    int64 `db:"slot" json:"slot"`
	StartAt int64 `db:"start_at" json:"start_at"`
	EndAt   int64 `db:"end_at" json:"end_at"`
}

func reserveLivestreamHandler(c echo.Context) error {
	ctx := c.Request().Context()
	defer c.Request().Body.Close()

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	var req *ReserveLivestreamRequest
	if err := json.UnmarshalRead(c.Request().Body, &req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to decode the request body as json")
	}

	// 2023/11/25 10:00からの１年間の期間内であるかチェック
	var (
		termStartAt    = time.Date(2023, 11, 25, 1, 0, 0, 0, time.UTC)
		termEndAt      = time.Date(2024, 11, 25, 1, 0, 0, 0, time.UTC)
		reserveStartAt = time.Unix(req.StartAt, 0)
		reserveEndAt   = time.Unix(req.EndAt, 0)
	)
	if (reserveStartAt.Equal(termEndAt) || reserveStartAt.After(termEndAt)) || (reserveEndAt.Equal(termStartAt) || reserveEndAt.Before(termStartAt)) {
		return echo.NewHTTPError(http.StatusBadRequest, "bad reservation time range")
	}

	// 予約枠をみて、予約が可能か調べる
	// NOTE: 並列な予約のoverbooking防止にFOR UPDATEが必要
	var slots []*ReservationSlotModel
	if err := dbConn.SelectContext(ctx, &slots, "SELECT * FROM reservation_slots WHERE start_at >= ? AND end_at <= ? FOR UPDATE", req.StartAt, req.EndAt); err != nil {
		c.Logger().Warnf("予約枠一覧取得でエラー発生: %+v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get reservation_slots: "+err.Error())
	}

	conditions := make([]string, len(slots))
	for i := range slots {
		conditions[i] = fmt.Sprintf("(start_at = %d AND end_at = %d AND slot > 0)", slots[i].StartAt, slots[i].EndAt)
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM reservation_slots WHERE %s", strings.Join(conditions, " OR "))
	var count int
	if err := dbConn.GetContext(ctx, &count, query); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get reservation_slots: "+err.Error())
	}
	if count < 1 {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("予約期間 %d ~ %dに対して、予約区間 %d ~ %dが予約できません", termStartAt.Unix(), termEndAt.Unix(), req.StartAt, req.EndAt))
	}

	var (
		livestreamModel = &LivestreamModel{
			UserID:       int64(userID),
			Title:        req.Title,
			Description:  req.Description,
			PlaylistUrl:  req.PlaylistUrl,
			ThumbnailUrl: req.ThumbnailUrl,
			StartAt:      req.StartAt,
			EndAt:        req.EndAt,
		}
	)

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "UPDATE reservation_slots SET slot = slot - 1 WHERE start_at >= ? AND end_at <= ?", req.StartAt, req.EndAt); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to update reservation_slot: "+err.Error())
	}

	rs, err := tx.NamedExecContext(ctx, "INSERT INTO livestreams (user_id, title, description, playlist_url, thumbnail_url, start_at, end_at) VALUES(:user_id, :title, :description, :playlist_url, :thumbnail_url, :start_at, :end_at)", livestreamModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert livestream: "+err.Error())
	}

	livestreamID, err := rs.LastInsertId()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get last inserted livestream id: "+err.Error())
	}
	livestreamModel.ID = livestreamID
	livestreamModelByIdCache.Set(livestreamID, *livestreamModel)
	livestreamModelsByUserID, ok := livestreamModelByUserIDCache.Get(livestreamModel.UserID)
	if !ok {
		livestreamModelsByUserID = make([]*LivestreamModel, 0)
	}
	livestreamModelsByUserID = append(livestreamModelsByUserID, livestreamModel)
	livestreamModelByUserIDCache.Set(livestreamModel.UserID, livestreamModelsByUserID)

	// タグ追加
	livestreamTagModels := make([]*LivestreamTagModel, len(req.Tags))
	for i := range req.Tags {
		livestreamTagModels[i] = &LivestreamTagModel{
			LivestreamID: livestreamID,
			TagID:        req.Tags[i],
		}
	}

	if len(livestreamTagModels) > 0 {
		if _, err := tx.NamedExecContext(ctx, "INSERT INTO livestream_tags (livestream_id, tag_id) VALUES (:livestream_id, :tag_id)", livestreamTagModels); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert livestream tag: "+err.Error())
		}
	}

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	livestream, err := fillLivestreamResponse(ctx, dbConn, *livestreamModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill livestream: "+err.Error())
	}

	return c.JSON(http.StatusCreated, livestream)
}

func searchLivestreamsHandler(c echo.Context) error {
	ctx := c.Request().Context()
	keyTagName := c.QueryParam("tag")

	var livestreamModels []*LivestreamModel
	if c.QueryParam("tag") != "" {
		// タグによる取得
		var tagIDList []int64
		all := tagModelCache.All()
		for _, tagModel := range all {
			if tagModel.Name == keyTagName {
				tagIDList = append(tagIDList, tagModel.ID)
			}
		}

		query, params, err := sqlx.In("SELECT * FROM livestream_tags WHERE tag_id IN (?) ORDER BY livestream_id DESC", tagIDList)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to construct IN query: "+err.Error())
		}
		var keyTaggedLivestreams []*LivestreamTagModel
		if err := dbConn.SelectContext(ctx, &keyTaggedLivestreams, query, params...); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get keyTaggedLivestreams: "+err.Error())
		}

		livestreamIDs := make([]int64, len(keyTaggedLivestreams))
		for i := range keyTaggedLivestreams {
			livestreamIDs[i] = keyTaggedLivestreams[i].LivestreamID
		}

		if len(livestreamIDs) > 0 {
			query, params, err = sqlx.In("SELECT * FROM livestreams WHERE id IN (?) ORDER BY id DESC", livestreamIDs)
			if err != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, "failed to construct IN query: "+err.Error())
			}
			if err := dbConn.SelectContext(ctx, &livestreamModels, query, params...); err != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams: "+err.Error())
			}
		}
	} else {
		// 検索条件なし
		query := `SELECT * FROM livestreams ORDER BY id DESC`
		if c.QueryParam("limit") != "" {
			limit, err := strconv.Atoi(c.QueryParam("limit"))
			if err != nil {
				return echo.NewHTTPError(http.StatusBadRequest, "limit query parameter must be integer")
			}
			query += fmt.Sprintf(" LIMIT %d", limit)
		}

		if err := dbConn.SelectContext(ctx, &livestreamModels, query); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams: "+err.Error())
		}
	}

	livestreams, err := fillLivestreamResponseBulk(ctx, dbConn, livestreamModels)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill livestream: "+err.Error())
	}

	return c.JSON(http.StatusOK, livestreams)
}

func getMyLivestreamsHandler(c echo.Context) error {
	ctx := c.Request().Context()
	if err := verifyUserSession(c); err != nil {
		return err
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	var livestreamModels []*LivestreamModel
	if err := dbConn.SelectContext(ctx, &livestreamModels, "SELECT * FROM livestreams WHERE user_id = ?", userID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams: "+err.Error())
	}
	livestreams, err := fillLivestreamResponseBulk(ctx, dbConn, livestreamModels)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill livestream: "+err.Error())
	}

	return c.JSON(http.StatusOK, livestreams)
}

func getUserLivestreamsHandler(c echo.Context) error {
	ctx := c.Request().Context()
	if err := verifyUserSession(c); err != nil {
		return err
	}

	username := c.Param("username")

	user, ok := userModelByNameCache.Get(username)
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "user not found")
	}

	var livestreamModels []*LivestreamModel
	if err := dbConn.SelectContext(ctx, &livestreamModels, "SELECT * FROM livestreams WHERE user_id = ?", user.ID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams: "+err.Error())
	}

	livestreams, err := fillLivestreamResponseBulk(ctx, dbConn, livestreamModels)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill livestream: "+err.Error())
	}

	return c.JSON(http.StatusOK, livestreams)
}

// viewerテーブルの廃止
func enterLivestreamHandler(c echo.Context) error {
	ctx := c.Request().Context()
	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id must be integer")
	}

	viewer := LivestreamViewerModel{
		UserID:       int64(userID),
		LivestreamID: int64(livestreamID),
		CreatedAt:    time.Now().Unix(),
	}

	if _, err := dbConn.NamedExecContext(ctx, "INSERT INTO livestream_viewers_history (user_id, livestream_id, created_at) VALUES(:user_id, :livestream_id, :created_at)", viewer); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert livestream_view_history: "+err.Error())
	}

	return c.NoContent(http.StatusOK)
}

func exitLivestreamHandler(c echo.Context) error {
	ctx := c.Request().Context()
	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	if _, err := dbConn.ExecContext(ctx, "DELETE FROM livestream_viewers_history WHERE user_id = ? AND livestream_id = ?", userID, livestreamID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to delete livestream_view_history: "+err.Error())
	}

	return c.NoContent(http.StatusOK)
}

func getLivestreamHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		return err
	}

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	livestreamModel := LivestreamModel{}
	err = dbConn.GetContext(ctx, &livestreamModel, "SELECT * FROM livestreams WHERE id = ?", livestreamID)
	if errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusNotFound, "not found livestream that has the given id")
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestream: "+err.Error())
	}

	livestream, err := fillLivestreamResponse(ctx, dbConn, livestreamModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill livestream: "+err.Error())
	}

	return c.JSON(http.StatusOK, livestream)
}

func getLivecommentReportsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		return err
	}

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	var livestreamModel LivestreamModel
	if err := dbConn.GetContext(ctx, &livestreamModel, "SELECT * FROM livestreams WHERE id = ?", livestreamID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestream: "+err.Error())
	}

	// error already check
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already check
	userID := sess.Values[defaultUserIDKey].(int64)

	if livestreamModel.UserID != userID {
		return echo.NewHTTPError(http.StatusForbidden, "can't get other streamer's livecomment reports")
	}

	var reportModels []LivecommentReportModel
	if err := dbConn.SelectContext(ctx, &reportModels, "SELECT * FROM livecomment_reports WHERE livestream_id = ?", livestreamID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livecomment reports: "+err.Error())
	}

	reports, err := fillLivecommentReportResponseBulk(ctx, dbConn, reportModels)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill livecomment report: "+err.Error())
	}

	return c.JSON(http.StatusOK, reports)
}

func fillLivestreamResponse(ctx context.Context, db *sqlx.DB, livestreamModel LivestreamModel) (Livestream, error) {
	ownerModel, ok := userModelByIdCache.Get(livestreamModel.UserID)
	if !ok {
		return Livestream{}, fmt.Errorf("failed to get user model by id: %d", livestreamModel.UserID)
	}
	owner, err := fillUserResponse(ctx, db, ownerModel)
	if err != nil {
		return Livestream{}, err
	}

	var livestreamTagModels []*LivestreamTagModel
	if err := db.SelectContext(ctx, &livestreamTagModels, "SELECT * FROM livestream_tags WHERE livestream_id = ?", livestreamModel.ID); err != nil {
		return Livestream{}, err
	}

	tags := make([]Tag, len(livestreamTagModels))
	var tagModels []TagModel
	for i := range livestreamTagModels {
		tagModel, ok := tagModelCache.Get(livestreamTagModels[i].TagID)
		if !ok {
			return Livestream{}, fmt.Errorf("failed to get tag: %d", livestreamTagModels[i].TagID)
		}
		tagModels = append(tagModels, tagModel)
	}

	for i := range tagModels {
		tagModel := tagModels[i]
		tags[i] = Tag{
			ID:   tagModel.ID,
			Name: tagModel.Name,
		}
	}

	livestream := Livestream{
		ID:           livestreamModel.ID,
		Owner:        owner,
		Title:        livestreamModel.Title,
		Tags:         tags,
		Description:  livestreamModel.Description,
		PlaylistUrl:  livestreamModel.PlaylistUrl,
		ThumbnailUrl: livestreamModel.ThumbnailUrl,
		StartAt:      livestreamModel.StartAt,
		EndAt:        livestreamModel.EndAt,
	}
	return livestream, nil
}

func fillLivestreamResponseBulk(ctx context.Context, db *sqlx.DB, livestreamModels []*LivestreamModel) ([]Livestream, error) {
	if len(livestreamModels) == 0 {
		return []Livestream{}, nil
	}

	livestreams := make([]Livestream, len(livestreamModels))
	var gErr error

	var ownerModels []UserModel
	livestreamIDs := make([]int64, len(livestreamModels))
	for i := range livestreamModels {
		userModel, ok := userModelByIdCache.Get(livestreamModels[i].UserID)
		if !ok {
			return nil, fmt.Errorf("failed to get user model by id: %d", livestreamModels[i].UserID)
		}
		ownerModels = append(ownerModels, userModel)

		livestreamIDs[i] = livestreamModels[i].ID
	}

	owners, err := fillUserResponseBulk(ctx, db, ownerModels)
	if err != nil {
		return nil, err
	}

	ownersMap := make(map[int64]User, len(owners))
	for i := range owners {
		ownersMap[owners[i].ID] = owners[i]
	}

	var allLivestreamTagModels []*LivestreamTagModel
	query, params, err := sqlx.In("SELECT * FROM livestream_tags WHERE livestream_id IN (?)", livestreamIDs)
	if err != nil {
		return nil, err
	}
	if err := db.SelectContext(ctx, &allLivestreamTagModels, query, params...); err != nil {
		return nil, err
	}

	livestreamTagsMap := make(map[int64][]*LivestreamTagModel, len(allLivestreamTagModels))
	for i := range allLivestreamTagModels {
		livestreamTagsModel := allLivestreamTagModels[i]
		if _, ok := livestreamTagsMap[livestreamTagsModel.LivestreamID]; !ok {
			livestreamTagsMap[livestreamTagsModel.LivestreamID] = []*LivestreamTagModel{livestreamTagsModel}
		} else {
			livestreamTagsMap[livestreamTagsModel.LivestreamID] = append(livestreamTagsMap[livestreamTagsModel.LivestreamID], livestreamTagsModel)
		}
	}

	var allTagModels []TagModel
	for i := range allLivestreamTagModels {
		tagModel, ok := tagModelCache.Get(allLivestreamTagModels[i].TagID)
		if !ok {
			gErr = fmt.Errorf("failed to get tag: %d", allLivestreamTagModels[i].TagID)
			break
		}
		allTagModels = append(allTagModels, tagModel)
	}

	tagsMap := make(map[int64]Tag, len(allTagModels))
	for i := range allTagModels {
		tagModel := allTagModels[i]
		tagsMap[tagModel.ID] = Tag{
			ID:   tagModel.ID,
			Name: tagModel.Name,
		}
	}

	for i := range livestreamModels {
		livestreamModel := livestreamModels[i]
		owner, ok := ownersMap[livestreamModel.UserID]
		if !ok {
			gErr = fmt.Errorf("failed to get owner of livestream: %d", livestreamModel.UserID)
			break
		}

		livestreamTagModels, ok := livestreamTagsMap[livestreamModel.ID]
		if !ok {
			livestreamTagModels = []*LivestreamTagModel{}
		}

		tags := make([]Tag, len(livestreamTagModels))
		for i := range livestreamTagModels {
			tags[i] = tagsMap[livestreamTagModels[i].TagID]
		}

		livestream := Livestream{
			ID:           livestreamModel.ID,
			Owner:        owner,
			Title:        livestreamModel.Title,
			Tags:         tags,
			Description:  livestreamModel.Description,
			PlaylistUrl:  livestreamModel.PlaylistUrl,
			ThumbnailUrl: livestreamModel.ThumbnailUrl,
			StartAt:      livestreamModel.StartAt,
			EndAt:        livestreamModel.EndAt,
		}

		livestreams[i] = livestream
	}

	if gErr != nil {
		return nil, gErr
	}

	return livestreams, nil
}
