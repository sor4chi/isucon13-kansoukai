package main

import (
	"context"
	"database/sql"

	"github.com/go-json-experiment/json"

	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/v4"
)

type PostLivecommentRequest struct {
	Comment string `json:"comment"`
	Tip     int64  `json:"tip"`
}

type LivecommentModel struct {
	ID           int64  `db:"id"`
	UserID       int64  `db:"user_id"`
	LivestreamID int64  `db:"livestream_id"`
	Comment      string `db:"comment"`
	Tip          int64  `db:"tip"`
	CreatedAt    int64  `db:"created_at"`
}

type Livecomment struct {
	ID         int64      `json:"id"`
	User       User       `json:"user"`
	Livestream Livestream `json:"livestream"`
	Comment    string     `json:"comment"`
	Tip        int64      `json:"tip"`
	CreatedAt  int64      `json:"created_at"`
}

type LivecommentReport struct {
	ID          int64       `json:"id"`
	Reporter    User        `json:"reporter"`
	Livecomment Livecomment `json:"livecomment"`
	CreatedAt   int64       `json:"created_at"`
}

type LivecommentReportModel struct {
	ID            int64 `db:"id"`
	UserID        int64 `db:"user_id"`
	LivestreamID  int64 `db:"livestream_id"`
	LivecommentID int64 `db:"livecomment_id"`
	CreatedAt     int64 `db:"created_at"`
}

type ModerateRequest struct {
	NGWord string `json:"ng_word"`
}

type NGWord struct {
	ID           int64  `json:"id" db:"id"`
	UserID       int64  `json:"user_id" db:"user_id"`
	LivestreamID int64  `json:"livestream_id" db:"livestream_id"`
	Word         string `json:"word" db:"word"`
	CreatedAt    int64  `json:"created_at" db:"created_at"`
}

func getLivecommentsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	query := "SELECT * FROM livecomments WHERE livestream_id = ? ORDER BY created_at DESC"
	if c.QueryParam("limit") != "" {
		limit, err := strconv.Atoi(c.QueryParam("limit"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "limit query parameter must be integer")
		}
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	livecommentModels := []LivecommentModel{}
	err = dbConn.SelectContext(ctx, &livecommentModels, query, livestreamID)
	if errors.Is(err, sql.ErrNoRows) {
		return c.JSON(http.StatusOK, []*Livecomment{})
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livecomments: "+err.Error())
	}

	livecomments, err := fillLivecommentResponseBulk(ctx, dbConn, livecommentModels)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fil livecomments: "+err.Error())
	}

	return c.JSON(http.StatusOK, livecomments)
}

func getNgwords(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
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

	var ngWords []*NGWord
	if err := dbConn.SelectContext(ctx, &ngWords, "SELECT * FROM ng_words WHERE user_id = ? AND livestream_id = ? ORDER BY created_at DESC", userID, livestreamID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return c.JSON(http.StatusOK, []*NGWord{})
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get NG words: "+err.Error())
		}
	}

	return c.JSON(http.StatusOK, ngWords)
}

func postLivecommentHandler(c echo.Context) error {
	ctx := c.Request().Context()
	defer c.Request().Body.Close()

	if err := verifyUserSession(c); err != nil {
		return err
	}

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	var req *PostLivecommentRequest
	if err := json.UnmarshalRead(c.Request().Body, &req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to decode the request body as json")
	}

	livestreamModel, ok := livestreamModelByIdCache.Get(int64(livestreamID))
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "livestream not found")
	}

	// スパム判定
	var ngwords []*NGWord
	if err := dbConn.SelectContext(ctx, &ngwords, "SELECT id, user_id, livestream_id, word FROM ng_words WHERE user_id = ? AND livestream_id = ?", livestreamModel.UserID, livestreamModel.ID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get NG words: "+err.Error())
	}

	var hitSpam int
	for _, ngword := range ngwords {
		if strings.Contains(req.Comment, ngword.Word) {
			hitSpam++
		}

		if hitSpam >= 1 {
			return echo.NewHTTPError(http.StatusBadRequest, "このコメントがスパム判定されました")
		}
	}

	now := time.Now().Unix()
	livecommentModel := LivecommentModel{
		UserID:       userID,
		LivestreamID: int64(livestreamID),
		Comment:      req.Comment,
		Tip:          req.Tip,
		CreatedAt:    now,
	}

	rs, err := dbConn.NamedExecContext(ctx, "INSERT INTO livecomments (user_id, livestream_id, comment, tip, created_at) VALUES (:user_id, :livestream_id, :comment, :tip, :created_at)", livecommentModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert livecomment: "+err.Error())
	}

	livecommentID, err := rs.LastInsertId()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get last inserted livecomment id: "+err.Error())
	}
	livecommentModel.ID = livecommentID

	livecomment, err := fillLivecommentResponse(ctx, dbConn, livecommentModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill livecomment: "+err.Error())
	}

	return c.JSON(http.StatusCreated, livecomment)
}

func reportLivecommentHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		return err
	}

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	livecommentID, err := strconv.Atoi(c.Param("livecomment_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livecomment_id in path must be integer")
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	now := time.Now().Unix()
	reportModel := LivecommentReportModel{
		UserID:        int64(userID),
		LivestreamID:  int64(livestreamID),
		LivecommentID: int64(livecommentID),
		CreatedAt:     now,
	}
	rs, err := dbConn.NamedExecContext(ctx, "INSERT INTO livecomment_reports(user_id, livestream_id, livecomment_id, created_at) VALUES (:user_id, :livestream_id, :livecomment_id, :created_at)", &reportModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert livecomment report: "+err.Error())
	}
	reportID, err := rs.LastInsertId()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get last inserted livecomment report id: "+err.Error())
	}
	reportModel.ID = reportID

	report, err := fillLivecommentReportResponse(ctx, dbConn, reportModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill livecomment report: "+err.Error())
	}

	return c.JSON(http.StatusCreated, report)
}

// NGワードを登録
func moderateHandler(c echo.Context) error {
	ctx := c.Request().Context()
	defer c.Request().Body.Close()

	if err := verifyUserSession(c); err != nil {
		return err
	}

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	var req *ModerateRequest
	if err := json.UnmarshalRead(c.Request().Body, &req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to decode the request body as json")
	}

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	// 配信者自身の配信に対するmoderateなのかを検証
	_, ok := livestreamModelByIdCache.Get(int64(livestreamID))
	if !ok {
		return echo.NewHTTPError(http.StatusBadRequest, "A streamer can't moderate livestreams that other streamers own")
	}

	rs, err := tx.NamedExecContext(ctx, "INSERT INTO ng_words(user_id, livestream_id, word, created_at) VALUES (:user_id, :livestream_id, :word, :created_at)", &NGWord{
		UserID:       int64(userID),
		LivestreamID: int64(livestreamID),
		Word:         req.NGWord,
		CreatedAt:    time.Now().Unix(),
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert new NG word: "+err.Error())
	}

	wordID, err := rs.LastInsertId()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get last inserted NG word id: "+err.Error())
	}

	var ngwords []*NGWord
	if err := tx.SelectContext(ctx, &ngwords, "SELECT * FROM ng_words WHERE livestream_id = ?", livestreamID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get NG words: "+err.Error())
	}

	// NGワードを含むlivecommentsを1クエリですべて削除する
	query := `
	DELETE FROM livecomments WHERE livestream_id = ? AND
	`
	for i, ngword := range ngwords {
		if i == 0 {
			query += fmt.Sprintf("comment LIKE '%%%s%%'", ngword.Word)
		} else {
			query += fmt.Sprintf(" OR comment LIKE '%%%s%%'", ngword.Word)
		}
	}
	if _, err := tx.ExecContext(ctx, query, livestreamID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to delete old livecomments that hit spams: "+err.Error())
	}

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	return c.JSON(http.StatusCreated, map[string]interface{}{
		"word_id": wordID,
	})
}

func fillLivecommentResponse(ctx context.Context, db *sqlx.DB, livecommentModel LivecommentModel) (Livecomment, error) {
	commentOwnerModel, ok := userModelByIdCache.Get(livecommentModel.UserID)
	if !ok {
		return Livecomment{}, fmt.Errorf("failed to get user model by id: %d", livecommentModel.UserID)
	}
	commentOwner, err := fillUserResponse(ctx, db, commentOwnerModel)
	if err != nil {
		return Livecomment{}, err
	}

	livestreamModel, ok := livestreamModelByIdCache.Get(livecommentModel.LivestreamID)
	if !ok {
		return Livecomment{}, fmt.Errorf("failed to get livestream model by id: %d", livecommentModel.LivestreamID)
	}

	livestream, err := fillLivestreamResponse(ctx, db, livestreamModel)
	if err != nil {
		return Livecomment{}, err
	}

	livecomment := Livecomment{
		ID:         livecommentModel.ID,
		User:       commentOwner,
		Livestream: livestream,
		Comment:    livecommentModel.Comment,
		Tip:        livecommentModel.Tip,
		CreatedAt:  livecommentModel.CreatedAt,
	}

	return livecomment, nil
}

func fillLivecommentResponseBulk(ctx context.Context, db *sqlx.DB, livecommentModels []LivecommentModel) ([]Livecomment, error) {
	if len(livecommentModels) == 0 {
		return []Livecomment{}, nil
	}

	livestreamIDs := make([]int64, len(livecommentModels))

	var userModels []UserModel

	for i := range livecommentModels {
		userModel, ok := userModelByIdCache.Get(livecommentModels[i].UserID)
		if !ok {
			return []Livecomment{}, fmt.Errorf("failed to get user model by id: %d", livecommentModels[i].UserID)
		}
		userModels = append(userModels, userModel)
		livestreamIDs[i] = livecommentModels[i].LivestreamID
	}

	commentOwners, err := fillUserResponseBulk(ctx, db, userModels)
	if err != nil {
		return []Livecomment{}, err
	}

	commentOwnersMap := make(map[int64]User, len(commentOwners))
	for i := range commentOwners {
		commentOwnersMap[commentOwners[i].ID] = commentOwners[i]
	}

	livestreamModels := []*LivestreamModel{}
	for _, livestreamID := range livestreamIDs {
		livestreamModel, ok := livestreamModelByIdCache.Get(livestreamID)
		if !ok {
			return []Livecomment{}, fmt.Errorf("failed to get livestream model by id: %d", livestreamID)
		}
		livestreamModels = append(livestreamModels, &livestreamModel)
	}

	livestreams, err := fillLivestreamResponseBulk(ctx, db, livestreamModels)
	if err != nil {
		return []Livecomment{}, err
	}

	livestreamMap := make(map[int64]Livestream)
	for _, livestream := range livestreams {
		livestreamMap[livestream.ID] = livestream
	}

	livecomments := make([]Livecomment, len(livecommentModels))
	for i := range livecommentModels {
		livecomments[i] = Livecomment{
			ID:         livecommentModels[i].ID,
			User:       commentOwnersMap[livecommentModels[i].UserID],
			Livestream: livestreamMap[livecommentModels[i].LivestreamID],
			Comment:    livecommentModels[i].Comment,
			Tip:        livecommentModels[i].Tip,
			CreatedAt:  livecommentModels[i].CreatedAt,
		}
	}

	return livecomments, nil
}

func fillLivecommentReportResponse(ctx context.Context, db *sqlx.DB, reportModel LivecommentReportModel) (LivecommentReport, error) {
	reporterModel, ok := userModelByIdCache.Get(reportModel.UserID)
	if !ok {
		return LivecommentReport{}, fmt.Errorf("failed to get user model by id: %d", reportModel.UserID)
	}
	reporter, err := fillUserResponse(ctx, db, reporterModel)
	if err != nil {
		return LivecommentReport{}, err
	}

	livecommentModel := LivecommentModel{}
	if err := db.GetContext(ctx, &livecommentModel, "SELECT * FROM livecomments WHERE id = ?", reportModel.LivecommentID); err != nil {
		return LivecommentReport{}, err
	}
	livecomment, err := fillLivecommentResponse(ctx, db, livecommentModel)
	if err != nil {
		return LivecommentReport{}, err
	}

	report := LivecommentReport{
		ID:          reportModel.ID,
		Reporter:    reporter,
		Livecomment: livecomment,
		CreatedAt:   reportModel.CreatedAt,
	}
	return report, nil
}

func fillLivecommentReportResponseBulk(ctx context.Context, db *sqlx.DB, reportModels []LivecommentReportModel) ([]LivecommentReport, error) {
	if len(reportModels) == 0 {
		return []LivecommentReport{}, nil
	}

	var userModels []UserModel
	livecommentIDs := make([]int64, len(reportModels))

	for i := range reportModels {
		userModel, ok := userModelByIdCache.Get(reportModels[i].UserID)
		if !ok {
			return []LivecommentReport{}, fmt.Errorf("failed to get user model by id: %d", reportModels[i].UserID)
		}
		userModels = append(userModels, userModel)
		livecommentIDs[i] = reportModels[i].LivecommentID
	}

	livecommentModels := []LivecommentModel{}
	query, args, err := sqlx.In("SELECT * FROM livecomments WHERE id IN (?)", livecommentIDs)
	if err != nil {
		return []LivecommentReport{}, err
	}
	query = db.Rebind(query)
	if err := db.SelectContext(ctx, &livecommentModels, query, args...); err != nil {
		return []LivecommentReport{}, err
	}

	reporters, err := fillUserResponseBulk(ctx, db, userModels)
	if err != nil {
		return []LivecommentReport{}, err
	}

	reportersMap := make(map[int64]User, len(reporters))
	for i := range reporters {
		reportersMap[reporters[i].ID] = reporters[i]
	}

	livecomments, err := fillLivecommentResponseBulk(ctx, db, livecommentModels)
	if err != nil {
		return []LivecommentReport{}, err
	}

	livecommentsMap := make(map[int64]Livecomment, len(livecomments))
	for i := range livecomments {
		livecommentsMap[livecomments[i].ID] = livecomments[i]
	}

	reports := make([]LivecommentReport, len(reportModels))
	for i := range reportModels {
		reports[i] = LivecommentReport{
			ID:          reportModels[i].ID,
			Reporter:    reportersMap[reportModels[i].UserID],
			Livecomment: livecommentsMap[reportModels[i].LivecommentID],
			CreatedAt:   reportModels[i].CreatedAt,
		}
	}

	return reports, nil
}
