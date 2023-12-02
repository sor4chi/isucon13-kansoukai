package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/v4"
)

type ReactionModel struct {
	ID           int64  `db:"id"`
	EmojiName    string `db:"emoji_name"`
	UserID       int64  `db:"user_id"`
	LivestreamID int64  `db:"livestream_id"`
	CreatedAt    int64  `db:"created_at"`
}

type Reaction struct {
	ID         int64      `json:"id"`
	EmojiName  string     `json:"emoji_name"`
	User       User       `json:"user"`
	Livestream Livestream `json:"livestream"`
	CreatedAt  int64      `json:"created_at"`
}

type PostReactionRequest struct {
	EmojiName string `json:"emoji_name"`
}

func getReactionsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	query := "SELECT * FROM reactions WHERE livestream_id = ? ORDER BY created_at DESC"
	if c.QueryParam("limit") != "" {
		limit, err := strconv.Atoi(c.QueryParam("limit"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "limit query parameter must be integer")
		}
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	reactionModels := []ReactionModel{}
	if err := dbConn.SelectContext(ctx, &reactionModels, query, livestreamID); err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "failed to get reactions")
	}

	reactions, err := fillReactionResponseBulk(ctx, dbConn, reactionModels)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill reaction: "+err.Error())
	}

	return c.JSON(http.StatusOK, reactions)
}

func postReactionHandler(c echo.Context) error {
	ctx := c.Request().Context()
	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	var req *PostReactionRequest
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to decode the request body as json")
	}

	reactionModel := ReactionModel{
		UserID:       int64(userID),
		LivestreamID: int64(livestreamID),
		EmojiName:    req.EmojiName,
		CreatedAt:    time.Now().Unix(),
	}

	result, err := dbConn.NamedExecContext(ctx, "INSERT INTO reactions (user_id, livestream_id, emoji_name, created_at) VALUES (:user_id, :livestream_id, :emoji_name, :created_at)", reactionModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert reaction: "+err.Error())
	}

	reactionID, err := result.LastInsertId()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get last inserted reaction id: "+err.Error())
	}
	reactionModel.ID = reactionID

	reaction, err := fillReactionResponse(ctx, dbConn, reactionModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill reaction: "+err.Error())
	}

	return c.JSON(http.StatusCreated, reaction)
}

func fillReactionResponse(ctx context.Context, db *sqlx.DB, reactionModel ReactionModel) (Reaction, error) {
	userModel := UserModel{}
	if err := db.GetContext(ctx, &userModel, "SELECT * FROM users WHERE id = ?", reactionModel.UserID); err != nil {
		return Reaction{}, err
	}
	user, err := fillUserResponse(ctx, db, userModel)
	if err != nil {
		return Reaction{}, err
	}

	livestreamModel := LivestreamModel{}
	if err := db.GetContext(ctx, &livestreamModel, "SELECT * FROM livestreams WHERE id = ?", reactionModel.LivestreamID); err != nil {
		return Reaction{}, err
	}
	livestream, err := fillLivestreamResponse(ctx, db, livestreamModel)
	if err != nil {
		return Reaction{}, err
	}

	reaction := Reaction{
		ID:         reactionModel.ID,
		EmojiName:  reactionModel.EmojiName,
		User:       user,
		Livestream: livestream,
		CreatedAt:  reactionModel.CreatedAt,
	}

	return reaction, nil
}

func fillReactionResponseBulk(ctx context.Context, db *sqlx.DB, reactionModels []ReactionModel) ([]Reaction, error) {
	if len(reactionModels) == 0 {
		return []Reaction{}, nil
	}
	userIDs := make([]int64, len(reactionModels))
	livestreamIDs := make([]int64, len(reactionModels))
	for i := range reactionModels {
		userIDs[i] = reactionModels[i].UserID
		livestreamIDs[i] = reactionModels[i].LivestreamID
	}

	userModels := []UserModel{}
	query, args, err := sqlx.In("SELECT * FROM users WHERE id IN (?)", userIDs)
	if err != nil {
		return nil, err
	}
	query = db.Rebind(query)
	if err := db.SelectContext(ctx, &userModels, query, args...); err != nil {
		return nil, err
	}
	users := make(map[int64]User, len(userModels))
	for i := range userModels {
		user, err := fillUserResponse(ctx, db, userModels[i])
		if err != nil {
			return nil, err
		}
		users[userModels[i].ID] = user
	}

	livestreamModels := []*LivestreamModel{}
	query, args, err = sqlx.In("SELECT * FROM livestreams WHERE id IN (?)", livestreamIDs)
	if err != nil {
		return nil, err
	}
	query = db.Rebind(query)
	if err := db.SelectContext(ctx, &livestreamModels, query, args...); err != nil {
		return nil, err
	}
	livestreams, err := fillLivestreamResponseBulk(ctx, db, livestreamModels)
	if err != nil {
		return nil, err
	}

	livestreamsMap := make(map[int64]Livestream, len(livestreams))
	for i := range livestreams {
		livestreamsMap[livestreams[i].ID] = livestreams[i]
	}

	reactions := make([]Reaction, len(reactionModels))
	for i := range reactionModels {
		reaction := Reaction{
			ID:         reactionModels[i].ID,
			EmojiName:  reactionModels[i].EmojiName,
			User:       users[reactionModels[i].UserID],
			Livestream: livestreamsMap[reactionModels[i].LivestreamID],
			CreatedAt:  reactionModels[i].CreatedAt,
		}
		reactions[i] = reaction
	}

	return reactions, nil
}
