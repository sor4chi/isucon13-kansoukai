package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/go-json-experiment/json"

	"github.com/google/uuid"
	"github.com/gorilla/sessions"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/v4"
	"golang.org/x/crypto/bcrypt"
)

const (
	defaultSessionIDKey      = "SESSIONID"
	defaultSessionExpiresKey = "EXPIRES"
	defaultUserIDKey         = "USERID"
	defaultUsernameKey       = "USERNAME"
	bcryptDefaultCost        = bcrypt.MinCost
)

var fallbackImage = "../img/NoImage.jpg"
var iconDir = "../img/icons/"

var fallbackImageHash = func() [32]byte {
	f, err := os.ReadFile(fallbackImage)
	if err != nil {
		panic(err)
	}
	return sha256.Sum256(f)
}()

type UserModel struct {
	ID             int64  `db:"id"`
	Name           string `db:"name"`
	DisplayName    string `db:"display_name"`
	Description    string `db:"description"`
	HashedPassword string `db:"password"`
}

type User struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	DisplayName string `json:"display_name,omitempty"`
	Description string `json:"description,omitempty"`
	Theme       Theme  `json:"theme,omitempty"`
	IconHash    string `json:"icon_hash,omitempty"`
}

type Theme struct {
	ID       int64 `json:"id"`
	DarkMode bool  `json:"dark_mode"`
}

type ThemeModel struct {
	ID       int64 `db:"id"`
	UserID   int64 `db:"user_id"`
	DarkMode bool  `db:"dark_mode"`
}

type PostUserRequest struct {
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
	Description string `json:"description"`
	// Password is non-hashed password.
	Password string               `json:"password"`
	Theme    PostUserRequestTheme `json:"theme"`
}

type PostUserRequestTheme struct {
	DarkMode bool `json:"dark_mode"`
}

type LoginRequest struct {
	Username string `json:"username"`
	// Password is non-hashed password.
	Password string `json:"password"`
}

type PostIconRequest struct {
	Image []byte `json:"image"`
}

type PostIconResponse struct {
	ID int64 `json:"id"`
}

func getIconHandler(c echo.Context) error {

	username := c.Param("username")

	if v, ok := hashCache.Get(username); ok {
		if strings.Contains(c.Request().Header.Get("If-None-Match"), fmt.Sprintf("%x", v)) {
			return c.NoContent(http.StatusNotModified)
		}
	}

	user, ok := userModelByNameCache.Get(username)
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "not found user that has the given username")
	}

	image, err := getIcon(user.ID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return c.File(fallbackImage)
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get user icon: "+err.Error())
		}
	}

	return c.Blob(http.StatusOK, "image/jpeg", image)
}

func getIcon(userId int64) ([]byte, error) {
	file, err := os.ReadFile(iconDir + fmt.Sprintf("%d.jpg", userId))
	if err != nil {
		return nil, err
	}

	return file, nil
}

func saveIcon(userId int64, image []byte) error {
	return os.WriteFile(iconDir+fmt.Sprintf("%d.jpg", userId), image, 0666)
}

func initIconDir() error {
	// remove dir
	if err := os.RemoveAll(iconDir); err != nil {
		return err
	}

	// create dir
	err := os.MkdirAll(iconDir, 0777)
	if err != nil {
		return err
	}

	return nil
}

func postIconHandler(c echo.Context) error {

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	var req *PostIconRequest
	if err := json.UnmarshalRead(c.Request().Body, &req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to decode the request body as json")
	}

	if err := saveIcon(userID, req.Image); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to save icon: "+err.Error())
	}

	user, ok := userModelByIdCache.Get(userID)
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "not found user that has the given userid")
	}

	hashCache.Delete(user.Name)

	return c.JSON(http.StatusCreated, &PostIconResponse{
		ID: randomId(),
	})
}

func randomId() int64 {
	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(err)
	}
	return int64(node.Generate())
}

func getMeHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	userModel, ok := userModelByIdCache.Get(userID)
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "not found user that has the userid in session")
	}

	user, err := fillUserResponse(ctx, dbConn, userModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill user: "+err.Error())
	}

	return c.JSON(http.StatusOK, user)
}

// ユーザ登録API
// POST /api/register
func registerHandler(c echo.Context) error {
	ctx := c.Request().Context()
	defer c.Request().Body.Close()

	req := PostUserRequest{}
	if err := json.UnmarshalRead(c.Request().Body, &req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to decode the request body as json")
	}

	if req.Name == "pipe" {
		return echo.NewHTTPError(http.StatusBadRequest, "the username 'pipe' is reserved")
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcryptDefaultCost)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to generate hashed password: "+err.Error())
	}

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	userModel := UserModel{
		Name:           req.Name,
		DisplayName:    req.DisplayName,
		Description:    req.Description,
		HashedPassword: string(hashedPassword),
	}

	result, err := tx.NamedExecContext(ctx, "INSERT INTO users (name, display_name, description, password) VALUES(:name, :display_name, :description, :password)", userModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert user: "+err.Error())
	}

	userID, err := result.LastInsertId()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get last inserted user id: "+err.Error())
	}
	userModel.ID = userID
	userModelByIdCache.Set(userModel.ID, userModel)
	userModelByNameCache.Set(userModel.Name, userModel)

	userModel.ID = userID

	themeModel := ThemeModel{
		UserID:   userID,
		DarkMode: req.Theme.DarkMode,
	}
	if _, err := tx.NamedExecContext(ctx, "INSERT INTO themes (user_id, dark_mode) VALUES(:user_id, :dark_mode)", themeModel); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert user theme: "+err.Error())
	}
	themeCache.Delete(req.Name)

	addSubdomain(req.Name + ".u.isucon.dev.")

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	user, err := fillUserResponse(ctx, dbConn, userModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill user: "+err.Error())
	}

	return c.JSON(http.StatusCreated, user)
}

// ユーザログインAPI
// POST /api/login
func loginHandler(c echo.Context) error {
	defer c.Request().Body.Close()

	req := LoginRequest{}
	if err := json.UnmarshalRead(c.Request().Body, &req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to decode the request body as json")
	}

	// usernameはUNIQUEなので、whereで一意に特定できる
	userModel, ok := userModelByNameCache.Get(req.Username)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "invalid username or password")
	}

	err := bcrypt.CompareHashAndPassword([]byte(userModel.HashedPassword), []byte(req.Password))
	if err == bcrypt.ErrMismatchedHashAndPassword {
		return echo.NewHTTPError(http.StatusUnauthorized, "invalid username or password")
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to compare hash and password: "+err.Error())
	}

	sessionEndAt := time.Now().Add(1 * time.Hour)

	sessionID := uuid.NewString()

	sess, err := session.Get(defaultSessionIDKey, c)
	if err != nil {
		return echo.NewHTTPError(http.StatusUnauthorized, "failed to get session")
	}

	sess.Options = &sessions.Options{
		Domain: "u.isucon.dev",
		MaxAge: int(60000),
		Path:   "/",
	}
	sess.Values[defaultSessionIDKey] = sessionID
	sess.Values[defaultUserIDKey] = userModel.ID
	sess.Values[defaultUsernameKey] = userModel.Name
	sess.Values[defaultSessionExpiresKey] = sessionEndAt.Unix()

	if err := sess.Save(c.Request(), c.Response()); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to save session: "+err.Error())
	}

	return c.NoContent(http.StatusOK)
}

// ユーザ詳細API
// GET /api/user/:username
func getUserHandler(c echo.Context) error {
	ctx := c.Request().Context()
	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	username := c.Param("username")

	userModel, ok := userModelByNameCache.Get(username)
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "not found user that has the given username")
	}

	user, err := fillUserResponse(ctx, dbConn, userModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill user: "+err.Error())
	}

	return c.JSON(http.StatusOK, user)
}

func verifyUserSession(c echo.Context) error {
	sess, err := session.Get(defaultSessionIDKey, c)
	if err != nil {
		return echo.NewHTTPError(http.StatusUnauthorized, "failed to get session")
	}

	sessionExpires, ok := sess.Values[defaultSessionExpiresKey]
	if !ok {
		return echo.NewHTTPError(http.StatusForbidden, "failed to get EXPIRES value from session")
	}

	_, ok = sess.Values[defaultUserIDKey].(int64)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "failed to get USERID value from session")
	}

	now := time.Now()
	if now.Unix() > sessionExpires.(int64) {
		return echo.NewHTTPError(http.StatusUnauthorized, "session has expired")
	}

	return nil
}

func fillUserResponse(ctx context.Context, db *sqlx.DB, userModel UserModel) (User, error) {
	var theme Theme
	if v, ok := themeCache.Get(userModel.Name); ok {
		theme = v
	} else {
		themeModel := ThemeModel{}
		if err := db.GetContext(ctx, &themeModel, "SELECT * FROM themes WHERE user_id = ?", userModel.ID); err != nil {
			return User{}, err
		}
		theme = Theme{
			ID:       themeModel.ID,
			DarkMode: themeModel.DarkMode,
		}
		themeCache.Set(userModel.Name, theme)
	}

	var iconHash [32]byte
	if v, ok := hashCache.Get(userModel.Name); ok {
		iconHash = v
	} else {
		if image, err := getIcon(userModel.ID); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return User{}, err
			}
			iconHash = fallbackImageHash
		} else {
			iconHash = sha256.Sum256(image)
		}
		hashCache.Set(userModel.Name, iconHash)
	}

	user := User{
		ID:          userModel.ID,
		Name:        userModel.Name,
		DisplayName: userModel.DisplayName,
		Description: userModel.Description,
		Theme:       theme,
		IconHash:    fmt.Sprintf("%x", iconHash),
	}

	return user, nil
}

func fillUserResponseBulk(ctx context.Context, db *sqlx.DB, userModels []UserModel) ([]User, error) {
	users := make([]User, 0, len(userModels))

	themeMap := make(map[int64]Theme)
	iconHashMap := make(map[int64][32]byte)
	requestThemeUserIDs := make([]int64, 0, len(userModels))
	requestIconHashUserIDs := make([]int64, 0, len(userModels))

	userModelsMap := make(map[int64]UserModel, len(userModels))

	for _, userModel := range userModels {
		userModelsMap[userModel.ID] = userModel
		if v, ok := themeCache.Get(userModel.Name); ok {
			themeMap[userModel.ID] = v
		} else {
			requestThemeUserIDs = append(requestThemeUserIDs, userModel.ID)
		}
	}

	if len(requestThemeUserIDs) > 0 {
		themeModels := []ThemeModel{}
		query, args, err := sqlx.In("SELECT * FROM themes WHERE user_id IN (?)", requestThemeUserIDs)
		if err != nil {
			return nil, err
		}
		query = db.Rebind(query)
		if err := db.SelectContext(ctx, &themeModels, query, args...); err != nil {
			return nil, err
		}

		for _, themeModel := range themeModels {
			theme := Theme{
				ID:       themeModel.ID,
				DarkMode: themeModel.DarkMode,
			}
			themeMap[themeModel.UserID] = theme
			themeCache.Set(userModelsMap[themeModel.UserID].Name, theme)
		}
	}

	for _, userModel := range userModels {
		if v, ok := hashCache.Get(userModel.Name); ok {
			iconHashMap[userModel.ID] = v
		} else {
			requestIconHashUserIDs = append(requestIconHashUserIDs, userModel.ID)
		}
	}

	if len(requestIconHashUserIDs) > 0 {
		images := make([]struct {
			UserID int64  `db:"user_id"`
			Image  []byte `db:"image"`
		}, len(requestIconHashUserIDs))
		for i := range requestIconHashUserIDs {
			image, err := getIcon(requestIconHashUserIDs[i])
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					image, err = os.ReadFile(fallbackImage)
					if err != nil {
						return nil, err
					}
				} else {
					return nil, err
				}
			}
			images[i] = struct {
				UserID int64  `db:"user_id"`
				Image  []byte `db:"image"`
			}{
				UserID: requestIconHashUserIDs[i],
				Image:  image,
			}
		}

		wg := sync.WaitGroup{}
		for _, image := range images {
			wg.Add(1)
			go func(userID int64, image []byte) {
				defer wg.Done()
				iconHashMap[userID] = sha256.Sum256(image)
			}(image.UserID, image.Image)
		}
		wg.Wait()

		for userID, iconHash := range iconHashMap {
			hashCache.Set(userModelsMap[userID].Name, iconHash)
		}
	}

	var gErr error

	for _, userModel := range userModels {
		user := User{
			ID:          userModel.ID,
			Name:        userModel.Name,
			DisplayName: userModel.DisplayName,
			Description: userModel.Description,
			Theme:       themeMap[userModel.ID],
			IconHash:    fmt.Sprintf("%x", iconHashMap[userModel.ID]),
		}

		users = append(users, user)
	}

	if gErr != nil {
		return nil, gErr
	}

	return users, nil
}
