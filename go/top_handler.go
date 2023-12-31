package main

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

type Tag struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type TagModel struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
}

type TagsResponse struct {
	Tags []*Tag `json:"tags"`
}

func getTagHandler(c echo.Context) error {
	tagModels := tagModelCache.All()
	tags := make([]*Tag, len(tagModels))
	for i := range tagModels {
		tags[i] = &Tag{
			ID:   tagModels[i].ID,
			Name: tagModels[i].Name,
		}
	}
	return c.JSON(http.StatusOK, &TagsResponse{
		Tags: tags,
	})
}

// 配信者のテーマ取得API
// GET /api/user/:username/theme
func getStreamerThemeHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		c.Logger().Printf("verifyUserSession: %+v\n", err)
		return err
	}

	username := c.Param("username")

	userModel, ok := userModelByNameCache.Get(username)
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "not found user that has the given username")
	}

	var theme Theme
	if v, ok := themeCache.Get(username); ok {
		theme = v
	} else {
		themeModel := ThemeModel{}
		if err := dbConn.GetContext(ctx, &themeModel, "SELECT * FROM themes WHERE user_id = ?", userModel.ID); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get user theme: "+err.Error())
		}

		theme = Theme{
			ID:       themeModel.ID,
			DarkMode: themeModel.DarkMode,
		}

		themeCache.Set(username, theme)
	}

	return c.JSON(http.StatusOK, theme)
}
