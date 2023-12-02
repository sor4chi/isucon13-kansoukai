package main

// ISUCON的な参考: https://github.com/isucon/isucon12-qualify/blob/main/webapp/go/isuports.go#L336
// sqlx的な参考: https://jmoiron.github.io/sqlx/

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"sync"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/labstack/echo/v4"

	"github.com/gorilla/sessions"
	"github.com/labstack/echo-contrib/session"
	echolog "github.com/labstack/gommon/log"
)

const (
	listenPort                     = 8080
	powerDNSSubdomainAddressEnvKey = "ISUCON13_POWERDNS_SUBDOMAIN_ADDRESS"
	powerDNSServerHostEnvKey       = "ISUCON13_POWERDNS_SERVER_HOST"
)

var (
	powerDNSSubdomainAddress string
	dbConn                   *sqlx.DB
	secret                   = []byte("isucon13_session_cookiestore_defaultsecret")
)

var (
	hashCache                    = NewCache[string, [32]byte]()
	themeCache                   = NewCache[string, Theme]()
	tagModelCache                = NewCache[int64, TagModel]()
	userModelByIdCache           = NewCache[int64, UserModel]()
	userModelByNameCache         = NewCache[string, UserModel]()
	livestreamModelByIdCache     = NewCache[int64, LivestreamModel]()
	livestreamModelByUserIDCache = NewCache[int64, []LivestreamModel]()
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	if secretKey, ok := os.LookupEnv("ISUCON13_SESSION_SECRETKEY"); ok {
		secret = []byte(secretKey)
	}
}

type InitializeResponse struct {
	Language string `json:"language"`
}

func connectDB(logger echo.Logger) (*sqlx.DB, error) {
	const (
		networkTypeEnvKey = "ISUCON13_MYSQL_DIALCONFIG_NET"
		addrEnvKey        = "ISUCON13_MYSQL_DIALCONFIG_ADDRESS"
		portEnvKey        = "ISUCON13_MYSQL_DIALCONFIG_PORT"
		userEnvKey        = "ISUCON13_MYSQL_DIALCONFIG_USER"
		passwordEnvKey    = "ISUCON13_MYSQL_DIALCONFIG_PASSWORD"
		dbNameEnvKey      = "ISUCON13_MYSQL_DIALCONFIG_DATABASE"
		parseTimeEnvKey   = "ISUCON13_MYSQL_DIALCONFIG_PARSETIME"
	)

	conf := mysql.NewConfig()

	// 環境変数がセットされていなかった場合でも一旦動かせるように、デフォルト値を入れておく
	// この挙動を変更して、エラーを出すようにしてもいいかもしれない
	conf.Net = "tcp"
	conf.Addr = net.JoinHostPort("127.0.0.1", "3306")
	conf.User = "isucon"
	conf.Passwd = "isucon"
	conf.DBName = "isupipe"
	conf.ParseTime = true
	conf.InterpolateParams = true

	if v, ok := os.LookupEnv(networkTypeEnvKey); ok {
		conf.Net = v
	}
	if addr, ok := os.LookupEnv(addrEnvKey); ok {
		if port, ok2 := os.LookupEnv(portEnvKey); ok2 {
			conf.Addr = net.JoinHostPort(addr, port)
		} else {
			conf.Addr = net.JoinHostPort(addr, "3306")
		}
	}
	if v, ok := os.LookupEnv(userEnvKey); ok {
		conf.User = v
	}
	if v, ok := os.LookupEnv(passwordEnvKey); ok {
		conf.Passwd = v
	}
	if v, ok := os.LookupEnv(dbNameEnvKey); ok {
		conf.DBName = v
	}
	if v, ok := os.LookupEnv(parseTimeEnvKey); ok {
		parseTime, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("failed to parse environment variable '%s' as bool: %+v", parseTimeEnvKey, err)
		}
		conf.ParseTime = parseTime
	}

	db, err := sqlx.Open("mysql", conf.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(500)

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

type IndexQuery struct {
	Table string
	Name  string
	Cols  []string
}

var IDX_QUERIES = []IndexQuery{
	{"livestream_tags", "livestream_tags_idx", []string{"tag_id", "livestream_id"}},
	{"livestream_tags", "livestream_tags_livestream_idx", []string{"livestream_id"}},
	{"icons", "icons_idx", []string{"user_id"}},
	{"ng_words", "livestream_viewers_idx", []string{"user_id", "livestream_id", "created_at DESC"}},
	{"ng_words", "livestream_viewers_middle_idx", []string{"user_id", "livestream_id"}},
	{"ng_words", "livestream_viewers_small_idx", []string{"livestream_id"}},
	{"reservation_slots", "reservation_slots_idx", []string{"start_at", "end_at"}},
	{"livestreams", "livestreams_idx", []string{"user_id"}},
	{"livecomment_reports", "livestream_id_idx", []string{"livestream_id"}},
	{"reactions", "livestream_id_idx", []string{"livestream_id", "created_at"}},
	{"reactions", "livestream_id_short_idx", []string{"livestream_id"}},
	{"livecomments", "livestream_id_idx", []string{"livestream_id"}},
	{"themes", "themes_idx", []string{"user_id"}},
}

func createIndexQueries() []string {
	qs := make([]string, 0, len(IDX_QUERIES))
	for _, idx := range IDX_QUERIES {
		qs = append(qs, fmt.Sprintf("ALTER TABLE `%s` ADD INDEX `%s` (%s)", idx.Table, idx.Name, "`"+idx.Cols[0]+"`"))
	}
	return qs
}

func initCaches() {
	hashCache.Init()
	themeCache.Init()
	tagModelCache.Init()
	userModelByIdCache.Init()
	userModelByNameCache.Init()
	livestreamModelByIdCache.Init()
	livestreamModelByUserIDCache.Init()
}

func initializeHandler(c echo.Context) error {
	resetSubdomains()
	initCaches()

	if out, err := exec.Command("../sql/init.sh").CombinedOutput(); err != nil {
		c.Logger().Warnf("init.sh failed with err=%s", string(out))
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to initialize: "+err.Error())
	}

	wg := sync.WaitGroup{}
	for _, qs := range createIndexQueries() {
		wg.Add(1)
		go func(qs string) {
			defer wg.Done()
			if _, err := dbConn.Exec(qs); err != nil {
				c.Logger().Infof("[KNOWN] ALREADY EXISTS: %s", qs)
			}
		}(qs)
	}
	wg.Wait()

	var tags []TagModel
	if err := dbConn.Select(&tags, "SELECT * FROM tags"); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get tags: "+err.Error())
	}
	for _, tag := range tags {
		tagModelCache.Set(tag.ID, tag)
	}

	var users []UserModel
	if err := dbConn.Select(&users, "SELECT * FROM users"); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get users: "+err.Error())
	}
	for _, user := range users {
		userModelByIdCache.Set(user.ID, user)
		userModelByNameCache.Set(user.Name, user)
	}

	c.Request().Header.Add("Content-Type", "application/json;charset=utf-8")
	return c.JSON(http.StatusOK, InitializeResponse{
		Language: "golang",
	})
}

func dropIndexHandler(c echo.Context) error {
	for _, idx := range IDX_QUERIES {
		if _, err := dbConn.Exec(fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`", idx.Table, idx.Name)); err != nil {
			c.Logger().Warnf("failed to drop index: %s", err.Error())
		}
	}

	return c.NoContent(http.StatusOK)
}

func main() {
	go startDNS()
	initCaches()

	e := echo.New()
	e.Debug = false
	e.Logger.SetLevel(echolog.ERROR)
	cookieStore := sessions.NewCookieStore(secret)
	cookieStore.Options.Domain = "*.u.isucon.dev"
	e.Use(session.Middleware(cookieStore))

	// 初期化
	e.POST("/api/initialize", initializeHandler)
	e.POST("/api/drop-index", dropIndexHandler)

	// top
	e.GET("/api/tag", getTagHandler)
	e.GET("/api/user/:username/theme", getStreamerThemeHandler)

	// livestream
	// reserve livestream
	e.POST("/api/livestream/reservation", reserveLivestreamHandler)
	// list livestream
	e.GET("/api/livestream/search", searchLivestreamsHandler)
	e.GET("/api/livestream", getMyLivestreamsHandler)
	e.GET("/api/user/:username/livestream", getUserLivestreamsHandler)
	// get livestream
	e.GET("/api/livestream/:livestream_id", getLivestreamHandler)
	// get polling livecomment timeline
	e.GET("/api/livestream/:livestream_id/livecomment", getLivecommentsHandler)
	// ライブコメント投稿
	e.POST("/api/livestream/:livestream_id/livecomment", postLivecommentHandler)
	e.POST("/api/livestream/:livestream_id/reaction", postReactionHandler)
	e.GET("/api/livestream/:livestream_id/reaction", getReactionsHandler)

	// (配信者向け)ライブコメントの報告一覧取得API
	e.GET("/api/livestream/:livestream_id/report", getLivecommentReportsHandler)
	e.GET("/api/livestream/:livestream_id/ngwords", getNgwords)
	// ライブコメント報告
	e.POST("/api/livestream/:livestream_id/livecomment/:livecomment_id/report", reportLivecommentHandler)
	// 配信者によるモデレーション (NGワード登録)
	e.POST("/api/livestream/:livestream_id/moderate", moderateHandler)

	// livestream_viewersにINSERTするため必要
	// ユーザ視聴開始 (viewer)
	e.POST("/api/livestream/:livestream_id/enter", enterLivestreamHandler)
	// ユーザ視聴終了 (viewer)
	e.DELETE("/api/livestream/:livestream_id/exit", exitLivestreamHandler)

	// user
	e.POST("/api/register", registerHandler)
	e.POST("/api/login", loginHandler)
	e.GET("/api/user/me", getMeHandler)
	// フロントエンドで、配信予約のコラボレーターを指定する際に必要
	e.GET("/api/user/:username", getUserHandler)
	e.GET("/api/user/:username/statistics", getUserStatisticsHandler)
	e.GET("/api/user/:username/icon", getIconHandler)
	e.POST("/api/icon", postIconHandler)

	// stats
	// ライブ配信統計情報
	e.GET("/api/livestream/:livestream_id/statistics", getLivestreamStatisticsHandler)

	// 課金情報
	e.GET("/api/payment", GetPaymentResult)

	e.HTTPErrorHandler = errorResponseHandler

	// DB接続
	conn, err := connectDB(e.Logger)
	if err != nil {
		e.Logger.Errorf("failed to connect db: %v", err)
		os.Exit(1)
	}
	defer conn.Close()
	dbConn = conn

	subdomainAddr, ok := os.LookupEnv(powerDNSSubdomainAddressEnvKey)
	if !ok {
		e.Logger.Errorf("environ %s must be provided", powerDNSSubdomainAddressEnvKey)
		os.Exit(1)
	}
	powerDNSSubdomainAddress = subdomainAddr

	// HTTPサーバ起動
	listenAddr := net.JoinHostPort("0.0.0.0", strconv.Itoa(listenPort))
	if err := e.Start(listenAddr); err != nil {
		e.Logger.Errorf("failed to start HTTP server: %v", err)
		os.Exit(1)
	}
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func errorResponseHandler(err error, c echo.Context) {
	c.Logger().Errorf("error at %s: %+v", c.Path(), err)
	if he, ok := err.(*echo.HTTPError); ok {
		if e := c.JSON(he.Code, &ErrorResponse{Error: err.Error()}); e != nil {
			c.Logger().Errorf("%+v", e)
		}
		return
	}

	if e := c.JSON(http.StatusInternalServerError, &ErrorResponse{Error: err.Error()}); e != nil {
		c.Logger().Errorf("%+v", e)
	}
}
