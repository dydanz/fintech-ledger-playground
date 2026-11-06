package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"auth-api/internal/service"
)

type Deps struct {
	Logger         *slog.Logger
	Auth           *service.AuthService
	Users          *service.UserService
	Sessions       SessionLookup
	Ready          func(ctx context.Context) error // dependency check for /readyz
	TrustedProxies []string                        // nil: ignore X-Forwarded-For
}

func NewRouter(d Deps) (*gin.Engine, error) {
	r, err := newEngine(d.Logger, d.TrustedProxies, d.Ready)
	if err != nil {
		return nil, err
	}

	h := &Handlers{auth: d.Auth, users: d.Users}
	requireAuth := Auth(d.Sessions)

	v1 := r.Group("/api/v1")
	{
		auth := v1.Group("/auth")
		auth.POST("/register", h.Register)
		auth.POST("/login", h.Login)
		auth.POST("/logout", requireAuth, h.Logout)

		users := v1.Group("/users", requireAuth)
		users.GET("/profile", h.GetProfile)
		users.PUT("/profile", h.UpdateProfile)
	}

	return r, nil
}

// newEngine builds the base every service shares: the middleware chain,
// JSON 404/405, /healthz and /readyz (ready checks the dependencies).
func newEngine(log *slog.Logger, trustedProxies []string, ready func(ctx context.Context) error) (*gin.Engine, error) {
	r := gin.New()
	r.HandleMethodNotAllowed = true
	if err := r.SetTrustedProxies(trustedProxies); err != nil {
		return nil, err
	}

	r.Use(RequestContext(log), AccessLog(), Recovery(), BodyLimit(maxBodyBytes))

	r.NoRoute(func(c *gin.Context) { abortError(c, http.StatusNotFound, "not_found", "route not found") })
	r.NoMethod(func(c *gin.Context) {
		abortError(c, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	})

	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.GET("/readyz", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		if err := ready(ctx); err != nil {
			_ = c.Error(err)
			abortError(c, http.StatusServiceUnavailable, "not_ready", "dependencies unavailable")
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})
	return r, nil
}
