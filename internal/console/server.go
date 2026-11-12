// Package console is ledger-console: it serves the web console's built
// React app and, from C2.2 on, the /console/api routes the app calls
// (spec/2026-11-10 Web Console Design D§4). It holds the service tokens; the
// browser never does.
package console

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"path"
	"strings"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/api"
)

// APIPrefix is the namespace of the console's own JSON API. Nothing under it
// is ever answered with the app.
const APIPrefix = "/console/api"

type Deps struct {
	Logger         *slog.Logger
	Web            fs.FS      // the built app (web/dist): index.html and assets/
	TrustedProxies []string   // nil: ignore X-Forwarded-For
	Auth           Upstream   // auth-api, for /console/api/auth/*, /profile, /password
	Ledger         Upstream   // ledger-api, for /console/api/me/*
	Demo           DemoConfig // the demo money actions on My account
	Ops            OpsConfig  // the read-only ops routes (needs Auth, Ledger and a read token)
	// AllowedHosts are the Host headers served (CONSOLE_ALLOWED_HOSTS); nil
	// serves any. Blocks DNS rebinding: a page on another site reaching
	// 127.0.0.1 still sends its own host.
	AllowedHosts []string
}

// Security headers for everything the app is served with. The app makes
// same-origin requests only, so 'self' is enough.
var securityHeaders = map[string]string{
	"Content-Security-Policy": "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; object-src 'none'",
	"X-Content-Type-Options":  "nosniff",
	"Referrer-Policy":         "no-referrer",
}

// NewRouter builds the console's HTTP handler on the shared base engine
// (request context, access log, recovery, /healthz, /readyz). /readyz checks
// that the built app is present.
func NewRouter(d Deps) (*gin.Engine, error) {
	if d.Web == nil {
		return nil, errors.New("console: Web is required")
	}
	r, err := api.NewBaseEngine(d.Logger, d.TrustedProxies, func(context.Context) error {
		_, err := fs.Stat(d.Web, "index.html")
		return err
	})
	if err != nil {
		return nil, err
	}
	if len(d.AllowedHosts) > 0 {
		r.Use(allowHosts(d.AllowedHosts))
	}
	apiGroup := r.Group(APIPrefix)
	var authProxy, ledgerProxy *proxy
	if d.Auth.BaseURL != "" {
		authProxy = newProxy(d.Auth)
		for _, rt := range authRoutes {
			apiGroup.Handle(rt.method, rt.path, authProxy.handle(rt))
		}
	}
	if d.Ledger.BaseURL != "" {
		ledgerProxy = newProxy(d.Ledger)
		for _, rt := range meRoutes {
			apiGroup.Handle(rt.method, rt.path, ledgerProxy.handle(rt))
		}
	}
	// The ops routes need a user with the ops role, so without auth-api
	// they aren't served at all.
	if authProxy != nil && ledgerProxy != nil && d.Ops.ReadToken != "" {
		o := newOps(d.Ops, ledgerProxy)
		opsGroup := apiGroup.Group("/ops", requireOps(authProxy))
		opsGroup.GET("/monitor", o.monitor)
		opsGroup.GET("/transactions", o.transactions)
		opsGroup.GET("/transactions/:id", o.transaction)
		opsGroup.GET("/accounts", o.accounts)
		opsGroup.GET("/accounts/:id", o.account)
	}
	if authProxy != nil && ledgerProxy != nil {
		dm := &demo{cfg: d.Demo, auth: authProxy, ledger: ledgerProxy}
		apiGroup.POST("/me/open", dm.open)
		apiGroup.POST("/me/credit", dm.money("credit"))
		apiGroup.POST("/me/debit", dm.money("debit"))
	}
	r.NoRoute(app(d.Web))
	return r, nil
}

// app serves the built SPA for every path no route matched:
//   - /console/api/...: a JSON 404, never the app;
//   - an existing file: that file (hashed assets/ cached for a year);
//   - a missing file (assets/ or a name with an extension): a 404, so a
//     broken build isn't hidden behind index.html;
//   - anything else: index.html, for client-side routes.
//
// Only GET and HEAD reach the app.
func app(web fs.FS) gin.HandlerFunc {
	return func(c *gin.Context) {
		p := c.Request.URL.Path
		if p == APIPrefix || strings.HasPrefix(p, APIPrefix+"/") {
			api.AbortError(c, http.StatusNotFound, "not_found", "route not found")
			return
		}
		if c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead {
			api.AbortError(c, http.StatusNotFound, "not_found", "route not found")
			return
		}
		name := strings.TrimPrefix(path.Clean("/"+p), "/") // Clean drops any ".."
		if name == "" {
			name = "index.html"
		}
		if isFile(web, name) {
			serve(c, web, name)
			return
		}
		if strings.HasPrefix(name, "assets/") || name == "assets" || path.Ext(name) != "" {
			api.AbortError(c, http.StatusNotFound, "not_found", "file not found")
			return
		}
		serve(c, web, "index.html")
	}
}

func isFile(web fs.FS, name string) bool {
	st, err := fs.Stat(web, name)
	return err == nil && st.Mode().IsRegular()
}

func serve(c *gin.Context, web fs.FS, name string) {
	h := c.Writer.Header()
	for k, v := range securityHeaders {
		h.Set(k, v)
	}
	if strings.HasPrefix(name, "assets/") {
		h.Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		h.Set("Cache-Control", "no-cache")
	}
	f, err := web.Open(name)
	if err != nil {
		_ = c.Error(err)
		api.AbortError(c, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer f.Close()
	st, err := f.Stat()
	rs, ok := f.(io.ReadSeeker)
	if err != nil || !ok {
		_ = c.Error(errors.Join(err, errors.New("console: web file is not seekable")))
		api.AbortError(c, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	// ServeContent sets Content-Type from the name, and handles HEAD and
	// conditional requests. (http.ServeFileFS would redirect /index.html.)
	http.ServeContent(c.Writer, c.Request, name, st.ModTime(), rs)
}
