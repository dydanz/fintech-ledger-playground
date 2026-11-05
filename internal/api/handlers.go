package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"auth-api/internal/repository"
	"auth-api/internal/service"
)

type Handlers struct {
	auth  *service.AuthService
	users *service.UserService
}

type credentialsRequest struct {
	Email    string `json:"email" binding:"required,email,max=254"`
	Password string `json:"password" binding:"required"`
}

type userResponse struct {
	ID        string    `json:"id"`
	Email     string    `json:"email"`
	FullName  string    `json:"full_name"`
	Bio       string    `json:"bio"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func toUserResponse(u repository.User) userResponse {
	return userResponse{ID: u.ID, Email: u.Email, FullName: u.FullName, Bio: u.Bio, CreatedAt: u.CreatedAt, UpdatedAt: u.UpdatedAt}
}

// POST /api/v1/auth/register
func (h *Handlers) Register(c *gin.Context) {
	var req credentialsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		abortError(c, http.StatusBadRequest, "invalid_request", "a valid email and password are required")
		return
	}
	u, err := h.auth.Register(c.Request.Context(), req.Email, req.Password)
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusCreated, toUserResponse(u))
}

type loginResponse struct {
	AccessToken string    `json:"access_token"`
	TokenType   string    `json:"token_type"`
	ExpiresIn   int       `json:"expires_in"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// POST /api/v1/auth/login
func (h *Handlers) Login(c *gin.Context) {
	var req credentialsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		abortError(c, http.StatusBadRequest, "invalid_request", "a valid email and password are required")
		return
	}
	res, err := h.auth.Login(c.Request.Context(), service.LoginInput{
		Email:     req.Email,
		Password:  req.Password,
		ClientIP:  c.ClientIP(),
		UserAgent: c.Request.UserAgent(),
	})
	if err != nil {
		h.fail(c, err)
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, loginResponse{
		AccessToken: res.Token,
		TokenType:   "Bearer",
		ExpiresIn:   int(res.ExpiresIn.Seconds()),
		ExpiresAt:   res.ExpiresAt.UTC(),
	})
}

// POST /api/v1/auth/logout
func (h *Handlers) Logout(c *gin.Context) {
	if err := h.auth.Logout(c.Request.Context(), c.GetString(ctxTokenHash), c.GetString(ctxUserID)); err != nil {
		h.fail(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// GET /api/v1/users/profile
func (h *Handlers) GetProfile(c *gin.Context) {
	u, err := h.users.GetProfile(c.Request.Context(), c.GetString(ctxUserID))
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, toUserResponse(u))
}

type profileRequest struct {
	FullName *string `json:"full_name"`
	Bio      *string `json:"bio"`
}

// PUT /api/v1/users/profile
func (h *Handlers) UpdateProfile(c *gin.Context) {
	var req profileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		abortError(c, http.StatusBadRequest, "invalid_request", "request body must be a JSON object")
		return
	}
	u, err := h.users.UpdateProfile(c.Request.Context(), c.GetString(ctxUserID),
		repository.ProfileUpdate{FullName: req.FullName, Bio: req.Bio})
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, toUserResponse(u))
}

// fail maps domain errors to HTTP responses. Unexpected errors are attached
// to the Gin context so the access log records them, and are never echoed.
func (h *Handlers) fail(c *gin.Context, err error) {
	var ve *service.ValidationError
	switch {
	case errors.As(err, &ve):
		abortError(c, http.StatusBadRequest, "invalid_request", ve.Msg)
	case errors.Is(err, service.ErrInvalidCredentials):
		abortError(c, http.StatusUnauthorized, "invalid_credentials", "invalid email or password")
	case errors.Is(err, service.ErrEmailTaken):
		abortError(c, http.StatusConflict, "email_taken", "email is already registered")
	case errors.Is(err, repository.ErrNotFound):
		abortError(c, http.StatusNotFound, "not_found", "resource not found")
	default:
		_ = c.Error(err)
		abortError(c, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}
