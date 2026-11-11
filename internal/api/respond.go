// Package api wires HTTP routing, middleware and handlers.
package api

import (
	"github.com/gin-gonic/gin"
)

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
}

// abortError writes a uniform JSON error and stops the handler chain.
func abortError(c *gin.Context, status int, code, msg string) {
	c.AbortWithStatusJSON(status, errorBody{Error: errorDetail{
		Code:      code,
		Message:   msg,
		RequestID: c.GetString(ctxRequestID),
	}})
}

// AbortError is abortError for other packages that build on NewBaseEngine
// (ledger-console), so every service answers with the same envelope.
func AbortError(c *gin.Context, status int, code, msg string) { abortError(c, status, code, msg) }
