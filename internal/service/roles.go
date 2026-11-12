package service

import (
	"context"
	"errors"
	"slices"

	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/repository"
)

// Roles a user can have. ops may read every account on the web console's ops
// screens (spec/2026-11-10 Web Console Design D§7.5).
var Roles = []string{"user", "ops"}

var ErrInvalidRole = errors.New("role must be user or ops")

// RoleStore is what RoleService needs from the user repository.
type RoleStore interface {
	SetRole(ctx context.Context, email, role string) (previous string, u repository.User, err error)
}

// RoleService grants roles. It's used by `auth-api grant-role` only: no API
// route changes a role, so nobody can grant one to themselves.
type RoleService struct {
	store RoleStore
}

func NewRoleService(store RoleStore) *RoleService { return &RoleService{store: store} }

// SetRole sets the role of the user with this email. It emits one
// user.role_change event (C1), with the user id and both roles; never the
// email (PII).
func (s *RoleService) SetRole(ctx context.Context, email, role string) (repository.User, error) {
	log := logger.FromContext(ctx)
	const event = "user.role_change"
	if !slices.Contains(Roles, role) {
		log.WarnContext(ctx, "role change refused", "event", event, "outcome", "denied", "reason", "invalid_input")
		return repository.User{}, ErrInvalidRole
	}
	previous, u, err := s.store.SetRole(ctx, email, role)
	switch {
	case errors.Is(err, repository.ErrNotFound):
		log.WarnContext(ctx, "role change refused", "event", event, "outcome", "denied", "reason", "not_found")
		return repository.User{}, err
	case err != nil:
		log.ErrorContext(ctx, "role change failed", "event", event, "outcome", "failure", "reason", "pg_error", "error", err)
		return repository.User{}, err
	}
	log.InfoContext(ctx, "role changed", "event", event, "outcome", "success", "user_id", u.ID, "role", u.Role, "previous_role", previous)
	return u, nil
}
