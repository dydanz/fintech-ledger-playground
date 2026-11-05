package service

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"auth-api/internal/repository"
)

const (
	maxFullName = 100
	maxBio      = 500
)

type UserService struct {
	users UserStore
}

func NewUserService(users UserStore) *UserService { return &UserService{users: users} }

func (s *UserService) GetProfile(ctx context.Context, userID string) (repository.User, error) {
	return s.users.GetByID(ctx, userID)
}

func (s *UserService) UpdateProfile(ctx context.Context, userID string, p repository.ProfileUpdate) (repository.User, error) {
	if p.FullName == nil && p.Bio == nil {
		return repository.User{}, &ValidationError{Msg: "at least one of full_name or bio is required"}
	}
	if p.FullName != nil {
		name := strings.TrimSpace(*p.FullName)
		if utf8.RuneCountInString(name) > maxFullName {
			return repository.User{}, &ValidationError{Msg: fmt.Sprintf("full_name must be at most %d characters", maxFullName)}
		}
		p.FullName = &name
	}
	if p.Bio != nil && utf8.RuneCountInString(*p.Bio) > maxBio {
		return repository.User{}, &ValidationError{Msg: fmt.Sprintf("bio must be at most %d characters", maxBio)}
	}
	return s.users.UpdateProfile(ctx, userID, p)
}
