// Package repository holds the PostgreSQL data access layer.
package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound   = errors.New("not found")
	ErrEmailTaken = errors.New("email already registered")
)

const pgUniqueViolation = "23505"

type User struct {
	ID           string
	Email        string
	PasswordHash string
	FullName     string
	Bio          string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// ProfileUpdate holds optional profile fields; nil leaves a field unchanged.
type ProfileUpdate struct {
	FullName *string
	Bio      *string
}

type UserRepo struct {
	db *pgxpool.Pool
}

func NewUserRepo(db *pgxpool.Pool) *UserRepo { return &UserRepo{db: db} }

const userColumns = `id, email, password_hash, full_name, bio, created_at, updated_at`

func scanUser(row pgx.Row) (User, error) {
	var u User
	err := row.Scan(&u.ID, &u.Email, &u.PasswordHash, &u.FullName, &u.Bio, &u.CreatedAt, &u.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	return u, err
}

func (r *UserRepo) Create(ctx context.Context, email, passwordHash string) (User, error) {
	u, err := scanUser(r.db.QueryRow(ctx,
		`INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING `+userColumns,
		email, passwordHash))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return User{}, ErrEmailTaken
		}
		return User{}, fmt.Errorf("insert user: %w", err)
	}
	return u, nil
}

func (r *UserRepo) GetByEmail(ctx context.Context, email string) (User, error) {
	u, err := scanUser(r.db.QueryRow(ctx,
		`SELECT `+userColumns+` FROM users WHERE lower(email) = lower($1)`, email))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return User{}, fmt.Errorf("get user by email: %w", err)
	}
	return u, err
}

func (r *UserRepo) GetByID(ctx context.Context, id string) (User, error) {
	u, err := scanUser(r.db.QueryRow(ctx,
		`SELECT `+userColumns+` FROM users WHERE id = $1`, id))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return User{}, fmt.Errorf("get user by id: %w", err)
	}
	return u, err
}

func (r *UserRepo) UpdateProfile(ctx context.Context, id string, p ProfileUpdate) (User, error) {
	u, err := scanUser(r.db.QueryRow(ctx,
		`UPDATE users
		    SET full_name  = COALESCE($2, full_name),
		        bio        = COALESCE($3, bio),
		        updated_at = now()
		  WHERE id = $1
		RETURNING `+userColumns,
		id, p.FullName, p.Bio))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return User{}, fmt.Errorf("update profile: %w", err)
	}
	return u, err
}
