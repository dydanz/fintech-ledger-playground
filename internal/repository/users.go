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
	Role         string // user or ops (migration 007)
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

const userColumns = `id, email, password_hash, full_name, bio, role, created_at, updated_at`

func scanUser(row pgx.Row) (User, error) {
	var u User
	err := row.Scan(&u.ID, &u.Email, &u.PasswordHash, &u.FullName, &u.Bio, &u.Role, &u.CreatedAt, &u.UpdatedAt)
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

// UpdatePasswordHash replaces the user's bcrypt hash.
// SetRole sets the role of the user with this email (any case), records the
// change in user_role_changes in the same statement, and returns the role it
// had before. by is the operator who ran it. ErrNotFound if there's no such
// user.
func (r *UserRepo) SetRole(ctx context.Context, email, role, by string) (string, User, error) {
	var previous string
	var u User
	err := r.db.QueryRow(ctx,
		`WITH old AS (SELECT id, role FROM users WHERE lower(email) = lower($1) FOR UPDATE),
		 upd AS (
		   UPDATE users SET role = $2, updated_at = now() FROM old WHERE users.id = old.id
		   RETURNING old.role AS previous, users.id, users.email, users.password_hash, users.full_name, users.bio, users.role,
		             users.created_at, users.updated_at),
		 audit AS (INSERT INTO user_role_changes (user_id, role, previous_role, changed_by) SELECT id, role, previous, $3 FROM upd)
		 SELECT previous, id, email, password_hash, full_name, bio, role, created_at, updated_at FROM upd`,
		email, role, by).Scan(&previous, &u.ID, &u.Email, &u.PasswordHash, &u.FullName, &u.Bio, &u.Role, &u.CreatedAt, &u.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", User{}, ErrNotFound
	}
	if err != nil {
		return "", User{}, fmt.Errorf("set role: %w", err)
	}
	return previous, u, nil
}

func (r *UserRepo) UpdatePasswordHash(ctx context.Context, id, hash string) error {
	tag, err := r.db.Exec(ctx,
		`UPDATE users SET password_hash = $2, updated_at = now() WHERE id = $1`, id, hash)
	if err != nil {
		return fmt.Errorf("update password hash: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
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
