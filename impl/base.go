package impl

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"gorm.io/gorm"
)

func base[T any | interface{ any }](db *gorm.DB, opts ...NewOption) *allopDB[T] {
	cfg := applyNewOpts(opts)
	a := &allopDB[T]{}
	a.j.db = db
	a.j.cfg = cfg
	a.j.redis = cfg.redisClient
	a.j.po = sync.Pool{
		New: func() any {
			s := make([]T, 0, cfg.poolCap)
			return &s
		},
	}
	return a
}

// ─── Helpers DB ───────────────────────────────────────────────────────────────

func (a *allopDB[T]) db(ctx context.Context) *gorm.DB { return a.j.db.WithContext(ctx) }
func (a *allopDB[T]) poolGet() *[]T                   { return a.j.po.Get().(*[]T) }
func (a *allopDB[T]) poolPut(s *[]T) {
	*s = (*s)[:0]
	a.j.po.Put(s)
}

func isDuplicateErr(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key") ||
		strings.Contains(msg, "duplicate entry") ||
		strings.Contains(msg, "unique constraint failed")
}

// ErrCode categoriza semanticamente os erros do repositório.
// Use errors.Is com as sentinelas públicas — nunca compare ErrCode diretamente.
type ErrCode int

const (
	ErrCodeNotFound ErrCode = iota + 1
	ErrCodeConflict
	ErrCodeValidation
	ErrCodeInternal
)

type DBError struct {
	Code    ErrCode
	Message string
	Cause   error
}

// ─── Sentinelas públicas ──────────────────────────────────────────────────────

var (
	ErrNotFound   *DBError = &DBError{Code: ErrCodeNotFound, Message: "record not found"}
	ErrConflict   *DBError = &DBError{Code: ErrCodeConflict, Message: "record already exists"}
	ErrValidation *DBError = &DBError{Code: ErrCodeValidation, Message: "validation failed"}
	ErrInternal   *DBError = &DBError{Code: ErrCodeInternal, Message: "internal error"}
)

func (e *DBError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("cqrs [%d] %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("cqrs [%d] %s", e.Code, e.Message)
}

func (e *DBError) Unwrap() error { return e.Cause }

func (e *DBError) Is(target error) bool {
	t, ok := target.(*DBError)
	return ok && e.Code == t.Code
}

// ─── Construtores internos ────────────────────────────────────────────────────

func errNotFound(cause error) error {
	return &DBError{Code: ErrCodeNotFound, Message: "record not found", Cause: cause}
}

func errConflict(cause error) error {
	return &DBError{Code: ErrCodeConflict, Message: "record already exists", Cause: cause}
}

func errValidation(msg string) error {
	return &DBError{Code: ErrCodeValidation, Message: msg}
}

func errInternal(msg string, cause error) error {
	return &DBError{Code: ErrCodeInternal, Message: msg, Cause: cause}
}
