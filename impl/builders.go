package impl

import (
	"sync"

	"github.com/llsavelino/cqrs-core/interfaces"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// ─── allopDB ──────────────────────────────────────────────────────────────────

type allopDB[T any | interface{ any }] struct {
	j struct {
		db      *gorm.DB
		mu      sync.RWMutex
		on      sync.Once
		po      sync.Pool
		cfg     instanceCfg
		initErr error
		redis   *redis.Client
	}
}

// ─── Construtores ─────────────────────────────────────────────────────────────

func C[T any | interface{ any }](writeDB *gorm.DB, opts ...NewOption) interfaces.Command[T] {
	return base[T](writeDB, opts...)
}
func Q[T any | interface{ any }](readDB *gorm.DB, opts ...NewOption) interfaces.Query[T] {
	return base[T](readDB, opts...)
}
func I[T any | interface{ any }](db *gorm.DB, opts ...NewOption) interfaces.InfrastructureDB {
	return base[T](db, opts...)
}
func S[T any | interface{ any }](db *gorm.DB, opts ...NewOption) interfaces.CommandSpecial[T] {
	return base[T](db, opts...)
}
func R[T any | interface{ any }](db *gorm.DB, opts ...NewOption) interfaces.QuerySpecial[T] {
	return base[T](db, opts...)
}
func L[T any | interface{ any }](db *gorm.DB, opts ...NewOption) interfaces.Locked[T] {
	return base[T](db, opts...)
}
func Tx[T any | interface{ any }](tx *gorm.DB, opts ...NewOption) interfaces.Transactional[T] {
	return newTxInstance[T](tx, applyNewOpts(opts))
}
