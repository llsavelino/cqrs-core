package impl

import (
	"context"
	"database/sql"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/llsavelino/cqrs-core/interfaces"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ─── Ping ─────────────────────────────────────────────────────────────────────

func (a *allopDB[T]) Ping(ctx context.Context) error {
	sqlDB, err := a.j.db.DB()
	if err != nil {
		return errInternal("failed to obtain sql.DB", err)
	}
	if err = sqlDB.PingContext(ctx); err != nil {
		return errInternal("ping failed", err)
	}
	return nil
}

// ─── Pong ─────────────────────────────────────────────────────────────────────

func (a *allopDB[T]) Pong(ctx context.Context) error {
	return a.Ping(ctx)
}

// Configurações de resiliência para o retry loop
const (
	maxRetries    = 5
	baseBackoffMs = 10
	maxBackoffMs  = 100
)

type txCommandRepo[T any] struct {
	*allopDB[T]
	mu *sync.RWMutex
}

type txQueryRepo[T any] struct {
	*allopDB[T]
	mu *sync.RWMutex
}

// Tx executa fn dentro de uma transação atômica com Retry automático para Serializable.
func (a *allopDB[T]) Tx(ctx context.Context, fn func(cmd interfaces.Command[T]) error) error {
	if err := a.init(ctx); err != nil {
		return err
	}

	return a.runWithRetry(ctx, sql.LevelSerializable, false, func(tx *gorm.DB) error {
		txInst := newTxInstance[T](tx, a.j.cfg)
		txCmd := &txCommandRepo[T]{allopDB: txInst, mu: &a.j.mu}
		return fn(txCmd)
	})
}

// TxSpecial executa operações especiais com Retry automático para Serializable.
func (a *allopDB[T]) TxSpecial(ctx context.Context, fn func(cmd interfaces.CommandSpecial[T]) error) error {
	if err := a.init(ctx); err != nil {
		return err
	}

	return a.runWithRetry(ctx, sql.LevelSerializable, false, func(tx *gorm.DB) error {
		txInst := newTxInstance[T](tx, a.j.cfg)
		txCmd := &txCommandRepo[T]{allopDB: txInst, mu: &a.j.mu}
		return fn(txCmd)
	})
}

// TxQuery executa uma função de leitura com isolamento Serializable e ReadOnly.
func (a *allopDB[T]) TxQuery(ctx context.Context, fn func(qry interfaces.Query[T]) error) error {
	if err := a.init(ctx); err != nil {
		return err
	}

	return a.runWithRetry(ctx, sql.LevelSerializable, true, func(tx *gorm.DB) error {
		txInst := newTxInstance[T](tx, a.j.cfg)
		txQry := &txQueryRepo[T]{allopDB: txInst, mu: &a.j.mu}
		return fn(txQry)
	})
}

// TxQuerySpecial executa leituras especiais com isolamento Serializable e ReadOnly.
func (a *allopDB[T]) TxQuerySpecial(ctx context.Context, fn func(qry interfaces.QuerySpecial[T]) error) error {
	if err := a.init(ctx); err != nil {
		return err
	}

	return a.runWithRetry(ctx, sql.LevelSerializable, true, func(tx *gorm.DB) error {
		txInst := newTxInstance[T](tx, a.j.cfg)
		txQry := &txQueryRepo[T]{allopDB: txInst, mu: &a.j.mu}
		return fn(txQry)
	})
}

// ── MOTOR DE RESILIÊNCIA INTERNO ──────────────────────────────────────────────────

// runWithRetry encapsula a lógica de execução da transação e aplica retries se o Postgres abortar por concorrência.
func (a *allopDB[T]) runWithRetry(ctx context.Context, isoLevel sql.IsolationLevel, readOnly bool, txFn func(tx *gorm.DB) error) error {
	opts := &sql.TxOptions{
		Isolation: isoLevel,
		ReadOnly:  readOnly,
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		err := a.db(ctx).Transaction(func(tx *gorm.DB) error {
			return txFn(tx)
		}, opts)

		if err == nil {
			return nil // Sucesso absoluto, sai do loop
		}

		// Se o contexto foi cancelado ou estourou timeout, não faz sentido tentar de novo
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return err
		}

		// Verifica se o erro foi uma falha de serialização do Postgres (Error Code: 40001)
		if isSerializationFailure(err) {
			// Aplica um backoff exponencial com jitter (aleatoriedade) para evitar que as threads colidam de novo no mesmo milissegundo
			backoff := time.Duration(baseBackoffMs<<attempt)*time.Millisecond + time.Duration(rand.Intn(10))*time.Millisecond
			if backoff > time.Duration(maxBackoffMs)*time.Millisecond {
				backoff = time.Duration(maxBackoffMs) * time.Millisecond
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				// Aguarda o tempo do backoff e continua para a próxima iteração do loop (Retry)
				continue
			}
		}

		// Se for qualquer outro erro (erro de sintaxe, validação de negócio, etc), crasha imediatamente sem dar retry
		return err
	}

	return errors.New("transaction failed after max serialization retries")
}

// ─── Transações Serializáveis ─────────────────────────────────────────────────
// PostgreSQL usa Serializable Snapshot Isolation (SSI).
// Quando duas transações concorrentes conflitam, o PostgreSQL rejeita uma
// com o código 40001 (serialization_failure). O retry é obrigatório.

// isSerializationFailure detecta falha de serialização do PostgreSQL (40001).
func isSerializationFailure(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40001" || pgErr.Code == "40P01"
	}
	var pgerr interface{ SQLState() string }
	if errors.As(err, &pgerr) {
		return pgerr.SQLState() == "40001"
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "40001")
}

// withTimeout aplica timeout configurado ao ctx se ele ainda não tiver deadline.
func (a *allopDB[T]) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if a.j.cfg.timeout > 0 {
		if _, ok := ctx.Deadline(); !ok {
			return context.WithTimeout(ctx, time.Duration(a.j.cfg.timeout)*time.Millisecond)
		}
	}
	return ctx, func() {}
}

// runSerializableTx executa fn em uma transação Serializable com retry automático
// em caso de serialization failure (40001).
//
// Usado em todos os Commands (escritas) e em Lock (SELECT FOR UPDATE).
// Cache-Aside: o caller invalida o cache APÓS esta função retornar nil.
//
// Backoff exponencial: 50ms, 100ms, 200ms, 400ms, 800ms.
func (a *allopDB[T]) runSerializableTx(ctx context.Context, fn func(tx *gorm.DB) error) error {
	ctx, cancel := a.withTimeout(ctx)
	defer cancel()

	const maxAttempts = 5
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := a.db(ctx).Transaction(fn, &sql.TxOptions{
			Isolation: sql.LevelSerializable,
		})
		if err == nil {
			return nil
		}

		if isSerializationFailure(err) && attempt < maxAttempts-1 {
			baseBackoff := time.Duration(50*(1<<attempt)) * time.Millisecond
			jitter := time.Duration(rand.Int63n(baseBackoff.Milliseconds())) * time.Millisecond
			backoff := baseBackoff + jitter
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		return err
	}
	return nil
}

// runReadTx executa fn em uma transação Serializable somente-leitura.
//
// Usado em todos os Queries (leituras) exceto Lock.
// ReadOnly: o PostgreSQL garante snapshot consistente sem adquirir locks de escrita,
// o que reduz contention em relação a uma tx Serializable de leitura/escrita.
func (a *allopDB[T]) runReadTx(ctx context.Context, fn func(tx *gorm.DB) error) error {
	ctx, cancel := a.withTimeout(ctx)
	defer cancel()

	return a.db(ctx).Transaction(fn, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  true,
	})
}

// ─── newTxInstance ────────────────────────────────────────────────────────────

func newTxInstance[T any](tx *gorm.DB, cfg instanceCfg) *allopDB[T] {
	inst := &allopDB[T]{}
	inst.j.db = tx
	inst.j.cfg = cfg
	inst.j.redis = cfg.redisClient
	inst.j.po = sync.Pool{
		New: func() any {
			s := make([]T, 0, cfg.poolCap)
			return &s
		},
	}
	inst.j.on.Do(func() {})
	return inst
}

// ─── LockReadOnly ─────────────────────────────────────────────────────────────

// LockReadOnly usa SELECT FOR SHARE: permite leituras concorrentes,
// mas bloqueia qualquer tentativa de escrita/atualização no registro
// enquanto a transação estiver aberta.
func (a *allopDB[T]) LockReadOnly(ctx context.Context, id string) (*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errValidation("id must not be empty")
	}

	a.j.mu.RLock()
	defer a.j.mu.RUnlock()

	var entity T
	if err := a.runSerializableTx(ctx, func(tx *gorm.DB) error {
		return tx.Clauses(clause.Locking{Strength: "SHARE"}).
			First(&entity, "id = ?", id).Error
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errNotFound(err)
		}
		return nil, errInternal("lock read-only query failed", err)
	}

	return &entity, nil
}

// ─── LockWriteOnly ────────────────────────────────────────────────────────────

// LockWriteOnly usa SELECT FOR NO KEY UPDATE: lock de escrita mais leve
// que FOR UPDATE — não bloqueia SELECTs FOR SHARE nem foreign-key checks,
// adequado quando você vai escrever mas não alterar a chave primária/FK.
func (a *allopDB[T]) LockWriteOnly(ctx context.Context, id string) (*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errValidation("id must not be empty")
	}

	a.j.mu.RLock()
	defer a.j.mu.RUnlock()

	var entity T
	if err := a.runSerializableTx(ctx, func(tx *gorm.DB) error {
		return tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&entity, "id = ?", id).Error
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errNotFound(err)
		}
		return nil, errInternal("lock write-only query failed", err)
	}

	return &entity, nil
}
