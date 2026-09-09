package impl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/llsavelino/cqrs-core/interfaces"
	"gorm.io/gorm"
)

// ─── Ok ───────────────────────────────────────────────────────────────────────

func (a *allopDB[T]) Ok(entity *T) bool { return entity != nil }

// ─── Exists ───────────────────────────────────────────────────────────────────

func (a *allopDB[T]) Exists(ctx context.Context, id string) (bool, error) {
	if err := a.init(ctx); err != nil {
		return false, err
	}
	if id == "" {
		return false, errValidation("id must not be empty")
	}

	a.j.mu.RLock()
	defer a.j.mu.RUnlock()

	key := "exists:" + id
	if v, ok := a.cacheGetBool(ctx, key); ok {
		return v, nil
	}

	var count int64
	if err := a.runReadTx(ctx, func(tx *gorm.DB) error {
		return tx.Model(new(T)).Where("id = ?", id).Limit(1).Count(&count).Error
	}); err != nil {
		return false, errInternal("exists query failed", err)
	}

	res := count > 0
	_ = a.cacheSet(ctx, key, res)
	return res, nil
}

// ─── Count ────────────────────────────────────────────────────────────────────

func (a *allopDB[T]) Count(ctx context.Context, filter map[string]any) (int64, error) {
	if err := a.init(ctx); err != nil {
		return 0, err
	}

	a.j.mu.RLock()
	defer a.j.mu.RUnlock()

	key := fmt.Sprintf("count:%v", filter)
	if v, ok := a.cacheGetInt64(ctx, key); ok {
		return v, nil
	}

	var count int64
	if err := a.runReadTx(ctx, func(tx *gorm.DB) error {
		q := tx.Model(new(T))
		if len(filter) > 0 {
			q = q.Where(filter)
		}
		return q.Count(&count).Error
	}); err != nil {
		return 0, errInternal("count query failed", err)
	}

	_ = a.cacheSet(ctx, key, count)
	return count, nil
}

// ─── Find ─────────────────────────────────────────────────────────────────────

func (a *allopDB[T]) Find(ctx context.Context, id string, opts ...interfaces.QueryOpt) (*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errValidation("id must not be empty")
	}

	cfg := applyQueryOpts(opts)

	a.j.mu.RLock()
	defer a.j.mu.RUnlock()

	key := "find:" + id
	if len(cfg.Preloads) == 0 {
		if v, ok := a.cacheGetEntity(ctx, key); ok {
			return &v, nil
		}
	}

	var entity T
	if err := a.runReadTx(ctx, func(tx *gorm.DB) error {
		q := tx
		for _, rel := range cfg.Preloads {
			q = q.Preload(rel)
		}
		return q.First(&entity, "id = ?", id).Error
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errNotFound(err)
		}
		return nil, errInternal("find query failed", err)
	}

	if len(cfg.Preloads) == 0 {
		_ = a.cacheSet(ctx, key, entity)
	}
	return &entity, nil
}

// ─── FindBy ───────────────────────────────────────────────────────────────────

func (a *allopDB[T]) FindBy(ctx context.Context, field string, value any, opts ...interfaces.QueryOpt) (*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if field == "" {
		return nil, errValidation("field must not be empty")
	}

	cfg := applyQueryOpts(opts)

	a.j.mu.RLock()
	defer a.j.mu.RUnlock()

	key := fmt.Sprintf("findby:%s=%v", field, value)
	if len(cfg.Preloads) == 0 {
		if v, ok := a.cacheGetEntity(ctx, key); ok {
			return &v, nil
		}
	}

	var entity T
	if err := a.runReadTx(ctx, func(tx *gorm.DB) error {
		q := tx
		for _, rel := range cfg.Preloads {
			q = q.Preload(rel)
		}
		return q.Where(field+" = ?", value).First(&entity).Error
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errNotFound(err)
		}
		return nil, errInternal("find_by query failed", err)
	}

	if len(cfg.Preloads) == 0 {
		_ = a.cacheSet(ctx, key, entity)
	}
	return &entity, nil
}

func (a *allopDB[T]) List(ctx context.Context, filter map[string]any, opts ...interfaces.QueryOpt) ([]T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}

	cfg := applyQueryOpts(opts)

	a.j.mu.RLock()
	defer a.j.mu.RUnlock()

	doCache := len(cfg.Preloads) == 0 && cfg.Page == 1
	cacheKey := fmt.Sprintf("list:filter=%v:ps=%d", filter, cfg.PageSize)
	if doCache && a.cacheEnabled() {
		if raw, err := a.j.redis.Get(ctx, a.fullKey(cacheKey)).Bytes(); err == nil {
			var items []T
			if json.Unmarshal(raw, &items) == nil {
				return items, nil
			}
		}
	}

	buf := a.poolGet()
	defer a.poolPut(buf)

	if err := a.runReadTx(ctx, func(tx *gorm.DB) error {
		q := tx.Model(new(T))
		if len(filter) > 0 {
			q = q.Where(filter)
		}
		for _, rel := range cfg.Preloads {
			q = q.Preload(rel)
		}

		if cfg.OrderBy != "" {
			q = q.Order(cfg.OrderBy)
		}
		return q.Offset((cfg.Page - 1) * cfg.PageSize).Limit(cfg.PageSize).Find(buf).Error
	}); err != nil {
		return nil, errInternal("list query failed", err)
	}

	items := make([]T, len(*buf))
	copy(items, *buf)

	if doCache {
		_ = a.cacheSet(ctx, cacheKey, items)
	}
	return items, nil
}

// ─── ListDeleted ─────────────────────────────────────────────────────────

func (a *allopDB[T]) OnlyListDeleted(ctx context.Context, opts ...interfaces.QueryOpt) ([]T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}

	cfg := applyQueryOpts(opts)

	a.j.mu.RLock()
	defer a.j.mu.RUnlock()

	doCache := len(cfg.Preloads) == 0 && cfg.Page == 1
	cacheKey := fmt.Sprintf("listSoft:ps=%d", cfg.PageSize)
	if doCache && a.cacheEnabled() {
		if raw, err := a.j.redis.Get(ctx, a.fullKey(cacheKey)).Bytes(); err == nil {
			var items []T
			if json.Unmarshal(raw, &items) == nil {
				return items, nil
			}
		}
	}

	buf := a.poolGet()
	defer a.poolPut(buf)

	if err := a.runReadTx(ctx, func(tx *gorm.DB) error {
		q := tx.Unscoped().Model(new(T)).Where("deleted_at IS NOT NULL")
		for _, rel := range cfg.Preloads {
			q = q.Preload(rel)
		}

		if cfg.OrderBy != "" {
			q = q.Order(cfg.OrderBy)
		}
		return q.Offset((cfg.Page - 1) * cfg.PageSize).Limit(cfg.PageSize).Find(buf).Error
	}); err != nil {
		return nil, errInternal("listDeleted query failed", err)
	}

	items := make([]T, len(*buf))
	copy(items, *buf)

	if doCache {
		_ = a.cacheSet(ctx, cacheKey, items)
	}
	return items, nil
}

// ─── NoListDeleted ─────────────────────────────────────────────────────────

func (a *allopDB[T]) NoListDeleted(ctx context.Context, opts ...interfaces.QueryOpt) ([]T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}

	cfg := applyQueryOpts(opts)

	a.j.mu.RLock()
	defer a.j.mu.RUnlock()

	doCache := len(cfg.Preloads) == 0 && cfg.Page == 1
	cacheKey := fmt.Sprintf("nolistSoft:ps=%d", cfg.PageSize)
	if doCache && a.cacheEnabled() {
		if raw, err := a.j.redis.Get(ctx, a.fullKey(cacheKey)).Bytes(); err == nil {
			var items []T
			if json.Unmarshal(raw, &items) == nil {
				return items, nil
			}
		}
	}

	buf := a.poolGet()
	defer a.poolPut(buf)

	if err := a.runReadTx(ctx, func(tx *gorm.DB) error {
		q := tx.Unscoped().Model(new(T)).Where("deleted_at IS NULL")
		for _, rel := range cfg.Preloads {
			q = q.Preload(rel)
		}

		if cfg.OrderBy != "" {
			q = q.Order(cfg.OrderBy)
		}
		return q.Offset((cfg.Page - 1) * cfg.PageSize).Limit(cfg.PageSize).Find(buf).Error
	}); err != nil {
		return nil, errInternal("NolistDeleted query failed", err)
	}

	items := make([]T, len(*buf))
	copy(items, *buf)

	if doCache {
		_ = a.cacheSet(ctx, cacheKey, items)
	}
	return items, nil
}

// ─── Stream ───────────────────────────────────────────────────────────────────
//
// Todo o stream roda em uma única transação Serializable read-only,
// garantindo snapshot consistente do início ao fim da leitura.

func (a *allopDB[T]) Stream(
	ctx context.Context,
	filter map[string]any,
	batchSize int,
	fn func(batch []T) error,
) error {
	if err := a.init(ctx); err != nil {
		return err
	}
	if fn == nil {
		return errValidation("fn callback must not be nil")
	}
	if batchSize <= 0 {
		batchSize = 100
	}

	buf := a.poolGet()
	defer a.poolPut(buf)

	return a.runReadTx(ctx, func(tx *gorm.DB) error {
		offset := 0
		for {
			if ctx.Err() != nil {
				return errInternal("stream cancelled", ctx.Err())
			}

			q := tx.Model(new(T))
			if len(filter) > 0 {
				q = q.Where(filter)
			}

			if err := q.Offset(offset).Limit(batchSize).Find(buf).Error; err != nil {
				return errInternal("stream query failed", err)
			}
			if len(*buf) == 0 {
				break
			}

			batch := make([]T, len(*buf))
			copy(batch, *buf)
			*buf = (*buf)[:0]

			if err := fn(batch); err != nil {
				return err
			}

			offset += len(batch)
			if len(batch) < batchSize {
				break
			}
		}
		return nil
	})
}

// ─── Search ────────────────────────────────────────────────────────────────────

func (a *allopDB[T]) Search(ctx context.Context, id string) (*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errValidation("id must not be empty")
	}

	a.j.mu.RLock()
	defer a.j.mu.RUnlock()

	key := "search:" + id
	if v, ok := a.cacheGetEntity(ctx, key); ok {
		return &v, nil
	}

	var entity T
	if err := a.runReadTx(ctx, func(tx *gorm.DB) error {
		return tx.Unscoped().
			Where("id = ? AND deleted_at IS NOT NULL", id).
			First(&entity).Error
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errNotFound(err)
		}
		return nil, errInternal("find soft deleted failed", err)
	}

	_ = a.cacheSet(ctx, key, entity)
	return &entity, nil
}
