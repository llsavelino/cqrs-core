package impl

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ─── Insert ───────────────────────────────────────────────────────────────────

func (a *allopDB[T]) Insert(ctx context.Context, entity *T) (*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if entity == nil {
		return nil, errValidation("entity must not be nil")
	}

	a.j.mu.Lock()
	defer a.j.mu.Unlock()

	if err := a.runSerializableTx(ctx, func(tx *gorm.DB) error {
		if err := tx.Create(entity).Error; err != nil {
			return err
		}
		if err := a.invalidateAfterWrite(ctx, extractID(entity)); err != nil {
			return fmt.Errorf("cache invalidation failed: %w", err)
		}
		return nil
	}); err != nil {
		if isDuplicateErr(err) {
			return nil, errConflict(err)
		}
		return nil, errInternal("insert failed", err)
	}

	return entity, nil
}

// InsertBatch insere múltiplas entidades em uma única transação serializável.
func (a *allopDB[T]) InsertBatch(ctx context.Context, entities []*T) ([]*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if len(entities) == 0 {
		return nil, errValidation("entities must not be empty")
	}

	a.j.mu.Lock()
	defer a.j.mu.Unlock()

	if err := a.runSerializableTx(ctx, func(tx *gorm.DB) error {
		if err := tx.Create(&entities).Error; err != nil {
			return err
		}
		ids := make([]string, 0, len(entities))
		for _, e := range entities {
			ids = append(ids, extractID(e))
		}
		if err := a.invalidateAfterWrite(ctx, ids...); err != nil {
			return fmt.Errorf("cache invalidation failed: %w", err)
		}
		return nil
	}); err != nil {
		if isDuplicateErr(err) {
			return nil, errConflict(err)
		}
		return nil, errInternal("insert batch failed", err)
	}

	return entities, nil
}

// ─── Update ───────────────────────────────────────────────────────────────────

func (a *allopDB[T]) Update(ctx context.Context, entity *T) (*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if entity == nil {
		return nil, errValidation("entity must not be nil")
	}

	a.j.mu.Lock()
	defer a.j.mu.Unlock()

	if err := a.runSerializableTx(ctx, func(tx *gorm.DB) error {
		if err := tx.Model(entity).Select("id").First(new(T)).Error; err != nil {
			return err
		}
		if err := tx.Model(entity).Select("*").Omit("id").Save(entity).Error; err != nil {
			return err
		}
		if err := a.invalidateAfterWrite(ctx, extractID(entity)); err != nil {
			return fmt.Errorf("cache invalidation failed: %w", err)
		}
		return nil
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errNotFound(err)
		}
		return nil, errInternal("update failed", err)
	}

	return entity, nil
}

// ─── Replace ──────────────────────────────────────────────────────────────────

func (a *allopDB[T]) Replace(ctx context.Context, id string, entity *T) (*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errValidation("id must not be empty")
	}
	if entity == nil {
		return nil, errValidation("entity must not be nil")
	}

	a.j.mu.Lock()
	defer a.j.mu.Unlock()

	var existing T
	if err := a.runSerializableTx(ctx, func(tx *gorm.DB) error {
		if err := tx.First(&existing, "id = ?", id).Error; err != nil {
			return err
		}
		if err := tx.Model(&existing).Select("*").Omit("id").Updates(entity).Error; err != nil {
			return err
		}
		if err := a.invalidateAfterWrite(ctx, id); err != nil {
			return fmt.Errorf("cache invalidation failed: %w", err)
		}
		return nil
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errNotFound(err)
		}
		return nil, errInternal("replace failed", err)
	}

	return &existing, nil
}

// ─── Patch ────────────────────────────────────────────────────────────────────

func (a *allopDB[T]) Patch(ctx context.Context, id string, fields map[string]any) (*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errValidation("id must not be empty")
	}
	if len(fields) == 0 {
		return nil, errValidation("at least one field must be provided")
	}

	a.j.mu.Lock()
	defer a.j.mu.Unlock()

	var entity T
	if err := a.runSerializableTx(ctx, func(tx *gorm.DB) error {
		if err := tx.First(&entity, "id = ?", id).Error; err != nil {
			return err
		}
		if err := tx.Model(&entity).Updates(fields).Error; err != nil {
			return err
		}
		if err := a.invalidateAfterWrite(ctx, id); err != nil {
			return fmt.Errorf("cache invalidation failed: %w", err)
		}
		return nil
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errNotFound(err)
		}
		return nil, errInternal("patch failed", err)
	}

	return &entity, nil
}

// ─── Upsert ───────────────────────────────────────────────────────────────────

func (a *allopDB[T]) Upsert(ctx context.Context, entity *T, conflictKeys []string) (*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if entity == nil {
		return nil, errValidation("entity must not be nil")
	}
	if len(conflictKeys) == 0 {
		return nil, errValidation("at least one conflict key must be provided")
	}

	cols := make([]clause.Column, len(conflictKeys))
	for i, k := range conflictKeys {
		cols[i] = clause.Column{Name: k}
	}

	a.j.mu.Lock()
	defer a.j.mu.Unlock()

	if err := a.runSerializableTx(ctx, func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{Columns: cols, UpdateAll: true}).Create(entity).Error; err != nil {
			return err
		}
		if err := a.invalidateAfterWrite(ctx, extractID(entity)); err != nil {
			return fmt.Errorf("cache invalidation failed: %w", err)
		}
		return nil
	}); err != nil {
		return nil, errInternal("upsert failed", err)
	}

	return entity, nil
}

// ─── Delete ───────────────────────────────────────────────────────────────────

func (a *allopDB[T]) Delete(ctx context.Context, id string) (*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errValidation("id must not be empty")
	}

	a.j.mu.Lock()
	defer a.j.mu.Unlock()

	var entity T
	if err := a.runSerializableTx(ctx, func(tx *gorm.DB) error {
		if err := tx.Unscoped().First(&entity, "id = ?", id).Error; err != nil {
			return err
		}
		if err := tx.Unscoped().Delete(&entity).Error; err != nil {
			return err
		}
		if err := a.invalidateAfterWrite(ctx, id); err != nil {
			return fmt.Errorf("cache invalidation failed: %w", err)
		}
		return nil
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errNotFound(err)
		}
		return nil, errInternal("delete failed", err)
	}

	return &entity, nil
}

// DeleteBatch remove vários registros (unscoped) e retorna seus últimos estados.
func (a *allopDB[T]) DeleteBatch(ctx context.Context, ids []string) ([]*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, errValidation("ids must not be empty")
	}

	a.j.mu.Lock()
	defer a.j.mu.Unlock()

	var entities []T
	if err := a.runSerializableTx(ctx, func(tx *gorm.DB) error {
		if err := tx.Unscoped().Where("id IN ?", ids).Find(&entities).Error; err != nil {
			return err
		}
		if len(entities) == 0 {
			return gorm.ErrRecordNotFound
		}
		if err := tx.Unscoped().Where("id IN ?", ids).Delete(&entities).Error; err != nil {
			return err
		}
		if err := a.invalidateAfterWrite(ctx, ids...); err != nil {
			return fmt.Errorf("cache invalidation failed: %w", err)
		}
		return nil
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errNotFound(err)
		}
		return nil, errInternal("delete batch failed", err)
	}

	res := make([]*T, len(entities))
	for i := range entities {
		res[i] = &entities[i]
	}

	return res, nil
}

// ─── HardDelete ───────────────────────────────────────────────────────────────

func (a *allopDB[T]) HardDelete(ctx context.Context, id string) (*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errValidation("id must not be empty")
	}

	a.j.mu.Lock()
	defer a.j.mu.Unlock()

	var entity T
	if err := a.runSerializableTx(ctx, func(tx *gorm.DB) error {
		if err := tx.Unscoped().First(&entity, "id = ?", id).Error; err != nil {
			return err
		}
		if err := tx.Unscoped().Delete(&entity).Error; err != nil {
			return err
		}
		if err := a.invalidateAfterWrite(ctx, id); err != nil {
			return fmt.Errorf("cache invalidation failed: %w", err)
		}
		return nil
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errNotFound(err)
		}
		return nil, errInternal("hard delete failed", err)
	}

	return &entity, nil
}

// ─── SoftDelete ───────────────────────────────────────────────────────────────

func (a *allopDB[T]) SoftDelete(ctx context.Context, id string) (*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errValidation("id must not be empty")
	}

	a.j.mu.Lock()
	defer a.j.mu.Unlock()

	var entity T
	if err := a.runSerializableTx(ctx, func(tx *gorm.DB) error {
		if err := tx.First(&entity, "id = ?", id).Error; err != nil {
			return err
		}
		// tx.Delete com DeletedAt no model → UPDATE deleted_at, não DELETE
		if err := tx.Delete(&entity).Error; err != nil {
			return err
		}
		if err := a.invalidateAfterWrite(ctx, id); err != nil {
			return fmt.Errorf("cache invalidation failed: %w", err)
		}
		return nil
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errNotFound(err)
		}
		return nil, errInternal("soft delete failed", err)
	}

	return &entity, nil
}

// ─── Restore ──────────────────────────────────────────────────────────────────

func (a *allopDB[T]) Restore(ctx context.Context, id string) (*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errValidation("id must not be empty")
	}

	a.j.mu.Lock()
	defer a.j.mu.Unlock()

	var entity T
	if err := a.runSerializableTx(ctx, func(tx *gorm.DB) error {
		// find including soft-deleted
		if err := tx.Unscoped().First(&entity, "id = ?", id).Error; err != nil {
			return err
		}
		// clear deleted_at to restore
		if err := tx.Unscoped().Model(&entity).Update("deleted_at", nil).Error; err != nil {
			return err
		}
		// reload entity to reflect restored state
		if err := tx.First(&entity, "id = ?", id).Error; err != nil {
			return err
		}
		if err := a.invalidateAfterWrite(ctx, id); err != nil {
			return fmt.Errorf("cache invalidation failed: %w", err)
		}
		return nil
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errNotFound(err)
		}
		return nil, errInternal("restore failed", err)
	}

	return &entity, nil
}

// ─── Save ─────────────────────────────────────────────────────────────────────

func (a *allopDB[T]) Save(ctx context.Context, entity *T) (*T, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	if entity == nil {
		return nil, errValidation("entity must not be nil")
	}

	a.j.mu.Lock()
	defer a.j.mu.Unlock()

	if err := a.runSerializableTx(ctx, func(tx *gorm.DB) error {
		if err := tx.Model(entity).Select("id").First(new(T)).Error; err != nil {
			return err
		}
		if err := tx.Model(entity).Select("*").Omit("id").Save(entity).Error; err != nil {
			return err
		}
		if err := a.invalidateAfterWrite(ctx, extractID(entity)); err != nil {
			return fmt.Errorf("cache invalidation failed: %w", err)
		}
		return nil
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errNotFound(err)
		}
		return nil, errInternal("save failed", err)
	}

	return entity, nil
}
