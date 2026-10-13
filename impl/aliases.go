package impl

import (
	"context"
	"fmt"
	"reflect"

	"github.com/llsavelino/cqrs-core/interfaces"
)

func (a *allopDB[T]) FindOne(ctx context.Context, id string, opts ...interfaces.QueryOpt) (*T, error) {
	return a.Find(ctx, id, opts...)
}

func (a *allopDB[T]) FindById(ctx context.Context, id string, opts ...interfaces.QueryOpt) (*T, error) {
	return a.Find(ctx, id, opts...)
}

func (a *allopDB[T]) Get(ctx context.Context, id string, opts ...interfaces.QueryOpt) (*T, error) {
	return a.Find(ctx, id, opts...)
}

func (a *allopDB[T]) Select(ctx context.Context, filter map[string]any, opts ...interfaces.QueryOpt) ([]T, error) {
	return a.List(ctx, filter, opts...)
}

func (a *allopDB[T]) Scan(ctx context.Context, filter map[string]any, opts ...interfaces.QueryOpt) ([]T, error) {
	return a.List(ctx, filter, opts...)
}

func (a *allopDB[T]) Filter(ctx context.Context, filter map[string]any, opts ...interfaces.QueryOpt) ([]T, error) {
	return a.List(ctx, filter, opts...)
}

func (a *allopDB[T]) Query(ctx context.Context, filter map[string]any, opts ...interfaces.QueryOpt) ([]T, error) {
	return a.List(ctx, filter, opts...)
}

func (a *allopDB[T]) Distinct(ctx context.Context, field string, filter map[string]any, opts ...interfaces.QueryOpt) ([]any, error) {
	if field == "" {
		return nil, errValidation("field must not be empty")
	}

	items, err := a.List(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}

	values := make([]any, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		rv := reflect.ValueOf(item)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		if rv.Kind() != reflect.Struct {
			return nil, errValidation("entity must be a struct")
		}
		fv := rv.FieldByName(field)
		if !fv.IsValid() {
			return nil, errValidation("field not found")
		}
		v := fv.Interface()
		key := fmt.Sprintf("%v", v)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		values = append(values, v)
	}
	return values, nil
}

func (a *allopDB[T]) Has(ctx context.Context, id string) (bool, error) {
	return a.Exists(ctx, id)
}

func (a *allopDB[T]) Create(ctx context.Context, entity *T) (*T, error) {
	return a.Insert(ctx, entity)
}

func (a *allopDB[T]) Add(ctx context.Context, entity *T) (*T, error) {
	return a.Insert(ctx, entity)
}

func (a *allopDB[T]) InsertMany(ctx context.Context, entities []*T) ([]*T, error) {
	return a.InsertBatch(ctx, entities)
}

func (a *allopDB[T]) BulkCreate(ctx context.Context, entities []*T) ([]*T, error) {
	return a.InsertBatch(ctx, entities)
}

func (a *allopDB[T]) UpdateOne(ctx context.Context, id string, entity *T) (*T, error) {
	return a.Replace(ctx, id, entity)
}

func (a *allopDB[T]) UpdateMany(ctx context.Context, filter map[string]any, fields map[string]any) ([]T, error) {
	if len(fields) == 0 {
		return nil, errValidation("at least one field must be provided")
	}

	items, err := a.List(ctx, filter)
	if err != nil {
		return nil, err
	}

	updated := make([]T, 0, len(items))
	for _, item := range items {
		id := extractID(item)
		if id == "" {
			return nil, errValidation("entity id must not be empty")
		}
		patched, err := a.Patch(ctx, id, fields)
		if err != nil {
			return nil, err
		}
		updated = append(updated, *patched)
	}
	return updated, nil
}

func (a *allopDB[T]) Modify(ctx context.Context, id string, fields map[string]any) (*T, error) {
	return a.Patch(ctx, id, fields)
}

func (a *allopDB[T]) DeleteOne(ctx context.Context, id string) (*T, error) {
	return a.Delete(ctx, id)
}

func (a *allopDB[T]) Remove(ctx context.Context, id string) (*T, error) {
	return a.Delete(ctx, id)
}

func (a *allopDB[T]) DeleteMany(ctx context.Context, filter map[string]any) ([]*T, error) {
	items, err := a.List(ctx, filter)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		if id := extractID(item); id != "" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return []*T{}, nil
	}
	return a.DeleteBatch(ctx, ids)
}

func (a *allopDB[T]) Destroy(ctx context.Context, filter map[string]any) ([]*T, error) {
	return a.DeleteMany(ctx, filter)
}

func (a *allopDB[T]) Archive(ctx context.Context, id string) (*T, error) {
	return a.SoftDelete(ctx, id)
}

func (a *allopDB[T]) ListDeleted(ctx context.Context, opts ...interfaces.QueryOpt) ([]T, error) {
	return a.OnlyListDeleted(ctx, opts...)
}

func (a *allopDB[T]) BeginTransaction(ctx context.Context, fn func(cmd interfaces.Command[T]) error) error {
	return a.Tx(ctx, fn)
}

func (a *allopDB[T]) StartSession(ctx context.Context, fn func(cmd interfaces.Command[T]) error) error {
	return a.Tx(ctx, fn)
}

func (a *allopDB[T]) Commit(ctx context.Context) error {
	_ = ctx
	return nil
}

func (a *allopDB[T]) Rollback(ctx context.Context) error {
	_ = ctx
	return nil
}

func (a *allopDB[T]) Abort(ctx context.Context) error {
	_ = ctx
	return nil
}
