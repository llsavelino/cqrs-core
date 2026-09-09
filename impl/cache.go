package impl

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// ─── Lazy init ────────────────────────────────────────────────────────────────

func (a *allopDB[T]) init(ctx context.Context) error {
	a.j.on.Do(func() {
		sqlDB, err := a.j.db.DB()
		if err != nil {
			a.j.initErr = errInternal("failed to obtain sql.DB", err)
			return
		}
		if err = sqlDB.PingContext(ctx); err != nil {
			a.j.initErr = errInternal("database ping failed", err)
			return
		}
		if a.j.cfg.autoMigrate {
			if err = a.j.db.WithContext(ctx).AutoMigrate(new(T)); err != nil {
				a.j.initErr = errInternal("auto-migrate failed", err)
			}
		}
	})
	return a.j.initErr
}

// ─── Cache Redis — Cache-Aside (Lazy Loading) ─────────────────────────────────

func (a *allopDB[T]) cacheEnabled() bool { return a.j.redis != nil && a.j.cfg.cacheTTL > 0 }
func (a *allopDB[T]) ttl() time.Duration { return time.Duration(a.j.cfg.cacheTTL) * time.Millisecond }

func (a *allopDB[T]) fullKey(key string) string {
	ns := reflect.TypeOf(*new(T)).Name()
	if a.j.cfg.cacheNamespace != "" {
		ns = a.j.cfg.cacheNamespace
	}
	return ns + ":" + key
}

func (a *allopDB[T]) cacheGetEntity(ctx context.Context, key string) (T, bool) {
	var zero T
	if !a.cacheEnabled() {
		return zero, false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	data, err := a.j.redis.Get(ctx, a.fullKey(key)).Bytes()
	if err != nil {
		return zero, false
	}
	var result T
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()
	if err := dec.Decode(&result); err != nil {
		return zero, false
	}
	return result, true
}

func (a *allopDB[T]) cacheGetEntities(ctx context.Context, key string) ([]T, bool) {
	if !a.cacheEnabled() {
		return nil, false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	data, err := a.j.redis.Get(ctx, a.fullKey(key)).Bytes()
	if err != nil {
		return nil, false
	}
	var result []T
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()
	if err := dec.Decode(&result); err != nil {
		return nil, false
	}
	return result, true
}

func (a *allopDB[T]) cacheGetBool(ctx context.Context, key string) (bool, bool) {
	if !a.cacheEnabled() {
		return false, false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	data, err := a.j.redis.Get(ctx, a.fullKey(key)).Bytes()
	if err != nil {
		return false, false
	}
	var result bool
	if err := json.Unmarshal(data, &result); err != nil {
		return false, false
	}
	return result, true
}

func (a *allopDB[T]) cacheGetInt64(ctx context.Context, key string) (int64, bool) {
	if !a.cacheEnabled() {
		return 0, false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	data, err := a.j.redis.Get(ctx, a.fullKey(key)).Bytes()
	if err != nil {
		return 0, false
	}
	var result int64
	if err := json.Unmarshal(data, &result); err != nil {
		return 0, false
	}
	return result, true
}

func (a *allopDB[T]) cacheSet(ctx context.Context, key string, v any) error {
	if !a.cacheEnabled() {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("cache marshal failed: %w", err)
	}
	return a.j.redis.Set(ctx, a.fullKey(key), data, a.ttl()).Err()
}

func (a *allopDB[T]) cacheDel(ctx context.Context, keys ...string) error {
	if !a.cacheEnabled() || len(keys) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = a.fullKey(k)
	}
	return a.j.redis.Del(ctx, full...).Err()
}

func (a *allopDB[T]) cacheDelPattern(ctx context.Context, pattern string) error {
	if !a.cacheEnabled() || pattern == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	fullPattern := a.fullKey(pattern)
	const batch = 100
	var cursor uint64
	for {
		keys, nextCursor, err := a.j.redis.Scan(ctx, cursor, fullPattern, batch).Result()
		if err != nil {
			return fmt.Errorf("cache scan failed: %w", err)
		}
		if len(keys) > 0 {
			if err := a.j.redis.Del(ctx, keys...).Err(); err != nil {
				return fmt.Errorf("cache del pattern failed: %w", err)
			}
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return nil
}

func (a *allopDB[T]) invalidateAfterWrite(ctx context.Context, ids ...string) error {
	keys := make([]string, 0, len(ids)*2)
	for _, id := range ids {
		if id != "" && len(id) <= 255 {
			keys = append(keys, "find:"+id, "exists:"+id, "search:"+id)
		}
	}
	if len(keys) > 0 {
		if err := a.cacheDel(ctx, keys...); err != nil {
			return fmt.Errorf("cache invalidate failed: %w", err)
		}
	}

	for _, pattern := range []string{"findby:*", "list:*", "count:*", "listSoft:*", "nolistSoft:*"} {
		if err := a.cacheDelPattern(ctx, pattern); err != nil {
			return fmt.Errorf("cache pattern invalidate failed: %w", err)
		}
	}
	return nil
}

func extractID(entity any) string {
	v := reflect.ValueOf(entity)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return ""
	}
	f := v.FieldByName("ID")
	if !f.IsValid() {
		return ""
	}
	return fmt.Sprintf("%v", f.Interface())
}
