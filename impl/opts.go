package impl

import (
	"strings"

	"github.com/llsavelino/cqrs-core/interfaces"
	"github.com/redis/go-redis/v9"
)

// ─── NewOption ────────────────────────────────────────────────────────────────

type instanceCfg struct {
	autoMigrate    bool
	poolCap        int
	retries        int
	backoff        int64
	timeout        int64
	cacheTTL       int64
	redisClient    *redis.Client
	cacheNamespace string
}

type NewOption func(*instanceCfg)

func WithAutoMigrate() NewOption {
	return func(c *instanceCfg) { c.autoMigrate = true }
}

func WithRetry(retries int, backoffMillis int64) NewOption {
	return func(c *instanceCfg) {
		if backoffMillis < 10 {
			backoffMillis = 10
		}
		if backoffMillis > 5000 {
			backoffMillis = 5000
		}
		c.retries = retries
		c.backoff = backoffMillis
	}
}

func WithTimeout(timeoutMillis int64) NewOption {
	return func(c *instanceCfg) {
		if timeoutMillis < 100 {
			timeoutMillis = 100
		}
		if timeoutMillis > 60000 {
			timeoutMillis = 60000
		}
		c.timeout = timeoutMillis
	}
}

func WithCache(client *redis.Client, ttlMillis int64, namespace string) NewOption {
	return func(c *instanceCfg) {
		if !isValidCacheNamespace(namespace) {
			return
		}
		c.redisClient = client
		c.cacheTTL = ttlMillis
		c.cacheNamespace = namespace
	}
}

func isValidCacheNamespace(ns string) bool {
	if ns == "" || len(ns) > 64 {
		return false
	}
	for _, ch := range ns {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}

func WithPoolCap(n int) NewOption {
	return func(c *instanceCfg) {
		if n < 1 {
			n = 1
		}
		if n > 10000 {
			n = 10000
		}
		c.poolCap = n
	}
}

func applyNewOpts(opts []NewOption) instanceCfg {
	cfg := instanceCfg{poolCap: 32, retries: 0, backoff: 50, timeout: 0, cacheTTL: 0}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// ─── QueryOpt helpers ─────────────────────────────────────────────────────────

func WithPage(page, size int) interfaces.QueryOpt {
	return func(c *interfaces.QueryConfig) { c.Page, c.PageSize = page, size }
}

func WithOrder(expr string) interfaces.QueryOpt {
	return func(c *interfaces.QueryConfig) {
		if isValidOrderExpr(expr) {
			c.OrderBy = expr
		}
	}
}

func isValidOrderExpr(expr string) bool {
	if expr == "" || len(expr) > 255 {
		return false
	}
	for _, word := range strings.Fields(expr) {
		if word == "ASC" || word == "DESC" {
			continue
		}
		if word == "," {
			continue
		}
		if !isValidFieldName(word) {
			return false
		}
	}
	return true
}

func isValidFieldName(field string) bool {
	if len(field) == 0 || len(field) > 64 {
		return false
	}
	for i, ch := range field {
		if i == 0 && (ch >= '0' && ch <= '9') {
			return false
		}
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '.') {
			return false
		}
	}
	return true
}

func WithPreload(relations ...string) interfaces.QueryOpt {
	return func(c *interfaces.QueryConfig) { c.Preloads = append(c.Preloads, relations...) }
}

func applyQueryOpts(opts []interfaces.QueryOpt) *interfaces.QueryConfig {
	cfg := &interfaces.QueryConfig{Page: 1, PageSize: 20}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.Page < 1 {
		cfg.Page = 1
	}
	switch {
	case cfg.PageSize < 1:
		cfg.PageSize = 20
	case cfg.PageSize > 1000:
		cfg.PageSize = 1000
	}
	return cfg
}
