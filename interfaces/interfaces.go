package interfaces

import "context"

type CommandConfig struct {
	// Tx é a transação interna injetada, se houver. Usada para operações atômicas.
	Tx interface{ getDB() interface{} }
}

type CommandOpt func(*CommandConfig)

type QueryConfig struct {
	Page     int                              // número da página para paginação (1-based)
	PageSize int                              // número de itens por página para paginação
	OrderBy  string                           // expressão de ordenação (ex: "created_at DESC")
	Preloads []string                         // relações para pré-carregar com Preload (ex: "User", "Items")
	Tx       interface{ getDB() interface{} } // injeção de transação interna
}

type QueryOpt func(*QueryConfig)

// ─── Locked ────────────────────────────────────────────────────────────────

type Locked[T any] interface {
	// ── Lock ────────────────────────────────────────────────────────────────
	LockReadOnly(ctx context.Context, id string) (*T, error)
	LockWriteOnly(ctx context.Context, id string) (*T, error)
}

// ─── Transactional ─────────────────────────────────────────────────────────

type Transactional[T any] interface {
	// ── Transação ─────────────────────────────────────────────────────────────

	// Tx executa fn dentro de uma transação atômica. Retornar erro faz
	// rollback; retornar nil faz commit. O CommandRepository passado para fn
	// usa a conexão transacional isolada.
	Tx(ctx context.Context, fn func(cmd Command[T]) error) error
	// TxSpecial é como Tx, mas para operações especiais de soft delete e restauração.
	TxSpecial(ctx context.Context, fn func(cmd CommandSpecial[T]) error) error
	// TxQuery é como Tx, mas para operações de leitura dentro de transação.
	TxQuery(ctx context.Context, fn func(qry Query[T]) error) error
	// TxQuerySpecial é como Tx, mas para operações de leitura especiais dentro de transação.
	TxQuerySpecial(ctx context.Context, fn func(qry QuerySpecial[T]) error) error
}

// ─── InfrastructureDB ─────────────────────────────────────────────────────────

type InfrastructureDB interface {
	// Ping verifica se a conexão com o banco de leitura está ativa.
	Ping(ctx context.Context) error
	// MOCK: Pong é um alias de Ping para testes, permitindo simular falhas de conexão.
	Pong(ctx context.Context) error
}

// Read Model (CQRS)

// ─── QueryRepository ─────────────────────────────────────────────────────────

type Query[T any] interface {
	// ── Verificação ───────────────────────────────────────────────────────────

	Ok(entity *T) bool // Ok retorna true quando entity é não-nulo.

	// Exists verifica se um registro existe via SELECT 1 LIMIT 1,
	// sem transferir nenhuma coluna de dados pelo wire.
	Exists(ctx context.Context, id string) (bool, error)

	// Count retorna o número de registros que satisfazem filter.
	// Passe nil ou map vazio para contar todos os registros da tabela.
	Count(ctx context.Context, filter map[string]any) (int64, error)

	// ── Busca singular ────────────────────────────────────────────────────────

	// Find recupera um único registro pela chave primária.
	Find(ctx context.Context, id string, opts ...QueryOpt) (*T, error)

	// FindBy busca o primeiro registro onde field = value.
	FindBy(ctx context.Context, field string, value any, opts ...QueryOpt) (*T, error)

	// ── Busca em coleção ──────────────────────────────────────────────────────

	// List retorna a coleção filtrada e paginada conforme opts.
	List(ctx context.Context, filter map[string]any, opts ...QueryOpt) ([]T, error)

	// Stream percorre os registros em janelas de batchSize, chamando fn
	// para cada lote. Nenhum lote completo é mantido em memória simultaneamente.
	// Ideal para exports, ETL e processamentos pesados.
	Stream(ctx context.Context, filter map[string]any, batchSize int, fn func(batch []T) error) error
}

// ─── QuerySpecial ─────────────────────────────────────────────────────────

type QuerySpecial[T any] interface {
	// ── Singular ──────────────────────────────────────────────────────────────

	// Search recupera um registro soft-deletado para fins de auditoria.
	Search(ctx context.Context, id string) (*T, error)

	// ── Busca em coleção ──────────────────────────────────────────────────────

	// ListDeleted retorna registros soft-deletados, para fins de auditoria e recuperação.
	OnlyListDeleted(ctx context.Context, opts ...QueryOpt) ([]T, error)

	// NoListDeleted retorna apenas registros não-deletados, ignorando o filtro de exclusão.
	NoListDeleted(ctx context.Context, opts ...QueryOpt) ([]T, error)
}

// Write Model (CQRS)

// ─── CommandSpecial ─────────────────────────────────────────────────────────

type CommandSpecial[T any] interface {
	// ── Singular ──────────────────────────────────────────────────────────────

	// Restore desfaz um soft-delete, reativando o registro.
	Restore(ctx context.Context, id string) (*T, error)

	// SoftDelete marca o registro como deletado sem removê-lo fisicamente.
	SoftDelete(ctx context.Context, id string) (*T, error)

	// HardDelete remove o registro fisicamente, sem deixar vestígios.
	HardDelete(ctx context.Context, id string) (*T, error)
}

// ─── CommandRepository ────────────────────────────────────────────────────────

type Command[T any | interface{ any }] interface {
	// ── Singular ──────────────────────────────────────────────────────────────

	// Insert persiste uma nova entidade e a retorna com campos gerados
	// pelo banco (ID, timestamps, defaults) preenchidos.
	Insert(ctx context.Context, entity *T) (*T, error)

	// Insert extension para Lotes
	InsertBatch(ctx context.Context, entities []*T) ([]*T, error)

	// Update persiste a struct inteira via Save — todas as colunas são
	// escritas, inclusive zero values. O id é lido da própria entidade.
	Update(ctx context.Context, entity *T) (*T, error)

	// Replace substitui completamente o registro pelo conteúdo de entity.
	// O id vem do argumento, nunca da entidade, garantindo consistência
	// entre a rota e o payload.
	Replace(ctx context.Context, id string, entity *T) (*T, error)

	// Patch aplica uma atualização parcial — somente as colunas do map
	// são escritas. Zero values no map são gravados corretamente.
	Patch(ctx context.Context, id string, fields map[string]any) (*T, error)

	// Upsert insere ou, em conflito nas colunas de conflictKeys,
	// atualiza todas as demais colunas atomicamente.
	Upsert(ctx context.Context, entity *T, conflictKeys []string) (*T, error)

	// Delete remove o registro e retorna seu último estado.
	// Suporta soft-delete via gorm.DeletedAt automaticamente.
	Delete(ctx context.Context, id string) (*T, error)

	// Delete extension para Lotes
	DeleteBatch(ctx context.Context, ids []string) ([]*T, error)

	// Save é um alias de Update para compatibilidade com gorm.DB
	Save(ctx context.Context, entity *T) (*T, error)
}
