# cqrs-core

Biblioteca Go para suporte de CQRS com abstrações de repositórios de leitura e escrita, transações, soft delete e helpers de query.

## Módulo

```go
module github.com/llsavelino/cqrs-core
```

## Instalação

```bash
go get github.com/llsavelino/cqrs-core@latest
```

## Uso

```go
package main

import (
    "github.com/llsavelino/cqrs-core/interfaces"
)

func main() {
    _ = interfaces.QueryConfig{}
    _ = interfaces.CommandConfig{}
}
```

## Estrutura

- `interfaces/` — contratos e tipos de interface
- `impl/` — implementações concretas

## Verificação

```bash
go test ./...
```
