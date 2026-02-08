APP      := migrate
MODULE   := postgreToPostgre
GOFILES  := $(shell find . -name '*.go' -not -path './vendor/*')
ADDR     ?= :8081

.PHONY: all build run clean deps fmt vet test lint help

all: build ## デフォルト: ビルド

build: ## バイナリをビルド
	go build -o $(APP) .

up: build ## ビルドして起動
	ADDR=$(ADDR) ./$(APP)

dev: ## go run で直接起動（ビルドなし）
	ADDR=$(ADDR) go run .

clean: ## 生成物を削除
	rm -f $(APP)
	go clean

deps: ## 依存パッケージを取得
	go mod download
	go mod tidy

fmt: ## コードをフォーマット
	gofmt -s -w $(GOFILES)

vet: ## 静的解析
	go vet ./...

test: ## テスト実行
	go test -v -race ./...

lint: vet fmt ## vet + fmt

help: ## このヘルプを表示
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
