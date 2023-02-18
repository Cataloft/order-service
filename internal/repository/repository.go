package repository

import (
	"context"
	"log"

	pgx "github.com/jackc/pgx/v5"
)

type Repository struct {
	Conn *pgx.Conn
}

func New(connStr string) *Repository {
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		log.Fatal(err)
	}
	err = conn.Ping(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	return &Repository{
		Conn: conn,
	}
}
