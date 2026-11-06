package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"tasks/internal/logger"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func run() error {
	logger := logger.New("test DB")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		logger.Error("db url is empty")
		return errors.New("db url is empty")
	}

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		logger.Error("create new pool:", "error", err)
		return err
	}
	defer pool.Close()

	pingCtx, cancelPing := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelPing()

	msg := "database connection established"
	level := slog.LevelInfo
	component := "dbcheck"

	start := time.Now()
	err = pool.Ping(pingCtx)
	duration := time.Since(start)

	attrs := []slog.Attr{
		slog.String("component", component),
		slog.Duration("duration", duration),
	}

	if err != nil {
		msg = "database connection failed"
		level = slog.LevelError
		attrs = append(attrs, slog.Any("error", err))
	}

	logger.LogAttrs(context.Background(), level, msg, attrs...)

	if err != nil {
		return err
	}
	
	txCtx, cancelTxCtx := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelTxCtx()

	tx, err := pool.Begin(txCtx)
	if err != nil {
		logger.Error(
			"create a transaction begin",
			"error", err,
		)
		return err
	}
	defer tx.Rollback(context.Background())
	
	queryCtx, cancelQuery := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelQuery()

	var done bool
	var title string
	var id int64

	query := "INSERT INTO tasks (title) VALUES ($1) RETURNING id, title, done;"
	taskTitle := "Learn SQL from Go"

	err = tx.QueryRow(queryCtx, query, taskTitle).Scan(&id, &title, &done)
	if err != nil {
		logger.Error("sql query failed", slog.Any("error", err))
		return err
	}

	logger.Info(
		"insert executed",
		slog.String("title", title),
		slog.Int64("id", id),
		slog.Bool("done", done),
	)

	updateQuery := "UPDATE tasks SET done = $1 WHERE id = $2 RETURNING id, title, done;"

	updateCtx, updateCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer updateCancel()

	var updatedID int64
	var updatedTitle string
	var updatedDone bool

	done = true
	err = tx.QueryRow(updateCtx, updateQuery, done, id).Scan(&updatedID, &updatedTitle, &updatedDone)
	if err != nil {
		msg := "updating task failed"

		if errors.Is(err, pgx.ErrNoRows) {
			msg = "update task: task with this id doesnt exist"
		}

		logger.Error(msg, "error", err)
		return err
	}

	logger.Info(
		"updating task successful",
		"id", updatedID,
		"title", updatedTitle,
		"done", updatedDone,
	)
	
	commitTxCtx, cancelCommitCtx := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelCommitCtx()

	err = tx.Commit(commitTxCtx)
	if err != nil {
		logger.Error(
			"commit transcation error",
			"error", err,
		)
		return err
	}

	selectCtx, selectCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer selectCancel()

	rows, err := pool.Query(selectCtx, "SELECT id, title, done FROM tasks ORDER BY id;")
	if err != nil {
		logger.Error("error while query()", "error", err)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var title string
		var done bool

		err := rows.Scan(&id, &title, &done)
		if err != nil {
			logger.Error("select query error", "error", err)
			return err
		}

		logger.Info("successful select query", "id", id, "title", title, "done", done)
	}

	if err = rows.Err(); err != nil {
		logger.Error("error while executing a query or reading its result", "error", err)
		return err
	}

	deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer deleteCancel()

	var deletedID int64
	var deletedTitle string
	var deletedDone bool

	deleteQuery := "DELETE FROM tasks WHERE id = $1 RETURNING id, title, done;"
	err = pool.QueryRow(deleteCtx, deleteQuery, id).Scan(&deletedID, &deletedTitle, &deletedDone)
	if err != nil {
		msg := "failed to delete task"
		if errors.Is(err, pgx.ErrNoRows) {
			msg = "task with this id doesnt exist"
		}
		logger.Error(msg, "error", err)
		return err
	}

	logger.Info(
		"delete task successful",
		"id", deletedID,
		"title", deletedTitle,
		"done", deletedDone,
	)

	return err
}

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}
