package storage

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Task struct {
	ID    int64
	Title string
	Done  bool
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{
		pool: pool,
	}
}

func (s *PostgresStore) Create(
	ctx context.Context,
	title string,
	done bool,
) (Task, error) {
	var task Task

	err := s.pool.QueryRow(ctx, "INSERT INTO tasks (title, done) VALUES ($1, $2) RETURNING id, title, done;", title, done).Scan(&task.ID, &task.Title, &task.Done)
	if err != nil {
		return Task{}, err
	}

	return task, nil
}

func (s *PostgresStore) GetByID(ctx context.Context, id int64) (Task, error) {
	var task Task

	err := s.pool.QueryRow(ctx, "SELECT id, title, done FROM tasks WHERE id = $1;", id).Scan(&task.ID, &task.Title, &task.Done)
	if err != nil {
		return Task{}, err
	}

	return task, nil
}

func (s *PostgresStore) List(ctx context.Context) ([]Task, error) {
	var tasks []Task

	rows, err := s.pool.Query(ctx, "SELECT id, title, done FROM tasks ORDER BY id;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var task Task

		err := rows.Scan(&task.ID, &task.Title, &task.Done)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, task)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return tasks, nil
}

func (s *PostgresStore) Update(
	ctx context.Context,
	id int64,
	title *string,
	done *bool,
) (Task, error) {
	var task Task

	err := s.pool.QueryRow(ctx, "UPDATE tasks SET title = COALESCE($1, title), done = COALESCE($2, done) WHERE id = $3 RETURNING id, title, done;", title, done, id).Scan(&task.ID, &task.Title, &task.Done)
	if err != nil {
		return Task{}, err
	}

	return task, nil
}

func (s *PostgresStore) Delete(ctx context.Context, id int64) error {
	var deletedID int64
	err := s.pool.QueryRow(ctx, "DELETE FROM tasks WHERE id = $1 RETURNING id", id).Scan(&deletedID)
	if err != nil {
		return err
	}

	return nil
}
