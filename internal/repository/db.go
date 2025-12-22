package repository

import (
	"database/sql"
	"fmt"
	"inMemoryAggregation/internal/middleware"
	"inMemoryAggregation/internal/models"
	"time"

	_ "github.com/lib/pq"
)

type Repository struct {
	db     *sql.DB
	logger middleware.Logger
}

func NewRepository(dsn string, logger middleware.Logger) (*Repository, error) {

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// connection pool
	db.SetMaxOpenConns(25)                 // Max open connections
	db.SetMaxIdleConns(10)                 // Max idle connections
	db.SetConnMaxLifetime(5 * time.Minute) // Max connection lifetime
	db.SetConnMaxIdleTime(1 * time.Minute) // Max idle time

	// test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	repo := &Repository{
		db:     db,
		logger: logger,
	}

	// create table if not exists
	if err := repo.createTable(); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	logger.Log("msg", "database connection established")

	return repo, nil
}

func (r *Repository) createTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS ad_metrics (
		date DATE NOT NULL,
		app TEXT NOT NULL,
		country TEXT NOT NULL,
		ad_request BIGINT DEFAULT 0,
		ad_response BIGINT DEFAULT 0,
		ad_impression BIGINT DEFAULT 0,
		ad_clicks BIGINT DEFAULT 0,
		dau BIGINT DEFAULT 0,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (date, app, country)
	)`

	_, err := r.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	r.logger.Log("msg", "table ad_metrics ready")
	return nil
}

func (r *Repository) BatchInsert(rows []models.AggregateRow) error {
	if len(rows) == 0 {
		return nil
	}

	startTime := time.Now()

	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO ad_metrics (date, app, country, ad_request, ad_response, ad_impression, ad_clicks, dau, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)
		ON CONFLICT (date, app, country)
		DO UPDATE SET
			ad_request = ad_metrics.ad_request + EXCLUDED.ad_request,
			ad_response = ad_metrics.ad_response + EXCLUDED.ad_response,
			ad_impression = ad_metrics.ad_impression + EXCLUDED.ad_impression,
			ad_clicks = ad_metrics.ad_clicks + EXCLUDED.ad_clicks,
			dau = ad_metrics.dau + EXCLUDED.dau,
			updated_at = CURRENT_TIMESTAMP
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
		_, err := stmt.Exec(
			row.Date,
			row.App,
			row.Country,
			row.AdRequest,
			row.AdResponse,
			row.AdImpression,
			row.AdClicks,
			row.DAU,
		)
		if err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	duration := time.Since(startTime)
	r.logger.Log(
		"msg", "batch insert successful",
		"rows", len(rows),
		"duration", duration.String(),
	)

	return nil
}

func (r *Repository) Close() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}
