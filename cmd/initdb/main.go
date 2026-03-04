package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"time"

	"arena-crawler/internal/db"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var safeDBName = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func main() {
	adminDSN := flag.String("admin-dsn", os.Getenv("PG_ADMIN_DSN"), "Admin Postgres DSN (usually connected to postgres DB)")
	dbName := flag.String("db-name", envOrDefault("DB_NAME", "arena_personal_projects"), "Database name to create")
	targetDSN := flag.String("target-dsn", os.Getenv("DATABASE_URL"), "Target DB DSN; if empty, derived from admin-dsn + db-name")
	flag.Parse()

	if *adminDSN == "" {
		log.Fatal("missing -admin-dsn (or PG_ADMIN_DSN)")
	}
	if !safeDBName.MatchString(*dbName) {
		log.Fatalf("invalid db name %q (letters, numbers, underscores only)", *dbName)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := ensureDatabase(ctx, *adminDSN, *dbName); err != nil {
		log.Fatalf("create database: %v", err)
	}

	resolvedTargetDSN := *targetDSN
	if resolvedTargetDSN == "" {
		dsn, err := deriveTargetDSN(*adminDSN, *dbName)
		if err != nil {
			log.Fatalf("derive target dsn: %v", err)
		}
		resolvedTargetDSN = dsn
	}

	if err := db.ApplySchema(ctx, resolvedTargetDSN); err != nil {
		log.Fatalf("apply schema: %v", err)
	}

	fmt.Println("database ready")
	fmt.Printf("db_name=%s\n", *dbName)
	fmt.Printf("target_dsn=%s\n", redactDSN(resolvedTargetDSN))
}

func ensureDatabase(ctx context.Context, adminDSN, dbName string) error {
	adminDB, err := sql.Open("pgx", adminDSN)
	if err != nil {
		return err
	}
	defer adminDB.Close()

	var exists bool
	if err := adminDB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}

	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, dbName))
	return err
}

func deriveTargetDSN(adminDSN, dbName string) (string, error) {
	u, err := url.Parse(adminDSN)
	if err != nil {
		return "", err
	}
	u.Path = "/" + dbName
	return u.String(), nil
}

func redactDSN(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid dsn>"
	}
	if u.User != nil {
		username := u.User.Username()
		u.User = url.User(username)
	}
	return u.String()
}

func envOrDefault(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
