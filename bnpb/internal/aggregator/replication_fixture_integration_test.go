//go:build integration

package aggregator

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type replicationFixture struct {
	bin, primary, standby  string
	primaryURL, standbyURL string
}

// This opt-in fixture owns both processes and every data directory it removes.
func newReplicationFixture(t *testing.T) *replicationFixture {
	t.Helper()
	bin := os.Getenv("BNPB_TEST_POSTGRES_BIN")
	if bin == "" {
		t.Skip("set BNPB_TEST_POSTGRES_BIN to run the disposable physical-replication fixture")
	}
	root := t.TempDir()
	fixture := &replicationFixture{bin: bin, primary: filepath.Join(root, "primary"), standby: filepath.Join(root, "standby")}
	primaryPort, standbyPort := fixturePort(t), fixturePort(t)
	fixture.primaryURL = fmt.Sprintf("postgres://postgres@127.0.0.1:%d/bnpb_m2_step4_test?sslmode=disable", primaryPort)
	fixture.standbyURL = fmt.Sprintf("postgres://postgres@127.0.0.1:%d/bnpb_m2_step4_test?sslmode=disable", standbyPort)
	fixture.command(t, "initdb", "-D", fixture.primary, "-U", "postgres", "-A", "trust", "--no-locale", "-E", "UTF8")
	fixtureAppend(t, filepath.Join(fixture.primary, "postgresql.conf"), fmt.Sprintf("\nlisten_addresses='127.0.0.1'\nport=%d\nwal_level=replica\nmax_wal_senders=8\nmax_replication_slots=8\nsynchronous_commit=local\n", primaryPort))
	t.Cleanup(func() {
		for _, directory := range []string{fixture.standby, fixture.primary} {
			if _, err := os.Stat(filepath.Join(directory, "postmaster.pid")); err == nil {
				fixture.command(t, "pg_ctl", "-D", directory, "-m", "immediate", "-w", "stop")
			}
		}
	})
	fixture.start(t, fixture.primary)
	admin := fixture.connect(t, fixture.primaryURL)
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE bnpb_m2_step4_test"); err != nil {
		t.Fatal(err)
	}
	_ = admin.Close(t.Context())
	backupURL, _ := url.Parse(fixture.primaryURL)
	query := backupURL.Query()
	query.Set("application_name", "durability-standby")
	backupURL.RawQuery = query.Encode()
	fixture.command(t, "pg_basebackup", "-d", backupURL.String(), "-D", fixture.standby, "-X", "stream", "-R", "-C", "-S", "durability_standby", "--checkpoint=fast")
	fixtureAppend(t, filepath.Join(fixture.standby, "postgresql.conf"), fmt.Sprintf("\nport=%d\nhot_standby=on\nwal_receiver_status_interval=1s\n", standbyPort))
	fixture.start(t, fixture.standby)
	fixture.sync(t, `FIRST 1 ("durability-standby")`)
	return fixture
}

func fixturePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port
}

func fixtureAppend(t *testing.T, path, configuration string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(configuration); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func (fixture *replicationFixture) command(t *testing.T, name string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, filepath.Join(fixture.bin, name), args...)
	// On Windows the postmaster inherits pg_ctl's output handles. A pipe would
	// keep CombinedOutput waiting for EOF until the server itself exits.
	output, err := os.CreateTemp(filepath.Dir(fixture.primary), "postgres-command-*.log")
	if err != nil {
		t.Fatal(err)
	}
	command.Stdout, command.Stderr = output, output
	err = command.Run()
	if closeErr := output.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		log, _ := os.ReadFile(output.Name())
		t.Fatalf("disposable PostgreSQL %s: %v (context: %v)\n%s", name, err, ctx.Err(), log)
	}
}

func (fixture *replicationFixture) start(t *testing.T, directory string) {
	t.Helper()
	fixture.command(t, "pg_ctl", "-D", directory, "-l", directory+".log", "-w", "-t", "30", "start")
}

func (fixture *replicationFixture) connect(t *testing.T, address string) *pgx.Conn {
	t.Helper()
	parsed, _ := url.Parse(address)
	parsed.Path = "/postgres"
	connection, err := pgx.Connect(t.Context(), parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func (fixture *replicationFixture) sync(t *testing.T, names string) {
	t.Helper()
	admin := fixture.connect(t, fixture.primaryURL)
	defer admin.Close(context.Background())
	var statement string
	if err := admin.QueryRow(t.Context(), "SELECT format('ALTER SYSTEM SET synchronous_standby_names = %L', $1::text)", names).Scan(&statement); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(t.Context(), statement); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(t.Context(), "SELECT pg_reload_conf()"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for {
		var ready bool
		err := admin.QueryRow(ctx, `SELECT current_setting('synchronous_standby_names') = $1
			AND ($1 = '' OR EXISTS (SELECT 1 FROM pg_stat_replication WHERE sync_state='sync' AND state='streaming'))`, names).Scan(&ready)
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (fixture *replicationFixture) routedConfig(t *testing.T) Config {
	t.Helper()
	cfg := testConfig(t)
	admin, err := pgx.Connect(t.Context(), fixture.primaryURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	if _, err := admin.Exec(t.Context(), "CREATE ROLE durability_writer LOGIN; GRANT pg_read_all_stats TO durability_writer"); err != nil {
		t.Fatal(err)
	}
	for index := -1; index < logicalShardCount; index++ {
		schemaName := "durability_control"
		if index >= 0 {
			schemaName = "durability_shard_" + strconv.Itoa(index)
		}
		schema := pgx.Identifier{schemaName}.Sanitize()
		if _, err := admin.Exec(t.Context(), "CREATE SCHEMA "+schema+"; GRANT USAGE ON SCHEMA "+schema+" TO durability_writer"); err != nil {
			t.Fatal(err)
		}
		address, _ := url.Parse(fixture.primaryURL)
		query := address.Query()
		query.Set("search_path", schemaName)
		address.RawQuery = query.Encode()
		adminURL := address.String()
		address.User = url.User("durability_writer")
		if index < 0 {
			cfg.DatabaseURL, cfg.WriterURL = adminURL, address.String()
		} else {
			cfg.ShardURLs[index], cfg.ShardWriterURLs[index] = adminURL, address.String()
		}
	}
	return cfg
}
