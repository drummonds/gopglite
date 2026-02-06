package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
)

var testPG *PGLite

func TestMain(m *testing.M) {
	// If running as subprocess for output capture, handle that
	if os.Getenv("PGLITE_SUBPROCESS") == "1" {
		runSubprocess()
		return
	}

	ctx := context.Background()

	var err error
	testPG, err = NewPGLite(ctx, os.Stdout, os.Stderr)
	if err != nil {
		panic("failed to initialize PGLite: " + err.Error())
	}

	code := m.Run()

	testPG.Close()
	os.Exit(code)
}

// runSubprocess runs queries passed via PGLITE_QUERIES env var and exits.
// Output goes to stderr (where PGLite writes query results).
func runSubprocess() {
	ctx := context.Background()
	pg, err := NewPGLite(ctx, os.Stdout, os.Stderr)
	if err != nil {
		os.Stderr.WriteString("INIT_ERROR: " + err.Error() + "\n")
		os.Exit(1)
	}

	queries := os.Getenv("PGLITE_QUERIES")
	for _, q := range strings.Split(queries, ";;") {
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}
		if err := pg.Query(q); err != nil {
			os.Stderr.WriteString("QUERY_ERROR: " + err.Error() + "\n")
			os.Exit(1)
		}
	}
	pg.Close()
	os.Exit(0)
}

// runQueriesInSubprocess executes queries in a fresh subprocess and returns
// the combined output. Queries are separated by ";;".
func runQueriesInSubprocess(t *testing.T, queries string) string {
	t.Helper()

	// Get the test binary path
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run", "^$")
	cmd.Env = append(os.Environ(),
		"PGLITE_SUBPROCESS=1",
		"PGLITE_QUERIES="+queries,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("subprocess failed: %v\noutput: %s", err, string(output))
	}
	return string(output)
}

func TestShowClientEncoding(t *testing.T) {
	output := runQueriesInSubprocess(t, "SHOW client_encoding;")
	if !strings.Contains(output, "UTF8") {
		t.Errorf("expected output to contain 'UTF8', got: %s", output)
	}
}

func TestCreateAndCallFunction(t *testing.T) {
	queries := "CREATE OR REPLACE FUNCTION test_func() RETURNS TEXT AS $$ BEGIN RETURN 'test'; END; $$ LANGUAGE plpgsql;;;" +
		"SELECT test_func();"
	output := runQueriesInSubprocess(t, queries)
	if !strings.Contains(output, "test") {
		t.Errorf("expected output to contain 'test', got: %s", output)
	}
}

func TestArithmeticFunction(t *testing.T) {
	createSQL := `CREATE OR REPLACE FUNCTION addition (entier1 integer, entier2 integer) RETURNS integer LANGUAGE plpgsql IMMUTABLE AS 'DECLARE resultat integer; BEGIN resultat := entier1 + entier2; RETURN resultat; END';`
	queries := createSQL + ";;;" + "SELECT addition(40,2);"
	output := runQueriesInSubprocess(t, queries)
	if !strings.Contains(output, "42") {
		t.Errorf("expected output to contain '42', got: %s", output)
	}
}

func TestSelectNow(t *testing.T) {
	output := runQueriesInSubprocess(t, "SELECT now(), current_database(), session_user, current_user;")
	if !strings.Contains(output, "postgres") {
		t.Errorf("expected output to contain 'postgres', got: %s", output)
	}
}
