package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	_ "embed"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"
)

var (
	//go:embed pglite-wasi.tar.gz
	compressed []byte
)

// PGLite wraps a PostgreSQL instance running via WebAssembly (wazero).
type PGLite struct {
	runtime wazero.Runtime
	mod     api.Module
	ctx     context.Context
	stdout  io.Writer
	stderr  io.Writer
}

// NewPGLite creates and initializes a PGLite instance. The stdout and stderr
// writers receive PostgreSQL output. Note: the PGLite WASM module redirects
// query output to stderr. An optional wazero.RuntimeConfig can be provided;
// if nil, the default (compiler) config is used. The caller must call Close
// when done.
func NewPGLite(ctx context.Context, stdout, stderr io.Writer, rtConfig ...wazero.RuntimeConfig) (*PGLite, error) {
	blob, err := setupEnv()
	if err != nil {
		return nil, fmt.Errorf("setupEnv: %w", err)
	}

	var r wazero.Runtime
	if len(rtConfig) > 0 && rtConfig[0] != nil {
		r = wazero.NewRuntimeWithConfig(ctx, rtConfig[0])
	} else {
		r = wazero.NewRuntime(ctx)
	}

	fsConfig := wazero.NewFSConfig().
		WithDirMount("./tmp", "/tmp").
		WithDirMount("./dev", "/dev")

	config := wazero.NewModuleConfig().
		WithStdout(stdout).
		WithStderr(stderr).
		WithFSConfig(fsConfig)

	wasi_snapshot_preview1.MustInstantiate(ctx, r)

	mod, err := r.InstantiateWithConfig(
		ctx,
		blob,
		config.
			WithArgs("--single", "postgres").
			WithEnv("ENVIRONMENT", "wasi-embed").
			WithEnv("REPL", "N").
			WithEnv("PGUSER", "postgres").
			WithEnv("PGDATABASE", "postgres"),
	)
	if err != nil {
		if exitErr, ok := err.(*sys.ExitError); ok && exitErr.ExitCode() != 0 {
			r.Close(ctx)
			return nil, fmt.Errorf("wasm exit_code: %d", exitErr.ExitCode())
		} else if !ok {
			r.Close(ctx)
			return nil, fmt.Errorf("instantiate: %w", err)
		}
	}

	p := &PGLite{
		runtime: r,
		mod:     mod,
		ctx:     ctx,
		stdout:  stdout,
		stderr:  stderr,
	}

	initDBRV, err := mod.ExportedFunction("pg_initdb").Call(ctx)
	if err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("pg_initdb: %w", err)
	}
	fmt.Fprintf(stderr, "initdb returned: %b\n", initDBRV)

	_, err = mod.ExportedFunction("use_socketfile").Call(ctx)
	if err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("use_socketfile: %w", err)
	}

	return p, nil
}

// Query executes a SQL statement. Output is written to the configured stderr
// writer (the PGLite WASM module directs query output to stderr).
func (p *PGLite) Query(sql string) error {
	sqlCstring := append([]byte(sql), 0)
	p.mod.Memory().Write(1, sqlCstring)

	_, err := p.mod.ExportedFunction("interactive_one").Call(p.ctx)
	return err
}

// RunQueries splits input on blank lines and executes each non-empty query.
func (p *PGLite) RunQueries(input string) error {
	for _, line := range strings.Split(input, "\n\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			fmt.Fprintf(p.stderr, "REPL: %s\n", line)
			if err := p.Query(line); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close releases all resources held by the PGLite instance.
func (p *PGLite) Close() {
	if p.runtime != nil {
		p.runtime.Close(p.ctx)
	}
}

func setupEnv() ([]byte, error) {
	if _, err := os.Stat("./tmp/pglite/base/PG_VERSION"); err != nil {
		fmt.Println("Extracting env....")
		gr, err := gzip.NewReader(bytes.NewReader(compressed))
		if err != nil {
			return nil, err
		}
		defer gr.Close()

		tr := tar.NewReader(gr)

		for {
			header, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}

			dest := filepath.Join("./", header.Name)

			switch header.Typeflag {
			case tar.TypeDir:
				if err := os.MkdirAll(dest, os.FileMode(header.Mode)); err != nil {
					return nil, err
				}
			case tar.TypeReg:
				if err := os.MkdirAll(filepath.Dir(dest), os.FileMode(header.Mode)); err != nil {
					return nil, err
				}

				of, err := os.Create(dest)
				if err != nil {
					return nil, err
				}
				defer of.Close()

				if _, err := io.Copy(of, tr); err != nil {
					return nil, err
				}
			case tar.TypeSymlink:
				if err := os.Symlink(header.Linkname, dest); err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("unknown file type in tar: %c (%s)", header.Typeflag, header.Name)
			}
		}
	}

	if err := os.MkdirAll("./dev", 0755); err != nil {
		return nil, err
	}

	rf, err := os.Create("./dev/urandom")
	if err != nil {
		return nil, err
	}
	defer rf.Close()

	rng := make([]byte, 128)
	if _, err := rand.Read(rng); err != nil {
		return nil, err
	}
	rf.Write(rng)

	return os.ReadFile("./tmp/pglite/bin/postgres.wasi")
}
