// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

package compat_test

import (
	"context"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	madmin "github.com/minio/madmin-go/v3"
)

type fixture struct {
	Name     string          `json:"name"`
	CLIExit  int             `json:"cliExit"`
	Response json.RawMessage `json:"response"`
}

func fixtures(t *testing.T) []fixture {
	t.Helper()
	data, err := os.ReadFile("../../../crates/madmin/tests/fixtures/heal-outcome-v3.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []fixture
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) != 8 {
		t.Fatalf("expected eight owner/receiver-validated fixtures, got %d", len(cases))
	}
	return cases
}

func fixtureServer(t *testing.T, response []byte, polls *atomic.Int32) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/minio/admin/v3/heal/") {
			t.Errorf("unexpected client request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("clientToken") == "" {
			fmt.Fprint(w, `{"clientToken":"fixture-token","clientAddress":"","startTime":"2026-01-01T00:00:00Z"}`)
			return
		}
		polls.Add(1)
		w.Write(response)
	}))
	t.Cleanup(server.Close)
	return server
}

func TestLegacyMadminDecoder(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.Name, func(t *testing.T) {
			var polls atomic.Int32
			server := fixtureServer(t, f.Response, &polls)
			client, err := madmin.New(strings.TrimPrefix(server.URL, "http://"), "fixture-access", "fixture-secret", false)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, status, err := client.Heal(ctx, "bucket", "", madmin.HealOpts{}, "fixture-token", false, false)
			if err != nil {
				t.Fatal(err)
			}
			var expected struct {
				Summary string `json:"summary"`
				Detail  string `json:"detail"`
			}
			if err := json.Unmarshal(f.Response, &expected); err != nil {
				t.Fatal(err)
			}
			if status.Summary != expected.Summary || status.FailureDetail != expected.Detail || polls.Load() != 1 {
				t.Fatalf("decoder changed legacy fields: %+v, polls=%d", status, polls.Load())
			}
		})
	}
}

func TestLegacyMCPoll(t *testing.T) {
	binary := os.Getenv("MC_BINARY")
	if binary == "" {
		t.Fatal("MC_BINARY must point to the pinned mc release; this check cannot be skipped")
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	if info.Main.Path != "github.com/minio/mc" || !strings.Contains(info.Main.Version, "7394ce0dd2a8") {
		t.Fatalf("expected mc RELEASE.2025-08-13T08-35-41Z (7394ce0dd2a8), got %+v", info.Main)
	}
	for _, f := range fixtures(t) {
		t.Run(f.Name, func(t *testing.T) {
			var polls atomic.Int32
			server := fixtureServer(t, f.Response, &polls)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "--config-dir", filepath.Join(t.TempDir(), "mc"), "--json", "admin", "heal", "--recursive", "w23/bucket")
			cmd.Env = append(os.Environ(), "MC_HOST_w23="+strings.Replace(server.URL, "http://", "http://fixture-access:fixture-secret@", 1), "MC_NO_COLOR=1")
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("legacy poll did not terminate: %s", output)
			}
			exit := 0
			if err != nil {
				var ok bool
				var status *exec.ExitError
				status, ok = err.(*exec.ExitError)
				if !ok {
					t.Fatal(err)
				}
				exit = status.ExitCode()
			}
			if exit != f.CLIExit || polls.Load() != 1 {
				t.Fatalf("exit=%d expected=%d polls=%d output=%s", exit, f.CLIExit, polls.Load(), output)
			}
			if f.CLIExit != 0 && !strings.Contains(string(output), "Heal had an error") {
				t.Fatalf("failure was not the expected legacy terminal result: %s", output)
			}
		})
	}
}
