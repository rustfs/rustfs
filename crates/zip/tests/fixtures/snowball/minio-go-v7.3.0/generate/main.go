// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const minioGoVersion = "v7.3.0"

type fixtureManifest struct {
	Generator   string           `json:"generator"`
	MinioGo     string           `json:"minio_go"`
	GeneratedAt string           `json:"generated_at"`
	Objects     []fixtureObject  `json:"objects"`
	Archives    []fixtureArchive `json:"archives"`
}

type fixtureObject struct {
	Key       string              `json:"key"`
	Body      string              `json:"body"`
	ModTime   string              `json:"mod_time"`
	VersionID string              `json:"version_id,omitempty"`
	Headers   map[string][]string `json:"headers,omitempty"`
}

type fixtureArchive struct {
	File       string `json:"file"`
	Compressed bool   `json:"compressed"`
	Length     int    `json:"length"`
	SHA256     string `json:"sha256"`
}

func objects() []fixtureObject {
	return []fixtureObject{
		{
			Key:       "alpha.txt",
			Body:      "alpha-body",
			ModTime:   "2024-01-02T03:04:05Z",
			VersionID: "018cc251-f400-7c22-9e8d-8b1800000001",
			Headers: map[string][]string{
				"Content-Type":     {"text/plain"},
				"X-Amz-Meta-Owner": {"snowball-fixture"},
				"X-Amz-Tagging":    {"project=rustfs&source=minio-go"},
			},
		},
		{
			Key:     "nested/世界.txt",
			Body:    "bravo-body",
			ModTime: "2024-01-02T03:05:05Z",
			Headers: map[string][]string{
				"Content-Language": {"zh-CN"},
				"X-Amz-Meta-Note":  {"unicode-path"},
			},
		},
	}
}

func captureSnowball(compressed bool, specs []fixtureObject) ([]byte, error) {
	body := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		payload, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		body <- payload
		writer.Header().Set("ETag", `"snowball-fixture"`)
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{
		// The S3 authentication layer removes AWS streaming-signature framing
		// before Snowball extraction sees the request body. Anonymous signing
		// captures those decoded archive bytes directly.
		Creds:  credentials.NewStatic("", "", "", credentials.SignatureAnonymous),
		Secure: false,
		Region: "us-east-1",
	})
	if err != nil {
		return nil, fmt.Errorf("construct minio client: %w", err)
	}

	input := make(chan minio.SnowballObject, len(specs))
	for _, spec := range specs {
		modTime, err := time.Parse(time.RFC3339, spec.ModTime)
		if err != nil {
			return nil, fmt.Errorf("parse mod time for %q: %w", spec.Key, err)
		}
		headers := make(http.Header, len(spec.Headers))
		for name, values := range spec.Headers {
			headers[name] = append([]string(nil), values...)
		}
		input <- minio.SnowballObject{
			Key:       spec.Key,
			Size:      int64(len(spec.Body)),
			ModTime:   modTime,
			Content:   bytes.NewReader([]byte(spec.Body)),
			VersionID: spec.VersionID,
			Headers:   headers,
		}
	}
	close(input)

	err = client.PutObjectsSnowball(context.Background(), "fixture-bucket", minio.SnowballOptions{
		Opts: minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		},
		InMemory: true,
		Compress: compressed,
	}, input)
	if err != nil {
		return nil, fmt.Errorf("generate snowball request: %w", err)
	}
	return <-body, nil
}

func main() {
	outDir := flag.String("out", "..", "fixture output directory")
	flag.Parse()

	specs := objects()
	archives := make([]fixtureArchive, 0, 2)
	for _, fixture := range []struct {
		name       string
		compressed bool
	}{
		{name: "snowball.tar"},
		{name: "snowball.tar.s2", compressed: true},
	} {
		payload, err := captureSnowball(fixture.compressed, specs)
		if err != nil {
			panic(err)
		}
		path := filepath.Join(*outDir, fixture.name)
		if err := os.WriteFile(path, payload, 0o644); err != nil {
			panic(fmt.Errorf("write %s: %w", path, err))
		}
		digest := sha256.Sum256(payload)
		archives = append(archives, fixtureArchive{
			File:       fixture.name,
			Compressed: fixture.compressed,
			Length:     len(payload),
			SHA256:     hex.EncodeToString(digest[:]),
		})
	}

	manifest := fixtureManifest{
		Generator:   "github.com/minio/minio-go/v7.Client.PutObjectsSnowball",
		MinioGo:     minioGoVersion,
		GeneratedAt: "2026-09-05T00:00:00Z",
		Objects:     specs,
		Archives:    archives,
	}
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		panic(err)
	}
	payload = append(payload, '\n')
	path := filepath.Join(*outDir, "manifest.json")
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		panic(fmt.Errorf("write %s: %w", path, err))
	}
}
