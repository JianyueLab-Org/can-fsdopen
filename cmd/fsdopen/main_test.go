package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"golang.org/x/crypto/bcrypt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHashPasswordPipedInputNeverEchoes(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := writer.WriteString("synthetic-secret\n"); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	var out, diagnostics bytes.Buffer
	if err := run([]string{"hash-password"}, reader, &out, &diagnostics); err != nil {
		t.Fatal(err)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(strings.TrimSpace(out.String())), []byte("synthetic-secret")); err != nil {
		t.Fatal("invalid generated hash")
	}
	if strings.Contains(out.String()+diagnostics.String(), "synthetic-secret") {
		t.Fatal("password echoed")
	}
}
func TestHashPasswordRejectsPlaintextArgumentsAndEmptyInput(t *testing.T) {
	for _, args := range [][]string{{"hash-password", "secret"}, {"hash-password", "-password", "secret"}, {"hash-password"}} {
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		writer.Close()
		var out, diagnostics bytes.Buffer
		err = run(args, reader, &out, &diagnostics)
		reader.Close()
		if err == nil || out.Len() != 0 {
			t.Fatal("accepted unsafe credential input")
		}
	}
}

func TestListenerFailurePreservesOperatorCause(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	hash, err := bcrypt.GenerateFromPassword([]byte("synthetic-secret"), 12)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	users, err := json.Marshal(map[string]any{"users": []map[string]any{{"id": "900001", "password_hash": string(hash), "rating": 3}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "users.json"), users, 0600); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{"users_file": "users.json", "listen_addrs": []string{listener.Addr().String()}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	var out, diagnostics bytes.Buffer
	err = run([]string{"-config", path}, os.Stdin, &out, &diagnostics)
	var cause *net.OpError
	if !errors.As(err, &cause) || cause.Op != "listen" {
		t.Fatalf("listener cause lost: %v", err)
	}
	if !strings.Contains(err.Error(), listener.Addr().String()) {
		t.Fatal("listener address missing from operator error")
	}
	for _, secret := range []string{"synthetic-secret", "900001", string(hash)} {
		if strings.Contains(err.Error()+out.String()+diagnostics.String(), secret) {
			t.Fatal("credential input exposed by listener failure")
		}
	}
}
