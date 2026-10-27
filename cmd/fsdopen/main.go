// Command fsdopen runs the standalone FSD server or hashes a local password.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/JianyueLab-Org/can-fsdopen/auth"
	"github.com/JianyueLab-Org/can-fsdopen/config"
	"github.com/JianyueLab-Org/can-fsdopen/fsd"
	"golang.org/x/term"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string, stdin *os.File, stdout, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "hash-password" {
		if len(args) != 1 {
			return errors.New("usage: fsdopen hash-password (password is read from terminal or stdin, never arguments)")
		}
		return hashPassword(stdin, stdout, stderr)
	}
	flags := flag.NewFlagSet("fsdopen", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("config", "config.json", "configuration file")
	debug := flags.Bool("debug", false, "enable debug event logging")
	if err := flags.Parse(args); err != nil {
		return errors.New("usage: fsdopen [-config path] [-debug]")
	}
	if flags.NArg() != 0 {
		return errors.New("usage: fsdopen [-config path] [-debug]")
	}
	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level})))
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	if cfg.CredentialsFile == "" {
		return errors.New("startup: users_file is required; generate bcrypt hashes with fsdopen hash-password and provide a users document")
	}
	users, err := auth.Load(cfg.CredentialsFile)
	if err != nil {
		return err
	}
	store := config.NewStore(*path, cfg)
	server, err := fsd.NewServer(fsd.Options{Config: store, Users: users})
	if err != nil {
		return errors.New("startup: cannot initialize FSD server")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	reloadDone := make(chan struct{})
	go func() {
		defer close(reloadDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				if _, err := store.Reload(); err != nil {
					slog.Error("configuration reload rejected", "error", err)
				} else {
					slog.Info("configuration reloaded; credentials remain unchanged until restart")
				}
			}
		}
	}()
	err = server.Run(ctx)
	cancel()
	<-reloadDone
	if !server.WaitConnections(10 * time.Second) {
		slog.Warn("connection shutdown grace period elapsed")
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("FSD listener failed: %w", err)
	}
	return nil
}
func hashPassword(stdin *os.File, stdout, stderr io.Writer) error {
	var password []byte
	var err error
	if term.IsTerminal(int(stdin.Fd())) {
		fmt.Fprint(stderr, "Password: ")
		password, err = term.ReadPassword(int(stdin.Fd()))
		fmt.Fprintln(stderr)
	} else {
		password, err = io.ReadAll(io.LimitReader(stdin, 76))
		password = []byte(strings.TrimSuffix(strings.TrimSuffix(string(password), "\n"), "\r"))
	}
	if err != nil {
		return errors.New("cannot read password from terminal or stdin")
	}
	defer clear(password)
	hash, err := auth.HashPassword(string(password))
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(stdout, hash); err != nil {
		return errors.New("cannot write password hash")
	}
	return nil
}
