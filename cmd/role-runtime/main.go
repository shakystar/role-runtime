package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/shakystar/role-runtime/internal/pispike"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "demo" {
		fmt.Fprintln(os.Stderr, "usage: role-runtime demo [--roles 40 --workers 4 --rounds 2 --queue 8 --fresh]")
		os.Exit(2)
	}
	flags := flag.NewFlagSet("demo", flag.ExitOnError)
	var opt pispike.Options
	flags.StringVar(&opt.Repository, "repo", ".", "repository root containing locked pi installation")
	flags.StringVar(&opt.StateDir, "state", "", "optional new state directory; default is temporary")
	flags.IntVar(&opt.Roles, "roles", 40, "logical role count")
	flags.IntVar(&opt.Workers, "workers", 4, "active worker count")
	flags.IntVar(&opt.Rounds, "rounds", 2, "tasks per role")
	flags.IntVar(&opt.Queue, "queue", 8, "pending queue bound")
	flags.BoolVar(&opt.Fresh, "fresh", false, "baseline: start a fresh pi process per task")
	_ = flags.Parse(os.Args[2:])
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	report, err := pispike.Run(ctx, opt)
	_ = json.NewEncoder(os.Stdout).Encode(report)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
