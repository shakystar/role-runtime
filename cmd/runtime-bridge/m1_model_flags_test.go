package main

import (
	"flag"
	"os"
	"testing"
)

func TestM1ModelFlagsBeforeState(t *testing.T) {
	const (
		stateErr  = "--state and valid --backend required"
		modelErr  = "--model must be gpt-6.2-sol or gpt-6.2-astra"
		effortErr = "--reasoning-effort must be low, medium, or high"
		lunaErr   = "--model=gpt-6-luna requires --reasoning-effort=high"
	)
	type testCase struct {
		name string
		args []string
		want string
	}
	tests := []testCase{
		{"omitted", nil, stateErr},
		{"sol-default-effort", []string{"--model=gpt-6.2-sol"}, stateErr},
		{"astra-default-effort", []string{"--model=gpt-6.2-astra"}, stateErr},
		{"effort-only", []string{"--reasoning-effort=medium"}, stateErr},
		{"empty-model", []string{"--model="}, modelErr},
		{"empty-effort", []string{"--reasoning-effort="}, effortErr},
		{"unknown-model", []string{"--model=unknown"}, modelErr},
		{"sol-alias", []string{"--model=Sol"}, modelErr},
		{"astra-alias", []string{"--model=Astra"}, modelErr},
		{"luna-alias", []string{"--model=Luna", "--reasoning-effort=high"}, modelErr},
		{"unknown-effort", []string{"--model=gpt-6.2-sol", "--reasoning-effort=unknown"}, effortErr},
		{"luna-high", []string{"--model=gpt-6-luna", "--reasoning-effort=high"}, stateErr},
		{"luna-omitted-effort", []string{"--model=gpt-6-luna"}, lunaErr},
		{"luna-empty-effort", []string{"--model=gpt-6-luna", "--reasoning-effort="}, effortErr},
		{"luna-low", []string{"--model=gpt-6-luna", "--reasoning-effort=low"}, lunaErr},
		{"luna-medium", []string{"--model=gpt-6-luna", "--reasoning-effort=medium"}, lunaErr},
		{"luna-unknown-effort", []string{"--model=gpt-6-luna", "--reasoning-effort=unknown"}, effortErr},
		{"older-luna-high", []string{"--model=gpt-5.6-luna", "--reasoning-effort=high"}, modelErr},
	}
	for _, model := range []string{"gpt-6.2-sol", "gpt-6.2-astra"} {
		for _, effort := range []string{"low", "medium", "high"} {
			tests = append(tests, testCase{model + "/" + effort, []string{"--model=" + model, "--reasoning-effort=" + effort}, stateErr})
		}
	}

	// No --state: accepted routes stop before opening a store or submitting.
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldFlags, oldArgs := flag.CommandLine, os.Args
			defer func() { flag.CommandLine, os.Args = oldFlags, oldArgs }()
			flag.CommandLine = flag.NewFlagSet("runtime-bridge", flag.ContinueOnError)
			os.Args = append([]string{"runtime-bridge", "--backend=codex"}, tc.args...)
			if err := run(); err == nil || err.Error() != tc.want {
				t.Fatalf("run() error = %v, want %q", err, tc.want)
			}
		})
	}
}
