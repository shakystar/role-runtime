package engine

import "errors"

// ExecutionFailure is scheduler-owned receipt metadata, independent of a harness.
// Retryable is adapter eligibility, never an automatic resubmission instruction.
type ExecutionFailure struct {
	Version   int    `json:"version"`
	Stage     string `json:"stage"`
	Code      string `json:"code"`
	Execution string `json:"execution"`
	Retryable bool   `json:"retryable"`
}

func FailureMetadata(err error) *ExecutionFailure {
	var source interface {
		ExecutionFailure() (string, string, string, bool)
	}
	if !errors.As(err, &source) {
		return nil
	}
	stage, code, state, retry := source.ExecutionFailure()
	if len(stage) > 64 || len(code) > 64 {
		return nil
	}
	switch state {
	case "not_started", "unknown", "started", "completed":
	default:
		return nil
	}
	return &ExecutionFailure{Version: 1, Stage: stage, Code: code, Execution: state, Retryable: retry && state == "not_started"}
}
