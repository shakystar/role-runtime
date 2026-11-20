// Receipt inspection is read-only and can run while the execution owner is active.
package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/shakystar/role-runtime/internal/engine"
)

type inspection struct {
	Version int    `json:"version"`
	Run     string `json:"run_id"`
	Attempt string `json:"attempt_id"`
	Status  string `json:"status"`
	Wire    []byte `json:"wire,omitempty"`
	Receipt []byte `json:"receipt,omitempty"`
}

func inspectAttempt(root, run, attempt string, out io.Writer) error {
	if root == "" || !identifier.MatchString(run) || !identifier.MatchString(attempt) {
		return errors.New("invalid inspection identity")
	}
	result := inspection{Version: 1, Run: run, Attempt: attempt, Status: "absent"}
	dir := filepath.Join(root, "bridge-attempts")
	data, err := os.ReadFile(filepath.Join(dir, "attempt-"+engine.Digest(attempt)+".json"))
	if os.IsNotExist(err) {
		return json.NewEncoder(out).Encode(result)
	}
	if err != nil {
		return err
	}
	var id string
	if json.Unmarshal(data, &id) != nil || !identifier.MatchString(id) {
		return errors.New("invalid attempt binding")
	}
	wire, err := os.ReadFile(filepath.Join(dir, engine.Digest(id)+".json"))
	if os.IsNotExist(err) {
		return json.NewEncoder(out).Encode(result)
	}
	if err != nil {
		return err
	}
	var req request
	if json.Unmarshal(wire, &req) != nil || req.ID != id || req.SchemaVersion != 1 || req.RunID != run || req.AttemptID != attempt {
		return errors.New("receipt identity mismatch")
	}
	receipt, err := os.ReadFile(filepath.Join(root, "tasks", engine.Digest(id)+".json"))
	if os.IsNotExist(err) {
		return json.NewEncoder(out).Encode(result)
	}
	if err != nil {
		return err
	}
	var rec engine.Record
	if json.Unmarshal(receipt, &rec) != nil || rec.Task.ID != id || rec.Task.Role != req.Role || rec.Task.Input != req.Input {
		return errors.New("invalid bound receipt")
	}
	result.Wire, result.Receipt, result.Status = wire, receipt, "observed"
	return json.NewEncoder(out).Encode(result)
}
