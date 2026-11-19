// workflowprobe is a synthetic SMS/checkpoint feasibility fixture, not a service API.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/shakystar/role-runtime/internal/engine"
	"os"
	"path/filepath"
	"time"
)

func task(id string) engine.Task {
	return engine.Task{ID: id, Project: "probe", Role: id, Version: "v1", Instance: "main", Input: "synthetic"}
}
func message() engine.Message {
	return engine.Message{ID: "document-ready", From: engine.AddressOf(task("producer")), To: engine.AddressOf(task("consumer")), Correlation: "document-v1", Body: "artifact.json"}
}
func scan(s *engine.Store) error {
	return s.ScanCooperative(func(engine.CooperativeRecord) (bool, error) { return false, nil })
}
func run() error {
	root := flag.String("state", "", "fixture state")
	action := flag.String("action", "observe", "fixture operation")
	flag.Parse()
	if *root == "" {
		return errors.New("state required")
	}
	s, err := engine.OpenStore(*root)
	if err != nil {
		return err
	}
	defer s.Close()
	m := message()
	lease := func(id string) (engine.CooperativeRecord, error) {
		return s.LeaseCooperative(id, time.Now(), time.Minute)
	}
	switch *action {
	case "setup":
		for _, id := range []string{"producer", "consumer"} {
			if err = s.SubmitCooperative(task(id)); err != nil {
				return err
			}
		}
		r, e := lease("consumer")
		if e != nil {
			return e
		}
		err = s.FinishCooperative("consumer", r.Generation, engine.Transition{Checkpoint: "await-document", Wait: &engine.WaitFor{From: m.From, Correlation: m.Correlation}}, time.Now())
	case "early", "duplicate":
		err = s.Deliver(m)
	case "wrong":
		m.Correlation = "other-document"
		err = s.Deliver(m)
	case "complete":
		r, e := lease("producer")
		if e != nil {
			return e
		}
		data := []byte(`{"synthetic":true,"document":"ready"}`)
		if err = os.WriteFile(filepath.Join(*root, "artifact.json"), data, 0600); err != nil {
			return err
		}
		hash := sha256.Sum256(data)
		ref, _ := json.Marshal(map[string]string{"sha256": hex.EncodeToString(hash[:])})
		err = s.FinishCooperative("producer", r.Generation, engine.Transition{Text: string(ref), Send: []engine.Message{m}}, time.Now())
	case "fail":
		r, e := lease("producer")
		if e != nil {
			return e
		}
		err = s.FenceCooperative("producer", r.Generation, errors.New("synthetic worker loss"))
	case "observe", "consume":
	default:
		return errors.New("unsupported fixture action")
	}
	if err != nil {
		return err
	}
	if err = scan(s); err != nil {
		return err
	}
	producer, err := s.CooperativeReceipt("producer")
	if err != nil {
		return err
	}
	consumer, err := s.CooperativeReceipt("consumer")
	if err != nil {
		return err
	}
	valid := false
	if producer.Status == "completed" {
		var ref map[string]string
		data, e := os.ReadFile(filepath.Join(*root, "artifact.json"))
		hash := sha256.Sum256(data)
		valid = e == nil && json.Unmarshal([]byte(producer.Text), &ref) == nil && ref["sha256"] == hex.EncodeToString(hash[:])
	}
	ready := valid && consumer.Status == "ready" && consumer.Inbox != nil && *consumer.Inbox == m
	cached := consumer.Status == "completed"
	if *action == "consume" && !cached {
		if !ready {
			return errors.New("completion evidence required")
		}
		r, e := lease("consumer")
		if e != nil {
			return e
		}
		if err = s.FinishCooperative("consumer", r.Generation, engine.Transition{Text: "synthetic document consumed"}, time.Now()); err != nil {
			return err
		}
		consumer, err = s.CooperativeReceipt("consumer")
		if err != nil {
			return err
		}
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"producer": producer.Status, "consumer": consumer.Status, "artifact_valid": valid, "ready": ready, "cached": cached, "consumer_generation": consumer.Generation, "usage": nil, "cost": nil})
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
