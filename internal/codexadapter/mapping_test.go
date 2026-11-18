package codexadapter

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

type mapping struct {
	Key          string `json:"key"`
	ThreadID     string `json:"threadId"`
	Workspace    string `json:"workspace"`
	Instructions string `json:"instructionDigest"`
	ActiveTask   string `json:"activeTask,omitempty"`
}

func researchProfile(tools []string) (bool, error) {
	if len(tools) == 1 {
		switch tools[0] {
		case "advisory":
			return false, nil
		case "web-research":
			return true, nil
		}
	}
	return false, errors.New("codex adapter supports exactly tools=[advisory] or tools=[web-research]")
}

func readMapping(path string) (m mapping, err error) {
	f, err := os.Open(path)
	if err != nil {
		return m, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 8193))
	if err != nil {
		return m, err
	}
	if len(b) > 8192 {
		return m, errors.New("oversized codex mapping")
	}
	err = json.Unmarshal(b, &m)
	return m, err
}
func writeMapping(path string, m mapping) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".codex-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
