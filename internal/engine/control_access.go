package engine

import (
	"io"
	"os"
	"path/filepath"
)

// Messages returns bounded immutable mailbox records to the owning control server.
func (s *Store) Messages(to Address, visit func(Message) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dir, e := os.Open(filepath.Join(s.Root, "mailbox"))
	if e != nil {
		return e
	}
	defer dir.Close()
	for {
		names, e := dir.Readdirnames(32)
		for _, name := range names {
			if filepath.Ext(name) != ".json" {
				continue
			}
			var m Message
			if err := readJSON(filepath.Join(s.Root, "mailbox", name), &m); err != nil {
				return err
			}
			if err := m.validate(); err != nil {
				return err
			}
			if m.To == to {
				if err := visit(m); err != nil {
					return err
				}
			}
		}
		if e != nil {
			if e == io.EOF {
				return nil
			}
			return e
		}
	}
}
