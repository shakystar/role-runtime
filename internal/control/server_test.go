package control

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/shakystar/role-runtime/internal/engine"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLocalControlBoundaries(t *testing.T) {
	root, e := os.MkdirTemp("/tmp", "control-")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(root)
	store, e := engine.OpenStore(filepath.Join(root, "state"))
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	if e = store.EnableLifecycle("run", 0); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &Server{Store: store, Socket: filepath.Join(root, "state", "control", "runtime.sock"), Config: Config{RunID: "run", Project: "p", Consumers: map[string]engine.Address{"graph": {Project: "p", Role: "r", Version: "v1", Instance: "main"}}}}
	if e = s.Start(ctx); e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	info, _ := os.Stat(s.Socket)
	if info.Mode().Perm() != 0600 {
		t.Fatal("socket permissions")
	}
	call := func(wire string) map[string]any {
		t.Helper()
		c, e := net.Dial("unix", s.Socket)
		if e != nil {
			t.Fatal(e)
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(time.Second))
		if _, e = c.Write([]byte(wire + "\n")); e != nil {
			t.Fatal(e)
		}
		data, e := bufio.NewReader(c).ReadBytes('\n')
		if e != nil {
			t.Fatal(e)
		}
		var out map[string]any
		if json.Unmarshal(data, &out) != nil {
			t.Fatal("invalid response")
		}
		return out
	}
	good := `{"version":1,"request_id":"q","run_id":"run","method":"health.get","params":{}}`
	if call(good)["ok"] != true {
		t.Fatal("health failed")
	}
	for _, wire := range []string{strings.Replace(good, `"version":1`, `"version":1,"version":1`, 1), strings.Replace(good, `"version":1`, `"Version":1`, 1), strings.Replace(good, `"params":{}`, `"params":null`, 1)} {
		if call(wire)["ok"] != false {
			t.Fatal("malformed accepted")
		}
	}
	wrong := strings.Replace(good, `"run"`, `"other"`, 1)
	reply := call(wrong)
	if reply["error"].(map[string]any)["code"] != "scope_mismatch" || reply["run_id"] != "other" {
		t.Fatal("scope response")
	}
	for _, params := range []string{`{"after_cursor":null,"limit":101}`, `{"after_cursor":"other:0","limit":1}`} {
		wire := `{"version":1,"request_id":"q","run_id":"run","method":"events.list","params":` + params + `}`
		if call(wire)["ok"] != false {
			t.Fatal("invalid page accepted")
		}
	}
	// Four idle clients occupy the fixed slots; a fifth gets a bounded busy reply.
	clients := []net.Conn{}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()
	for i := 0; i < 4; i++ {
		c, e := net.Dial("unix", s.Socket)
		if e != nil {
			t.Fatal(e)
		}
		clients = append(clients, c)
	}
	time.Sleep(30 * time.Millisecond)
	if call(good)["error"].(map[string]any)["code"] != "busy" {
		t.Fatal("unbounded handlers")
	}
	for _, c := range clients {
		c.Close()
	}
}
func TestArtifactConfinedToRoot(t *testing.T) {
	store, e := engine.OpenStore(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	os.MkdirAll(filepath.Join(store.Root, "artifacts"), 0700)
	outside := filepath.Join(t.TempDir(), "secret")
	os.WriteFile(outside, []byte("secret"), 0600)
	if e = os.Symlink(outside, filepath.Join(store.Root, "artifacts", "document")); e != nil {
		t.Fatal(e)
	}
	s := &Server{Store: store}
	if s.artifactOK(Artifact{ID: "document", Version: "v1", SHA256: strings.Repeat("0", 64)}) {
		t.Fatal("symlink escape")
	}
	if s.artifactOK(Artifact{ID: "../secret", Version: "v1", SHA256: strings.Repeat("0", 64)}) {
		t.Fatal("path escape")
	}
}
