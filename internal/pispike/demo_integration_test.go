//go:build integration

package pispike

import (
	"context"
	"testing"
	"time"
)

func TestRealPiFortyRolesFourWorkers(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		name := "reuse"
		if fresh {
			name = "fresh-process-baseline"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			report, err := Run(ctx, Options{Repository: "../..", StateDir: t.TempDir(), Roles: 40, Workers: 4, Rounds: 2, Queue: 8, Fresh: fresh})
			if err != nil {
				t.Fatal(err)
			}
			if report.Stats.Succeeded != 80 || report.Stats.Failed != 0 || report.Stats.MaxActive != 4 || report.Stats.MaxQueued > 8 {
				t.Fatalf("%+v", report)
			}
			want := int64(4)
			if fresh {
				want = 80
			}
			if report.ProcessesStarted != want {
				t.Fatalf("process starts: %+v", report)
			}
			if !fresh && report.UniquePIDs != 4 {
				t.Fatalf("expected 4 actual pi processes: %+v", report)
			}
			for worker, count := range report.WorkerRoleCounts {
				if count < 2 {
					t.Fatalf("worker %d did not switch roles", worker)
				}
			}
			t.Logf("report: %+v", report)
		})
	}
}
