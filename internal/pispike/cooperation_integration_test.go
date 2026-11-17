//go:build integration

package pispike

import (
	"context"
	"testing"
	"time"
)

func TestRealPiCooperation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	report, err := RunCooperation(ctx, "../..")
	if err != nil {
		t.Fatal(err)
	}
	if !report.AllFourWaiting || report.Completed != 8 || report.Suspended != 8 || report.Resumed != 8 || report.Steps != 16 || report.MaxActive != 4 || report.ProcessesStarted != 4 || report.Heartbeats <= 16 {
		t.Fatalf("unexpected report: %+v", report)
	}
	t.Logf("report: %+v", report)
}

func TestRealPiCooperationAfterRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	report, err := runCooperation(ctx, "../..", true)
	if err != nil {
		t.Fatal(err)
	}
	if !report.AllFourWaiting || report.Completed != 8 || report.Suspended != 8 || report.Resumed != 8 || report.Steps != 16 || report.MaxActive != 4 || report.ProcessesStarted != 8 {
		t.Fatalf("unexpected restart report: %+v", report)
	}
	t.Logf("restart report: %+v", report)
}
