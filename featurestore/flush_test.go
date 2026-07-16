package featurestore

import (
	"runtime"
	"testing"
	"time"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/domain"
)

// TestFlushOldProjectsCallsWriteFlush verifies that flushOldProjects
// calls WriteFlush on every FeatureView stored in the old projectMap.
func TestFlushOldProjectsCallsWriteFlush(t *testing.T) {
	client := &FeatureStoreClient{
		projectMap: make(map[string]*domain.Project),
	}

	m1 := &domain.MockFeatureView{}
	m2 := &domain.MockFeatureView{}
	m3 := &domain.MockFeatureView{}
	project := &domain.Project{}
	project.FeatureViewMap.Store("fv1", m1)
	project.FeatureViewMap.Store("fv2", m2)
	project.FeatureViewMap.Store("fv3", m3)
	client.projectMap["test_project"] = project

	// Call the actual method under test
	client.flushOldProjects()

	// Allow any goroutine scheduling to settle
	time.Sleep(50 * time.Millisecond)

	if got := m1.FlushCount.Load(); got != 1 {
		t.Errorf("fv1: expected 1 WriteFlush call, got %d", got)
	}
	if got := m2.FlushCount.Load(); got != 1 {
		t.Errorf("fv2: expected 1 WriteFlush call, got %d", got)
	}
	if got := m3.FlushCount.Load(); got != 1 {
		t.Errorf("fv3: expected 1 WriteFlush call, got %d", got)
	}
}

// TestFlushOldProjectsEmptyMap verifies flushOldProjects is safe on empty projectMap.
func TestFlushOldProjectsEmptyMap(t *testing.T) {
	client := &FeatureStoreClient{
		projectMap: make(map[string]*domain.Project),
	}

	client.flushOldProjects()
}

// TestFlushOldProjectsNilMap verifies flushOldProjects is safe on nil projectMap.
func TestFlushOldProjectsNilMap(t *testing.T) {
	client := &FeatureStoreClient{}

	client.flushOldProjects()
}

// TestStopCallsFlushOldProjects verifies that Stop() flushes all FeatureView
// DAO background goroutines, preventing leaks on shutdown.
func TestStopCallsFlushOldProjects(t *testing.T) {
	client := &FeatureStoreClient{
		projectMap: make(map[string]*domain.Project),
		stopChan:   make(chan struct{}),
	}

	m1 := &domain.MockFeatureView{}
	project := &domain.Project{}
	project.FeatureViewMap.Store("fv1", m1)
	client.projectMap["test_project"] = project

	client.Stop()

	time.Sleep(50 * time.Millisecond)

	if got := m1.FlushCount.Load(); got != 1 {
		t.Errorf("expected 1 WriteFlush call after Stop, got %d", got)
	}
}

// TestNoGoroutineLeakOnEmptyRefresh verifies that repeated flushOldProjects
// calls on an empty projectMap do not spawn or leak goroutines.
func TestNoGoroutineLeakOnEmptyRefresh(t *testing.T) {
	before := runtime.NumGoroutine()

	client := &FeatureStoreClient{
		projectMap: make(map[string]*domain.Project),
	}

	for i := 0; i < 5; i++ {
		client.flushOldProjects()
		client.projectMap = make(map[string]*domain.Project)
	}

	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()

	if after > before+5 {
		t.Errorf("goroutine leak: before=%d, after=%d", before, after)
	}
}
