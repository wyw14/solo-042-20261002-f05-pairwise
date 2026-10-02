package circuit

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestFaultedReservationRequiresRecovery(t *testing.T) {
	manager := NewManager("C-A")
	sessionID := uuid.New()
	if _, err := manager.Reserve("C-A", "V-01", sessionID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := manager.RetainFault("C-A", sessionID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Release("C-A", sessionID); err == nil {
		t.Fatal("faulted reservation was released normally")
	}
	if err := manager.Recover("C-A", sessionID); err != nil {
		t.Fatal(err)
	}
	if _, occupied := manager.Get("C-A"); occupied {
		t.Fatal("recovered circuit remained occupied")
	}
}
