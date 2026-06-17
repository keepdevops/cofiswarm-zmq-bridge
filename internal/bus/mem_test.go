package bus

import "testing"

func TestMemPublishFansOutAndRecords(t *testing.T) {
	b := NewMem([]string{"t1"})
	got := make(chan map[string]any, 1)
	cancel, err := b.Subscribe("t1", func(_ string, p map[string]any) { got <- p })
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	if err := b.Publish("t1", map[string]any{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-got:
		if p["k"] != "v" {
			t.Fatalf("payload = %v", p)
		}
	default:
		t.Fatal("subscriber was not delivered")
	}
	if len(b.Recent()) != 1 {
		t.Fatalf("recent = %d, want 1", len(b.Recent()))
	}
}

func TestMemCancelStopsDelivery(t *testing.T) {
	b := NewMem(nil)
	n := 0
	cancel, _ := b.Subscribe("t", func(string, map[string]any) { n++ })
	cancel()
	_ = b.Publish("t", nil)
	if n != 0 {
		t.Fatalf("delivered %d times after cancel", n)
	}
}
