package control

import (
	"encoding/json"
	"testing"
)

func TestTrafficCountersSkippedJSON(t *testing.T) {
	want := TrafficCounters{SkippedPackets: 3, SkippedBytes: 512}
	payload, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}

	var fields map[string]uint64
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["skippedPackets"] != want.SkippedPackets || fields["skippedBytes"] != want.SkippedBytes {
		t.Fatalf("skipped JSON fields = %#v, want %d packets/%d bytes", fields, want.SkippedPackets, want.SkippedBytes)
	}
}

func TestTrafficCountersLegacyJSONDefaultsSkippedToZero(t *testing.T) {
	var counters TrafficCounters
	if err := json.Unmarshal([]byte(`{"rxPackets":1,"rxBytes":64,"txPackets":2,"txBytes":128,"dropPackets":0,"dropBytes":0}`), &counters); err != nil {
		t.Fatal(err)
	}
	if counters.SkippedPackets != 0 || counters.SkippedBytes != 0 {
		t.Fatalf("legacy JSON skipped counters = %d packets/%d bytes, want zero", counters.SkippedPackets, counters.SkippedBytes)
	}
}

func TestStatusJSONUsesSessions(t *testing.T) {
	tests := []struct {
		name   string
		status any
	}{
		{name: "server", status: ServerStatus{Sessions: 2}},
		{name: "client", status: ClientStatus{Sessions: 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := json.Marshal(tt.status)
			if err != nil {
				t.Fatal(err)
			}

			var fields map[string]json.RawMessage
			if err := json.Unmarshal(payload, &fields); err != nil {
				t.Fatal(err)
			}
			if _, ok := fields["sessions"]; !ok {
				t.Fatalf("status JSON = %s, want sessions field", payload)
			}
			if _, ok := fields["standbyCarriers"]; ok {
				t.Fatalf("status JSON = %s, contains removed standbyCarriers field", payload)
			}
		})
	}
}
