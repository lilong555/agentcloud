package control

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestDurableToolReadKeepsSuspendedActorAndTenantIsolation(t *testing.T) {
	cfg := testConfig(t)
	c := newTestController(t, cfg)
	createTestSession(t, c, "tenant-a", "receipt")
	original, err := c.Tool(context.Background(), "tenant-a", "receipt", ToolRequest{
		ID: "completion", Epoch: 1, Kind: "file_write", Path: "report.json", Content: `{"status":"COMPLETED"}`})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = c.Lifecycle(context.Background(), "tenant-a", "receipt", "suspend", LifecycleRequest{OperationID: "park", ExpectedEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := c.Get("tenant-a", "receipt")
	status, err := c.SessionStatus("tenant-a", "receipt")
	if err != nil || status.State != "SUSPENDED" || status.Epoch != before.Epoch {
		t.Fatalf("bounded polling state differs: %+v %v", status, err)
	}
	_, err = c.SessionStatus("tenant-b", "receipt")
	assertCode(t, err, "session_not_found")
	encoded, _ := json.Marshal(status)
	var fields map[string]any
	_ = json.Unmarshal(encoded, &fields)
	if fields["tools"] != nil || fields["lifecycle"] != nil || fields["scheduling"] != nil {
		t.Fatal("polling response copied accumulated receipt histories")
	}
	handler := c.Handler()
	for _, row := range []struct {
		token, operation string
		code             int
	}{
		{"token-a", "completion", 200}, {"token-a", "missing", 404}, {"token-b", "completion", 404}, {"", "completion", 401},
	} {
		request := httptest.NewRequest("GET", "/v1/sessions/receipt/tools/"+row.operation, nil)
		if row.token != "" {
			request.Header.Set("Authorization", "Bearer "+row.token)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != row.code {
			t.Fatalf("receipt read status %d: %s", response.Code, response.Body.String())
		}
		if response.Code == 200 {
			var got ToolReceipt
			if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil || !reflect.DeepEqual(&got, original) {
				t.Fatalf("durable receipt differs: %+v %v", got, err)
			}
		}
	}
	after, _ := c.Get("tenant-a", "receipt")
	if !reflect.DeepEqual(before, after) || after.State != "SUSPENDED" {
		t.Fatal("receipt observation changed the parked session")
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := newTestController(t, cfg)
	read, err := reopened.ToolStatus("tenant-a", "receipt", "completion")
	if err != nil || !reflect.DeepEqual(read, original) {
		t.Fatalf("receipt did not survive coordinator restart: %+v %v", read, err)
	}
	read.Status = "modified-client-copy"
	durable, _ := reopened.ToolStatus("tenant-a", "receipt", "completion")
	if durable.Status != "COMPLETED" {
		t.Fatal("caller mutation changed the committed receipt")
	}
}
