package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	substrate "github.com/lilong555/agentcloud/internal/adapters/substrate"
)

func main() {
	address := flag.String("address", "127.0.0.1:9443", "Substrate API")
	ca := flag.String("ca", "artifacts/substrate/client/ca.pem", "CA file")
	token := flag.String("token-file", "artifacts/substrate/client/token", "private bearer file")
	atespace := flag.String("atespace", "agentcloud", "atespace")
	actor := flag.String("actor", "", "source Actor name")
	uid := flag.String("uid", "", "source Actor UID")
	snapshot := flag.String("snapshot", "", "source snapshot UUID")
	generation := flag.Int("generation", 0, "source snapshot generation")
	tag := flag.String("tag", "", "retained tag name")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client, err := substrate.New(ctx, substrate.Config{Address: *address, Atespace: *atespace, CAFile: *ca, ServerName: "api.ate-system.svc", TokenFile: *token})
	if err != nil {
		fail(err)
	}
	defer client.Close()
	retained, err := client.RetainSnapshot(ctx, *actor, *uid, *snapshot, int32(*generation), *tag)
	if err != nil {
		fail(err)
	}
	if err = json.NewEncoder(os.Stdout).Encode(retained); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
