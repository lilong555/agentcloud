package substrate

import (
	"context"
	"fmt"
	"net"
	"strings"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (c *Client) AllowCoordinatorEgress(ctx context.Context, actorName, hostname string, port uint32) error {
	if actorName == "" || hostname == "" || net.ParseIP(hostname) != nil || strings.ContainsAny(hostname, "*/:") || port == 0 || port > 65535 {
		return fmt.Errorf("coordinator egress requires an actor, DNS hostname and valid port")
	}
	policy := &ateapi.EgressPolicy{Metadata: &ateapi.ResourceMetadata{Atespace: c.atespace, Name: "default"}, Rules: []*ateapi.EgressRule{{Http: &ateapi.HttpRule{Hostnames: []string{hostname}, Ports: &ateapi.Ports{Numbers: []int32{int32(port)}}}}}}
	_, err := c.control.CreateActorEgressPolicy(ctx, &ateapi.CreateActorEgressPolicyRequest{Actor: c.ref(actorName), EgressPolicy: policy})
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.AlreadyExists {
		return err
	}
	existing, err := c.control.GetActorEgressPolicy(ctx, &ateapi.GetActorEgressPolicyRequest{Actor: c.ref(actorName)})
	if err != nil {
		return err
	}
	rules := existing.GetRules()
	if len(rules) == 1 {
		rule := rules[0].GetHttp()
		if rule != nil && len(rule.Hostnames) == 1 && rule.Hostnames[0] == hostname && len(rule.GetPorts().GetNumbers()) == 1 && rule.Ports.Numbers[0] == int32(port) && rule.Ports.All == nil && rule.Effects == nil {
			return nil
		}
	}
	return status.Error(codes.FailedPrecondition, "Actor egress policy differs from the coordinator origin")
}
