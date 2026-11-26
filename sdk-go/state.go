package flux

import "context"

// State: stub temporal de la fase `security`.
type State struct{ bus *Bus }

func newState(b *Bus) *State { return &State{bus: b} }
func (s *State) stopAll()    {}

type rpcClient struct{ bus *Bus }

func newRPCClient(b *Bus) *rpcClient         { return &rpcClient{bus: b} }
func (r *rpcClient) stopAll(context.Context) {}
