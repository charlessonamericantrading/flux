package flux

import "context"

// rpcClient: stub temporal; la capacidad `rpc` llega en el commit siguiente.
type rpcClient struct{ bus *Bus }

func newRPCClient(b *Bus) *rpcClient         { return &rpcClient{bus: b} }
func (r *rpcClient) stopAll(context.Context) {}
