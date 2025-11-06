package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/common"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

func immutableTip(nodeSocket string) (*common.Point, int64, error) {
	conn, err := ouroboros.New(
		ouroboros.WithNetwork(ouroboros.NetworkMainnet),
		ouroboros.WithNodeToNode(false), // Node-to-Client over the local socket
		ouroboros.WithLocalStateQueryConfig(
			localstatequery.NewConfig(
				localstatequery.WithAcquireTimeout(5*time.Second),
				localstatequery.WithQueryTimeout(5*time.Second),
			),
		),
	)
	if err != nil {
		return nil, 0, err
	}
	defer conn.Close()

	// cardano-node UNIX socket path
	if err := conn.Dial("unix", nodeSocket); err != nil {
		return nil, 0, err
	}

	lsq := conn.LocalStateQuery().Client

	// Acquire the immutable tip snapshot
	if err := lsq.AcquireImmutableTip(); err != nil {
		return nil, 0, err
	}
	defer lsq.Release()

	// Point = {Slot, Hash}
	pt, err := lsq.GetChainPoint()
	if err != nil {
		return nil, 0, err
	}
	// Optional but handy for logging
	bn, err := lsq.GetChainBlockNo()
	if err != nil {
		return nil, 0, err
	}

	fmt.Printf("immutable tip: slot=%d blockNo=%d hash=%s\n", pt.Slot, bn, hex.EncodeToString(pt.Hash))
	return pt, bn, nil
}

func txIsImmutable(txSlot uint64, nodeSocket string) (bool, *common.Point, error) {
	pt, _, err := immutableTip(nodeSocket)
	if err != nil {
		return false, nil, err
	}
	return txSlot <= uint64(pt.Slot), pt, nil
}

func main() {
	ok, tip, err := txIsImmutable(123456789, "/opt/cardano/cnode/sockets/node.socket")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("is immutable: %v at tip slot %d\n", ok, tip.Slot)
}
