package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
	//hash of server===id server
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	//start
	//blockId..hash
	var keys []string // create a slice with capacity equal to the length of the map

	for k, _ := range c.ServerMap {
		keys = append(keys, k) // add each key to the slice
	}
	sort.Strings(keys)
	if len(keys) < 2 {
		// There is only one server in the ring, return its address
		for _, serverAddr := range c.ServerMap {
			return serverAddr
		}
	}
	//fmt.Println(keys) // print the slice containing all the keys
	min_server := keys[0]
	max_server := keys[len(keys)-1]

	if blockId > max_server {
		return c.ServerMap[min_server]
	}
	//if blockId>min_server && blockId<max_server {
	for _, i := range keys {
		if blockId <= i {
			return c.ServerMap[i]
		}
	}
	//
	return c.ServerMap[min_server]

	//lastServer = server

	// If the block's hash is greater than all servers' hashes, return the first server
	//return lastServer
	//sort hashes..//server
	//backward...edge...by min and mx ..(max...min)
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	// panic("todo")
	ring := &ConsistentHashRing{ServerMap: make(map[string]string)}

	for _, serverAddr := range serverAddrs {
		serverName := "blockstore" + serverAddr
		hash := ring.Hash(serverName)
		ring.ServerMap[hash] = serverAddr
	}
	return ring
}
