package surfstore

import (
	context "context"
	"errors"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	//hashstring to block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// panic("todo")
	//fmt.Println("Getting Block Hashes...Building the Block Hashes")
	block, ok := bs.BlockMap[blockHash.Hash]
	if !ok {
		return nil, errors.New("block not found")
	}
	b := &Block{
		BlockData: block.BlockData,
		BlockSize: block.BlockSize,
	}
	return b, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// panic("todo")
	//fmt.Println("Putting Blocks at a given address")
	bs.BlockMap[GetBlockHashString(block.BlockData)] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// panic("todo")
	//fmt.Println("Usually vadam")
	out := &BlockHashes{}
	for _, hash := range blockHashesIn.GetHashes() {
		if _, ok := bs.BlockMap[hash]; ok {
			out.Hashes = append(out.Hashes, hash)
		}
	}
	return out, nil

}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	//panic("todo")
	//fmt.Println("Getting Block Hashes...Building the Block Hashes")
	out := &BlockHashes{}
	for hash := range bs.BlockMap {
		out.Hashes = append(out.Hashes, hash)
	}
	return out, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
