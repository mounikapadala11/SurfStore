package surfstore

import (
	context "context"
	"os"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	put_rpc_conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(put_rpc_conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	Success, err := c.PutBlock(ctx, block) //??
	if err != nil {
		put_rpc_conn.Close()
		return err
	}

	// close the connection
	// put_rpc_conn.Close()
	*succ = Success.Flag //??
	return nil
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// create input message for the call
	hashes := &BlockHashes{}
	hashes.Hashes = blockHashesIn

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := c.HasBlocks(ctx, hashes)
	if err != nil {
		conn.Close()
		return err
	}

	// copy the results to the output parameter
	*blockHashesOut = result.Hashes

	// close the connection
	return nil
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	//panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	//defer conn.Close()

	// Create the client
	c := NewBlockStoreClient(conn)

	// Update the file metadata on the server
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.
		GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}

	// Update the latest version
	*blockHashes = b.Hashes

	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, l := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(l, grpc.WithInsecure())
		if err != nil {
			//conn.Close()
			return err
		}

		// create the client
		// c := NewMetaStoreClient(conn)
		c := NewRaftSurfstoreClient(conn)

		// perform the remote call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		//defer
		res, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			//return err
			cancel()
			conn.Close()
			continue
		} else {
			// copy the server response to the output parameter
			cancel()
			*serverFileInfoMap = res.FileInfoMap
			return conn.Close()
		}
	}
	os.Exit(1)
	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	//panic("todo")
	for _, l := range surfClient.MetaStoreAddrs {
		conn_updatefile, err := grpc.Dial(l, grpc.WithInsecure())
		if err != nil {
			//conn_updatefile.Close()
			return err
		}
		//defer conn_updatefile.Close()

		// Create the client
		// c := NewMetaStoreClient(conn_updatefile)
		c := NewRaftSurfstoreClient(conn_updatefile)

		// Update the file metadata on the server
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		version, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			//return err
			cancel()
			conn_updatefile.Close()
			continue
		} else {
			// Update the latest version
			cancel()
			*latestVersion = version.Version
			return conn_updatefile.Close()
		}
		//cancel()
		//conn_updatefile.Close()
	}
	os.Exit(1)
	return nil
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, l := range surfClient.MetaStoreAddrs {
		// connect to the metadata store
		conn, err := grpc.Dial(l, grpc.WithInsecure())
		if err != nil {
			return err
		}
		// c := NewMetaStoreClient(conn)
		c := NewRaftSurfstoreClient(conn)

		// create input message for the call
		hashes := &BlockHashes{}
		hashes.Hashes = blockHashesIn

		// perform the remote call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		res, err := c.GetBlockStoreMap(ctx, hashes)
		if err != nil {
			//return err
			cancel()
			conn.Close()
			continue
		} else {
			// copy the server response to the output parameter

			ky := make([]string, 0, len(res.BlockStoreMap))
			for loc := range res.BlockStoreMap {

				ky = append(ky, loc)
			}

			temp_hm := make(map[string][]string)
			for _, t := range ky {
				h_m := res.BlockStoreMap[t]
				temp_hm[t] = h_m.Hashes
			}
			*blockStoreMap = temp_hm
			cancel()
			conn.Close()
		}
		//conn.Close()
		// close the connection

	}
	os.Exit(1)
	return nil

}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, l := range surfClient.MetaStoreAddrs {
		// connect to the metadata store
		conn, err := grpc.Dial(l, grpc.WithInsecure())
		if err != nil {
			return err
		}
		// c := NewMetaStoreClient(conn)
		c := NewRaftSurfstoreClient(conn)

		// perform the remote call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		res1, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err == nil {
			*blockStoreAddrs = res1.BlockStoreAddrs
			cancel()
			return conn.Close()
		} else {
			cancel()
			conn.Close()
			continue
		}

	}
	os.Exit(1)
	return nil
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
