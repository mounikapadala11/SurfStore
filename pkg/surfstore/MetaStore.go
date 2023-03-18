package surfstore

import (
	context "context"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	fileInfoMap := &FileInfoMap{FileInfoMap: make(map[string]*FileMetaData)}
	fileInfoMap.FileInfoMap = m.FileMetaMap //???

	return fileInfoMap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	filename := fileMetaData.Filename
	fileMeta, ok := m.FileMetaMap[filename]
	if !ok {
		// File doesn't exist, create new FileMetaData
		m.FileMetaMap[filename] = &FileMetaData{
			Filename:      filename,
			Version:       1,
			BlockHashList: fileMetaData.BlockHashList,
		}
		return &Version{Version: 1}, nil
	}
	//file is present
	if fileMetaData.Version != fileMeta.Version+1 {
		return &Version{Version: -1}, nil
		//fmt.Errorf("version number is not one greater than the current version number")
	}
	m.FileMetaMap[filename] = fileMetaData

	return &Version{Version: fileMetaData.Version}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	//start
	//blockHashesIn has all the input file string hashed values, we need to make a blockStoreMap, which has the key as the server responsible for that hash andit should be pointed to the hash value itself.
	blockStoreMap := &BlockStoreMap{BlockStoreMap: make(map[string]*BlockHashes)}
	for _, blockHash := range blockHashesIn.Hashes {
		temp_id := m.ConsistentHashRing.GetResponsibleServer(blockHash)
		if _, ok := blockStoreMap.BlockStoreMap[temp_id]; ok {
			//fmt.Println("foo exists in the map")
			blockStoreMap.BlockStoreMap[temp_id].Hashes = append(blockStoreMap.BlockStoreMap[temp_id].Hashes, blockHash)
		} else {
			//fmt.Println("foo does not exist in the map")
			blockStoreMap.BlockStoreMap[temp_id] = &BlockHashes{Hashes: []string{blockHash}}
		}

		//blockStoreMap.BlockStoreMap[m.ConsistentHashRing.GetResponsibleServer(blockHash)] = &BlockHashes{Hashes: []string{blockHash}}
	} //check for the m.ConsistentHashRing.GetResponsibleServer(blockHash).....if not..create and add...
	//else::::append

	return blockStoreMap, nil
	//hashvalue to server address
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	//start
	// return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
