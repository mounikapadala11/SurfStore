package surfstore

import (
	"database/sql"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	reflect "reflect"
)

// BaseDir field of the client object is used to locate the directory where files to be synced are stored.
// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// panic("todo")
	p, err := filepath.Abs(client.BaseDir + "/" + DEFAULT_META_FILENAME)
	if err != nil {
// 		log.Print("Could not create absolute path to the new index.db file", err
		return
	}

	// fmt.Println("path:", p)

	//The function checks whether an index file exists in the directory. If not, it creates an empty one.
	// Load or create index.db
	_, err = sql.Open("sqlite3", client.BaseDir+"/index.db")
	if err != nil {
		file1, err := os.Create(p)
		if err != nil {
// 			log.Print("Error creating index.db:", err)
			return
		}
		defer file1.Close()
		db, err := sql.Open("sqlite3", p)
		if err != nil {
// 			log.Print("error during opening the file after creating the file")
			return
		}
		// Create indexes table
		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS indexes (
				fileName TEXT,
				version INT,
				hashIndex INT,
				hashValue TEXT
			)`)
		if err != nil {
// 			log.Print("Error creating indexes table hehe:", err)
			return
		}
		// log.Print("Error opening index.db:", err)
	}
	// 	defer db.Close()

	// Create or truncate the database file.

	// Get local file info FileMetaData map from index.db

	// fileMetaMap[filename] = &FileMetaData{
	// 	Filename:      filename,
	// 	Version:       int32(version),
	// 	BlockHashList: blockHashList,
	// }
	localMeta := make(map[string]*FileMetaData)
	localMeta, err = LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
// 		log.Println("Could not load meta from meta file: ", err
		return
	}
	baseAbsolute, err := filepath.Abs(client.BaseDir)
	if err != nil {
// 		log.Println("error in creating Filepath absolutre for the baseDir")
		return
	}
	baseDirFiles, err := ioutil.ReadDir(baseAbsolute)
	if err != nil {
// 		log.Print("Error when reading basedir:%d ", err)
		return
	}

	// Load the current state of the local metadata from the index.db file

	// rows, err := db.Query("SELECT fileName, version, hashIndex, hashValue FROM indexes")
	// if err != nil {
	// 	log.Fatal("Error querying indexes table:", err)
	// }
	// defer rows.Close()

	//

	hashMap := make(map[string][]string)

	for _, file := range baseDirFiles {
		if file.Name() == "index.db" {
			continue
		}

		filePath := filepath.Join(client.BaseDir, file.Name())
		f, err := os.Open(filePath)
		if err != nil {
			//log.Println("Error opening file:", err)
			return
			continue
		}
		defer f.Close()

		fileInfo, err := f.Stat()
		if err != nil {
			//log.Println("Error getting file info:", err)
			return
			continue
		}

		numBlocks := int(math.Ceil(float64(fileInfo.Size()) / float64(client.BlockSize)))

		var bHL []string

		for i := 0; i < numBlocks; i++ {
			byteSlice := make([]byte, client.BlockSize)
			len, err := f.Read(byteSlice)
			if err != nil && err != io.EOF {
				//log.Println("Error reading file:", err)
				break
			}
			byteSlice = byteSlice[:len]
			hash := GetBlockHashString(byteSlice)
			bHL = append(bHL, hash)
		}
		hashMap[file.Name()] = bHL

		if meta, ok := localMeta[file.Name()]; ok {
			if !reflect.DeepEqual(bHL, meta.BlockHashList) {
				meta.BlockHashList = bHL //???
				meta.Version++           //????updates
			}
		} else {
			meta := FileMetaData{Filename: file.Name(), Version: 1, BlockHashList: bHL}
			localMeta[file.Name()] = &meta
		}
	}

	//check for deleted files
	for fileName, metaData := range localMeta {
		// Check if the file is present in the hashMap
		_, present := hashMap[fileName]
		if !present {
			// Check if the file was not empty previously
			if len(metaData.BlockHashList) != 1 || metaData.BlockHashList[0] != "0" {
				// Update the metadata to reflect that the file is now empty
				metaData.Version++
				metaData.BlockHashList = []string{"0"}
			}
		}
	}

	var blockStoreAddrs []string
	if err := client.GetBlockStoreAddrs(&blockStoreAddrs); err != nil {
		//log.Println("Could not get blockStoreAddr from the rpc client: ", err)
		return
	}

	remoteIndex := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteIndex); err != nil {
		//log.Println("Error getting index from server: ", err)
		return
	}

	// Check each file in the local metadata and update as needed(upload new data to server)
	//Check if server has locas files, upload changes
	for fileName, localMetaData := range localMeta {
		if remoteMetaData, ok := remoteIndex[fileName]; ok {
			if localMetaData.Version > remoteMetaData.Version {
				//h2>h3...h
				// uploadFile(client, localMetaData, blockStoreAddr)
				///
				path := client.BaseDir + "/" + fileName
				var latestVersion int32

				if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
					err = client.UpdateFile(localMetaData, &latestVersion)
					if err != nil {
						//log.Println("Could not upload file: ", err)
						return
					}
					localMetaData.Version = latestVersion
					// return err
					break
				}
				file, err := os.Open(path)
				if err != nil {
					//log.Println("Error opening file: ", err)
					return
				}
				defer file.Close()
				fileStat, _ := os.Stat(path)
				var numBlocks int = int(math.Ceil(float64(fileStat.Size()) / float64(client.BlockSize)))
				for i := 0; i < numBlocks; i++ {
					byteSlice := make([]byte, client.BlockSize)
					len, err := file.Read(byteSlice)
					if err != nil && err != io.EOF {
						//log.Println("Error reading bytes from file in basedir: ", err)
						return
					}
					byteSlice = byteSlice[:len]

					block := Block{BlockData: byteSlice, BlockSize: int32(len)}
					hash := GetBlockHashString(byteSlice)
					var hashes []string
					hashes = append(hashes, hash)
					mapp := make(map[string][]string)
					client.GetBlockStoreMap(hashes, &mapp)
					addr := ""
					for key, val := range mapp {
						if val[0] == hash {
							addr = key
						}
					}
					var succ bool
					// Instead of blockstoreAddr , get the needed addr using hash value of block
					if err := client.PutBlock(&block, addr, &succ); err != nil {
						//log.Println("Failed to put block: ", err)
						return
					}
				}

				if err := client.UpdateFile(localMetaData, &latestVersion); err != nil {
					//log.Println("Failed to update file: ", err)
					localMetaData.Version = -1
				}
				localMetaData.Version = latestVersion
				///
			}
		} else { //local has a new file..that needs to be uploaded to server
			// uploadFile(client, localMetaData, blockStoreAddr)
			path := client.BaseDir + "/" + fileName
			var latestVersion int32

			if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
				err = client.UpdateFile(localMetaData, &latestVersion)
				if err != nil {
					//log.Println("Could not upload file: ", err)
					return
				}
				localMetaData.Version = latestVersion
				// return err
				break
			}
			file, err := os.Open(path)
			if err != nil {
				//log.Println("Error opening file: ", err)
				return
			}
			defer file.Close()
			fileStat, _ := os.Stat(path)
			var numBlocks int = int(math.Ceil(float64(fileStat.Size()) / float64(client.BlockSize)))
			for i := 0; i < numBlocks; i++ {
				byteSlice := make([]byte, client.BlockSize)
				len, err := file.Read(byteSlice)
				if err != nil && err != io.EOF {
					//log.Println("Error reading bytes from file in basedir: ", err)
					return
				}
				byteSlice = byteSlice[:len]

				block := Block{BlockData: byteSlice, BlockSize: int32(len)}
				hash := GetBlockHashString(byteSlice)
				var hashes []string
				hashes = append(hashes, hash)
				mapp := make(map[string][]string)
				client.GetBlockStoreMap(hashes, &mapp)
				addr := ""
				for key, val := range mapp {
					if val[0] == hash {
						addr = key
					}
				}

				var succ bool
				//fmt.Println("Hash :", hash[:4], " addr ", addr)
				// Same as above
				if err := client.PutBlock(&block, addr, &succ); err != nil {
					//log.Println("Failed to put block: ", err)
					return
				}
			}

			if err := client.UpdateFile(localMetaData, &latestVersion); err != nil {
				//log.Println("Failed to update file: ", err)
				localMetaData.Version = -1
			}
			localMetaData.Version = latestVersion
		}
	}

	// Check each file on the server and update as needed(download to basedir)
	for filename, remoteMetaData := range remoteIndex {
		if localMetaData, ok := localMeta[filename]; ok {
			if localMetaData.Version < remoteMetaData.Version { //download a high versioned file from server...(not a new file)
				// downloadFile(client, localMetaData, remoteMetaData, blockStoreAddr)
				path := client.BaseDir + "/" + filename
				file, err := os.Create(path)
				if err != nil {
					//log.Println("Error creating file: ", err)
					return
				}
				defer file.Close()
				//fmt.Println("//download a high versioned file from server")
				localMeta[filename] = remoteMetaData
				// *localMetaData = *remoteMetaData

				//File deleted in server
				if len(remoteMetaData.BlockHashList) == 1 && remoteMetaData.BlockHashList[0] == "0" {
					if err := os.Remove(path); err != nil {
						//log.Println("Could not remove local file: ", err)
						// return err
						break
					}
					// return nil
					break
				}

				data := ""
				for _, hash := range remoteMetaData.BlockHashList {
					var block Block

					//
					//hash := GetBlockHashString(byteSlice)
					var hashes []string
					hashes = append(hashes, hash)
					mapp := make(map[string][]string)
					client.GetBlockStoreMap(hashes, &mapp)
					addr := ""
					for key, val := range mapp {
						for _, v := range val {
							if v == hash {
								addr = key
							}
						}
					}
					//
					//fmt.Println("Hash 1: ", hash[:4], " addr ", addr)
					if err := client.GetBlock(hash, addr, &block); err != nil {
						//log.Println("Failed to get block: ", err)
						return
					}

					data += string(block.BlockData)
				}
				file.WriteString(data)
				//
				//
			} else if localMetaData.Version == remoteMetaData.Version && !reflect.DeepEqual(localMetaData.BlockHashList, remoteMetaData.BlockHashList) {
				//same versions..but different content
				// downloadFile(client, localMetaData, remoteMetaData, blockStoreAddr)
				///
				//fmt.Println("Downloading the file...same versions..but different content")
				path := client.BaseDir + "/" + filename
				file, err := os.Create(path)
				if err != nil {
					//log.Println("Error creating file: ", err)
					return
				}
				defer file.Close()

				localMeta[filename] = remoteMetaData

				// *localMetaData = *remoteMetaData

				//File deleted in server
				if len(remoteMetaData.BlockHashList) == 1 && remoteMetaData.BlockHashList[0] == "0" {
					if err := os.Remove(path); err != nil {
						//log.Println("Could not remove local file: ", err)
						// return err
						break
					}
					// return nil
					break
				}

				data := ""
				for _, hash := range remoteMetaData.BlockHashList {
					var block Block
					//
					var hashes []string
					hashes = append(hashes, hash)
					mapp := make(map[string][]string)
					client.GetBlockStoreMap(hashes, &mapp)
					addr := ""
					for key, val := range mapp {
						for _, v := range val {
							if v == hash {
								addr = key
							}
						}
					}
					//
					//fmt.Println("Hash 2: ", hash[:4], " addr ", addr)
					if err := client.GetBlock(hash, addr, &block); err != nil {
						//log.Println("Failed to get block: ", err)
						return
					}

					data += string(block.BlockData)
				}
				file.WriteString(data)
				///
				///
			}
		} else { //the file in server is not at all present in the local base dir
			localMeta[filename] = &FileMetaData{}
			// localMetaData := localMeta[filename] //???
			// downloadFile(client, localMetaData, remoteMetaData, blockStoreAddr)
			///
			path := client.BaseDir + "/" + filename
			file, err := os.Create(path)
			if err != nil {
				//log.Println("Error creating file: ", err)
				return
			}
			defer file.Close()
			//fmt.Println("downloading the file in server is not at all present in the local base dir")

			// localMetaData = remoteMetaData //??
			localMeta[filename] = remoteMetaData

			//File deleted in server
			if len(remoteMetaData.BlockHashList) == 1 && remoteMetaData.BlockHashList[0] == "0" {
				if err := os.Remove(path); err != nil {
					//log.Println("Could not remove local file: ", err)
					// return err
					break
				}
				// return nil
				break
			}

			data := ""

			hashes := remoteMetaData.BlockHashList
			//mapp := make(map[string][]string)
			//client.GetBlockStoreMap(hashes, &mapp)
			//count := 0
			//for _, hash := range hashes {
			//	//fmt.Println("All hashes ", hash[:4])
			//}
			for _, hash := range hashes {
				var hashes2 []string
				hashes2 = append(hashes2, hash)
				mapp := make(map[string][]string)
				client.GetBlockStoreMap(hashes, &mapp)
				//fmt.Println(hash[:4], "---- Map: ", mapp)
				for addrs, vals := range mapp {
					for _, val := range vals {
						if val == hash {
							add := addrs
							var block Block
							//fmt.Println("Hash 3: ", hash[:4], " addr ", add)
							if err := client.GetBlock(hash, add, &block); err != nil {
								//log.Println("Failed to get block: ", err)
								return
							}
							data += string(block.BlockData)
						}
					}
				}
			}

			//fmt.Println("map ", mapp)
			//for _, hash := range hashes {
			//	for key, val := range mapp {
			//		for _, v := range val {
			//			if v == hash {
			//				//count += 1
			//				addr := key
			//				var block Block
			//				fmt.Println("Hash 3: ", hash[:4], " addr ", addr)
			//				if err := client.GetBlock(hash, addr, &block); err != nil {
			//					log.Println("Failed to get block: ", err)
			//				}
			//				data += string(block.BlockData)
			//			}
			//		}
			//	}
			//}
			//for _, hash := range remoteMetaData.BlockHashList {
			//	var block Block
			//	//
			//	var hashes []string
			//	hashes = append(hashes, hash)
			//
			//	mapp := make(map[string][]string)
			//	client.GetBlockStoreMap(hashes, &mapp)
			//	addr := ""
			//	for key, val := range mapp {
			//		for _, v := range val {
			//			if v == hash {
			//				addr = key
			//			}
			//		}
			//	}
			//	//
			//	if err := client.GetBlock(hash, addr, &block); err != nil {
			//		log.Println("Failed to get block: ", err)
			//	}
			//
			//	data += string(block.BlockData)
			//}
			//fmt.Println(filename + string(len(hashes)) + "  " + string(count))
			file.WriteString(data)
			///
			///
		}
	}

	WriteMetaFile(localMeta, client.BaseDir)

}
