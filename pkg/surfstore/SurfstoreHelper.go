package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `INSERT INTO indexes (fileName, version, hashIndex, hashValue) VALUES (?, ?, ?, ?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			//log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		//log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		//log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	// panic("todo")

	defer db.Close()
	//started
	insertStmt, err := db.Prepare(insertTuple)
	if err != nil {
		return err
	}
	defer insertStmt.Close()

	for _, fileMeta := range fileMetas {
		for idx, hashVal := range fileMeta.BlockHashList {

			if _, err := insertStmt.Exec(fileMeta.Filename, fileMeta.Version, idx, hashVal); err != nil {
				return err
			}
		}
	}
	// fmt.Println("updated table")
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `SELECT DISTINCT fileName FROM indexes;`

const getTuplesByFileName string = `SELECT * FROM indexes WHERE fileName = ?;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		//log.Fatal("Error When Opening Meta")
	}
	// panic("todo")
	//starts
	fileMetaMap = make(map[string]*FileMetaData)
	//fmt.Println(getDistinctFileName)
	//fmt.Println(db.Ping())
	//fmt.Println("Here 3")
	rows, err := db.Query(getDistinctFileName)
	if err != nil {
		//log.Print("Error When Querying Meta")
		return
	}
	defer rows.Close()

	for rows.Next() {
		var filename string
		if err := rows.Scan(&filename); err != nil {
			//log.Print("Error When Scanning Meta rows which are distinct filename rows")
			return
		}
		// Get the tuples for the current file name from the indexes table
		//SELECT version, hashIndex, hashValue FROM indexes WHERE fileName = ?;`

		tuplesRows, err := db.Prepare(getTuplesByFileName)
		// Query(getTuplesByFileName, filename)
		if err != nil {
			//log.Print("Error When Querying Meta")
			return
		}
		defer tuplesRows.Close()

		metadata, err := tuplesRows.Query(filename)
		if err != nil {
			return
		}
		defer metadata.Close()
		blockHashList := make([]string, 0)
		var version int32
		var file_2 string
		for metadata.Next() {

			var hashIndex int
			var hashValue string
			if err := metadata.Scan(&file_2, &version, &hashIndex, &hashValue); err != nil {
				//log.Print("Error When Scanning Meta")
				return
			}
			blockHashList = append(blockHashList, hashValue)
		}

		fileMetaData := &FileMetaData{
			Filename:      file_2,
			Version:       int32(version),
			BlockHashList: blockHashList}
		fileMetaMap[filename] = fileMetaData
	}

	return fileMetaMap, nil
}

/*
/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	//fmt.Println("--------BEGIN PRINT MAP--------")

	//for _, filemeta := range metaMap {
	//	fmt.Println("\t", filemeta.Filename, filemeta.Version)
	//	for _, blockHash := range filemeta.BlockHashList {
	//		fmt.Println("\t", blockHash)
	//	}
	//}

	//fmt.Println("---------END PRINT MAP--------")

}
