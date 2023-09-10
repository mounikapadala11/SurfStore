
package SurfTest

import (
	//"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"os"
	"testing"
	//	"time"
)

// A creates and syncs with a file. B creates and syncs with same file. A syncs again.
func TestSyncTwoClientsSameFileLeaderFailure(t *testing.T) {
	t.Logf("client1 syncs with file1. client2 syncs with file1 (different content). client1 syncs again.")
	cfgPath := "./config_files/3nodes.txt"
	//fmt.Println("InitTest ENTERING")
	test := InitTest(cfgPath)
	defer EndTest(test)
	//fmt.Println("Setting Leader for client[0]")
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	//fmt.Println("Setting Leader done and starting SendHeartbeat")
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	//fmt.Println("Done SENDHeartbeat and InitDirectoryorker")
	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	//fmt.Println("Done initDirectoryWorker")
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	file2 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}
	err = worker2.AddFile(file2)
	if err != nil {
		t.FailNow()
	}
	//fmt.Println("entering UpdateFile")
	err = worker2.UpdateFile(file2, "update text")
	if err != nil {
		t.FailNow()
	}
	//fmt.Println("updatefile done and Sync Client Entering ")
	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	//fmt.Println("Sync Client Done and sendign HeartbeAT ")
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	//fmt.Println("Client1 Crash, SetLeader,SendHeartBeAT dONE ")
	//client2 syncs
	//fmt.Println("Client2 STart and sync Clinet start")
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	//fmt.Println("Client2 sync done, SendHeartBeat")
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	//fmt.Println("Client2 done")

	//client1 syncs
	//fmt.Println("Client1 Starts")

	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	//fmt.Println("Client 1 2nd time SnedHeartBeat")
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	workingDir, _ := os.Getwd()
	//fmt.Println("Client 1 2nd time after heart beat.....stat cheking")
	//check client1
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}
	//fmt.Println("Client 1 2nd time after stat cheking and Load MetaFrom DB start")
	fileMeta1, err := LoadMetaFromDB(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1 == nil || fileMeta1[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}

	c, e := SameFile(workingDir+"/test0/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("file1 should not change at client1")
	}
	//fmt.Println("Client 1 2nd time after sodhi")
	//check client2
	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}
	//fmt.Println("Checked client2 and LoadingmetafromDB")
	fileMeta2, err := LoadMetaFromDB(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	//fmt.Println("11")
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	//fmt.Println("22")
	if fileMeta2 == nil || fileMeta2[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}
	//fmt.Println("33")

	c, e = SameFile(workingDir+"/test1/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("wrong file2 contents at client2")
	}
	//fmt.Println("Finally done")
}
