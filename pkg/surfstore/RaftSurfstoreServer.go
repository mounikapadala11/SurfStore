package surfstore

import (
	context "context"
	//"fmt"
	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"sync"
	"time"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	id    int64
	peers []string
	//pendingCommits []*chan bool
	commitIndex int64
	lastApplied int64
	sameIndices []int64
	sameIndex   int64
	metaStore   *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	//fmt.Println("Entering GetInfoMap")
	//GetFileInfoMap: Returns the FileInfoMap of the SurfStore system.
	//If the server is not the leader or has crashed, it returns an error.
	//It also waits for a majority of servers to be alive before returning the FileInfoMap.
	//gets FileInfoMap, only when the server is the leader.

	//leader Check
	s.isLeaderMutex.Lock()
	// If the server is not the leader, return an error
	if !s.isLeader {
		s.isLeaderMutex.Unlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.Unlock()

	//crash Check
	s.isCrashedMutex.Lock()
	crashStatus := s.isCrashed
	s.isCrashedMutex.Unlock()
	if crashStatus {
		return nil, ERR_SERVER_CRASHED
		//Error("This server already crashed, couldn't take Append Entry")
	}

	for {
		majority := s.checkMajorityAlive()
		if majority {
			return &FileInfoMap{
				FileInfoMap: s.metaStore.FileMetaMap,
			}, nil
		}
	}

}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	//fmt.Println("Entering GetBlockStoreMap")
	//GetBlockStoreMap: Returns the BlockStoreMap of the SurfStore system, given a list of block hashes.
	//If the server is not the leader or has crashed, it returns an error.
	//It also waits for a majority of servers to be alive before returning the BlockStoreMap.

	//Given a list of block hashes, find out which block server they
	//belong to. Returns a mapping from block server address to block
	//hashes

	//leader Check
	s.isLeaderMutex.Lock()
	// If the server is not the leader, return an error
	if !s.isLeader {
		s.isLeaderMutex.Unlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.Unlock()

	//crash Check
	s.isCrashedMutex.Lock()
	crashStatus := s.isCrashed
	s.isCrashedMutex.Unlock()
	if crashStatus {
		return nil, ERR_SERVER_CRASHED
		//Error("This server already crashed, couldn't take Append Entry")
	}

	for {
		if s.checkMajorityAlive() {
			return s.metaStore.GetBlockStoreMap(ctx, hashes)
		}
	}
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	//fmt.Println("Entering GetBlock Store Address")
	//leader Check
	s.isLeaderMutex.Lock()
	// If the server is not the leader, return an error
	if !s.isLeader {
		s.isLeaderMutex.Unlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.Unlock()

	//crash Check
	s.isCrashedMutex.Lock()
	crashStatus := s.isCrashed
	s.isCrashedMutex.Unlock()
	if crashStatus {
		return nil, ERR_SERVER_CRASHED
		//Error("This server already crashed, couldn't take Append Entry")
	}

	for {
		if s.checkMajorityAlive() {
			return s.metaStore.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		}
	}
}

func (s *RaftSurfstore) checkMajorityAlive() bool {
	//fmt.Println("Entering Check Majority Alive")
	count := 1 //cuz
	// for server
	for i, peer := range s.peers {

		if s.id == int64(i) {
			continue
		}

		s.isCrashedMutex.Lock()
		crashStatus := s.isCrashed
		s.isCrashedMutex.Unlock()
		if crashStatus {
			return false
			//Error("This server already crashed, couldn't take Append Entry")
		}

		///////////
		// Send the heartbeat to all the followers
		conn, err := grpc.Dial(peer, grpc.WithInsecure())
		if err != nil {
			// Handle error
			continue
		}
		//defer conn.Close()

		client := NewRaftSurfstoreClient(conn)
		t := int64(-1)
		//e := s.log
		var e []*UpdateOperation
		if s.sameIndices[i] != -1 {
			t = s.log[s.sameIndices[i]].Term
			//e = s.log[s.sameIndices[i]:]
		}
		if int(s.sameIndices[i]+1) < len(s.log) {
			e = s.log[s.sameIndices[i]+1:]
		}
		// Create an AppendEntryInput object with empty entries to send a heartbeat
		input := &AppendEntryInput{
			Term:         -1,
			PrevLogIndex: s.sameIndices[i],
			PrevLogTerm:  t,
			Entries:      e,
			LeaderCommit: s.commitIndex,
		}

		nctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = client.AppendEntries(nctx, input)
		if err == nil {
			count++
			//s.sameIndices[i]=r.sameIndex
		}
		cancel()
		/////////////////

	}
	return count >= len(s.peers)/2+1
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	//fmt.Println("Entering UpdateFile in server", s.id, " filemeta", filemeta)
	s.isLeaderMutex.RLock()
	// If the server is not the leader, return an error
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
		//Error("This server already crashed, couldn't take Append Entry")
	}
	s.isCrashedMutex.RUnlock()

	////
	updated := false
	var v Version
	upd := new(UpdateOperation)
	filename := filemeta.Filename
	_, ok := s.metaStore.FileMetaMap[filename]
	if !ok {
		// File doesn't exist, create new FileMetaData
		upd.FileMetaData = filemeta
		upd.Term = s.term
		v = Version{Version: filemeta.Version}
		updated = true
		//fmt.Println("New file")
		//___________________________________
		//s.metaStore.FileMetaMap[filename] = &FileMetaData{
		//	Filename:      filename,
		//	Version:       1,
		//	BlockHashList: fileMetaData.BlockHashList,
		//}
		//return &Version{Version: 1}, nil
	} else {
		//file is present
		//fmt.Println("Old file")
		if filemeta.Version != s.metaStore.FileMetaMap[filemeta.Filename].Version+1 {
			//return &Version{Version: -1}, nil
			//fmt.Errorf("version number is not one greater than the current version number")
			v = Version{Version: -1}
			return &v, nil
		} else {
			updated = true
			upd.Term = s.term
			upd.FileMetaData = filemeta
			v = Version{Version: filemeta.Version}
		}
		// Create a new UpdateOperation with the updated FileMetaData
		//op := &UpdateOperation{
		//	Term:         s.term,
		//	FileMetaData: filemeta,
		//}

	}
	if updated {
		// Append the new operation to the log
		s.log = append(s.log, upd)
	}
	if !updated {

		return &v, nil
	}
	could_upd := false
	////

	for {
		//iteratively checking if the server crashes
		s.isCrashedMutex.RLock()
		if s.isCrashed {
			s.isCrashedMutex.RUnlock()

			return nil, ERR_SERVER_CRASHED
			//Error("This server already crashed, couldn't take Append Entry")
		}
		s.isCrashedMutex.RUnlock()

		//if s.checkMajorityAlive() {
		//could_upd = true
		count_s := 0 //cuz for server
		for i, peer := range s.peers {

			if s.id == int64(i) {
				count_s++
				continue
			}

			s.isCrashedMutex.RLock()
			crashStatus := s.isCrashed
			s.isCrashedMutex.RUnlock()
			if crashStatus {
				return nil, ERR_SERVER_CRASHED
				//Error("This server already crashed, couldn't take Append Entry")
			}

			///////////
			// Send the heartbeat to all the followers
			conn, err := grpc.Dial(peer, grpc.WithInsecure())
			if err != nil {
				// Handle error
				conn.Close()
				continue
			}
			//defer conn.Close()

			client := NewRaftSurfstoreClient(conn)
			t := int64(-1)
			//e := s.log
			var e []*UpdateOperation

			if s.sameIndices[i] != -1 {
				t = s.log[s.sameIndices[i]].Term
				//e = s.log[s.sameIndices[i]:]
			}

			if int(s.sameIndices[i]+1) < len(s.log) {
				e = s.log[s.sameIndices[i]+1:]
			}
			// Create an AppendEntryInput object with empty entries to send a heartbeat
			input := &AppendEntryInput{
				Term:         s.term,
				PrevLogIndex: s.sameIndices[i],
				PrevLogTerm:  t,
				Entries:      e,
				LeaderCommit: s.commitIndex,
			}

			nctx, cancel := context.WithTimeout(context.Background(), time.Second)
			r, err := client.AppendEntries(nctx, input)
			//cancel()
			//if err == nil {
			//	if r.Success == true {
			//		s.sameIndices[i]++
			//		count1 += 1
			//	}
			//	//s.sameIndices[i]=r.sameIndex
			//}
			if err != nil {
				cancel()
				conn.Close()
				continue
			}
			if r.Success {
				count_s += 1
				s.sameIndices[i] = s.sameIndex
			} else {
				if s.sameIndices[i] >= 0 {
					s.sameIndices[i] -= 1
				}
			}
			cancel()
			conn.Close()
			/////////////////
			//if len(s.peers)/2 < count1 {
			//	s.commitIndex += 1
			//	return s.metaStore.UpdateFile(ctx, filemeta)
			//}
		}
		if count_s > len(s.peers)/2 {
			could_upd = true
			break
		}

	}

	if could_upd {

		// Update the commit index to the last entry in the log
		//s.commitIndex = int64(len(s.log) - 1)
		ix := int(s.commitIndex + 1)
		for {
			if ix >= len(s.log) {
				break
			}
			s.metaStore.FileMetaMap[s.log[ix].FileMetaData.Filename] = s.log[ix].FileMetaData
			ix += 1
		}
		s.commitIndex = int64(len(s.log) - 1)
	}

	return &v, nil
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	//fmt.Println("Entering Append Entries ", s.id)
	//s.isLeaderMutex.Lock()
	//defer s.isLeaderMutex.Unlock()
	//
	//if s.isCrashed {
	//	return nil, ERR_SERVER_CRASHED
	//	//Error("This server already crashed, couldn't take Append Entry")
	//}
	//leader Check
	//s.isLeaderMutex.Lock()
	//// If the server is not the leader, return an error
	//if !s.isLeader {
	//	s.isLeaderMutex.Unlock()
	//	return nil, ERR_NOT_LEADER
	//}
	//s.isLeaderMutex.Unlock()

	//crash Check
	s.isCrashedMutex.Lock()
	crashStatus := s.isCrashed
	s.isCrashedMutex.Unlock()
	if crashStatus {
		return nil, ERR_SERVER_CRASHED
		//Error("This server already crashed, couldn't take Append Entry")
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < s.term {
		return &AppendEntryOutput{Term: s.term, Success: false}, nil
	}
	//fmt.Println("1")
	if input.Term > s.term {
		// update term and convert to follower
		s.isLeader = false
		s.term = input.Term
	}
	//fmt.Println("2")

	// check if log contains entry at prevLogIndex whose term matches prevLogTerm
	if (input.PrevLogIndex) >= int64(0) {
		if len(s.log) <= int(input.PrevLogIndex) || s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
			return &AppendEntryOutput{Term: s.term, Success: false}, nil
		}
	}
	//fmt.Println("3")

	// delete conflicting existing entry and all that follow it
	for i := input.GetPrevLogIndex() + 1; i < int64(len(s.log)); i++ {
		if s.log[i].Term != input.Entries[i-input.GetPrevLogIndex()-1].Term {
			s.log = s.log[:i]
			break
		}
	}

	//fmt.Println("4")
	// append any new entries not already in the log
	s.log = append(s.log, input.Entries[int64(len(s.log))-input.GetPrevLogIndex()-1:]...)

	// update commit index
	if input.LeaderCommit > s.commitIndex {
		lastNewEntryIndex := len(s.log) - 1
		//s.commitIndex = min(input.LeaderCommit, int64(lastNewEntryIndex))

		if input.LeaderCommit < int64(lastNewEntryIndex) {
			s.commitIndex = input.LeaderCommit
		} else {
			s.commitIndex = int64(lastNewEntryIndex)
		}
	}

	//fmt.Println("5")
	// apply all committed entries
	for s.lastApplied < s.commitIndex {
		entry := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}
	//fmt.Println(s)
	//fmt.Println("Exiting AppendEntries ", s.id)
	return &AppendEntryOutput{Term: s.term, Success: true}, nil

}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//fmt.Println("Entering Setting Leader")

	//leader Check
	//s.isLeaderMutex.Lock()
	//// If the server is not the leader, return an error
	//if !s.isLeader {
	//	s.isLeaderMutex.Unlock()
	//	return nil, ERR_NOT_LEADER
	//}
	//s.isLeaderMutex.Unlock()

	////crash Check
	s.isCrashedMutex.Lock()
	crashStatus := s.isCrashed
	s.isCrashedMutex.Unlock()
	if crashStatus {
		return nil, ERR_SERVER_CRASHED
		//Error("This server already crashed, couldn't take Append Entry")
	}

	s.isLeader = true
	s.term++
	return &Success{
		Flag: true,
	}, nil

}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//fmt.Println(s)
	//fmt.Println("Entering SendHeartBeat")
	//leader Check
	s.isLeaderMutex.Lock()
	// If the server is not the leader, return an error
	if !s.isLeader {
		s.isLeaderMutex.Unlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.Unlock()

	//fmt.Println("After Leader Check")
	//crash Check
	s.isCrashedMutex.Lock()
	crashStatus := s.isCrashed
	s.isCrashedMutex.Unlock()
	if crashStatus {
		return nil, ERR_SERVER_CRASHED
		//Error("This server already crashed, couldn't take Append Entry")
	}

	//fmt.Println("After Checks")
	// Create an AppendEntryInput object with empty entries to send a heartbeat
	//input := &AppendEntryInput{
	//	Term:         s.term,
	//	PrevLogIndex: s.lastApplied,
	//	PrevLogTerm:  s.log[s.lastApplied].Term,
	//	Entries:      []*UpdateOperation{},
	//	LeaderCommit: s.commitIndex,
	//}

	// Send the heartbeat to all the followers
	for i, peer := range s.peers {
		//conn, err := grpc.Dial(peer, grpc.WithInsecure())
		//if err != nil {
		//	// Handle error
		//	continue
		//}
		//defer conn.Close()
		//
		//client := NewRaftSurfstoreClient(conn)
		//_, err = client.AppendEntries(context.Background(), input)
		//if err != nil {
		//	continue
		//}

		if s.id == int64(i) {
			continue
		}
		//fmt.Println("Dialing now...And my i, sid is ", i, s.id)
		conn, err := grpc.Dial(peer, grpc.WithInsecure())
		if err != nil {
			// Handle error
			conn.Close()
			continue
			//return nil, err
		}
		//defer conn.Close()
		//fmt.Println("Creating NewRaftStoreClient", i)
		client := NewRaftSurfstoreClient(conn)
		t := int64(-1)
		//e := s.log
		var e []*UpdateOperation
		if s.sameIndices[i] != -1 {
			t = s.log[s.sameIndices[i]].Term
			//e = s.log[s.sameIndices[i]:]
		}
		if int(s.sameIndices[i]+1) < len(s.log) {
			e = s.log[s.sameIndices[i]+1:]
		}
		// Create an AppendEntryInput object with empty entries to send a heartbeat
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: s.sameIndices[i],
			PrevLogTerm:  t,
			Entries:      e,
			LeaderCommit: s.commitIndex,
		}
		//fmt.Println("Creating context and append entries", i)
		nctx, cancel := context.WithTimeout(context.Background(), time.Second)

		_, err = client.AppendEntries(nctx, input)
		cancel()
		if err != nil {
			conn.Close()
			//fmt.Println("Error in AppendEntries", i)
			continue
		}
		conn.Close()
		//fmt.Println("loop done for", i)
	}
	//fmt.Println("All heartbeat sent")
	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	//fmt.Println("entered getInternalState")
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	//fmt.Println("returned the fileInfoMap")
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
