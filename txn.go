// Copyright 2014-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"sync/atomic"
	"time"

	sm "github.com/aerospike/aerospike-client-go/v8/internal/atomic/map"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// Transaction state.
type TxnState byte

const (
	TxnStateOpen TxnState = iota
	TxnStateVerified
	TxnStateCommitted
	TxnStateAborted
)

var txnRandomState atomic.Int64

func init() {
	txnRandomState.Store(time.Now().UnixNano())
}

// Transaction. Each command in the Transaction must use the same namespace.
type Txn struct {
	id             int64
	reads          sm.Map[*Key, *uint64]
	writes         sm.Map[*Key, struct{}]
	namespace      *string
	timeout        int
	deadline       int
	monitorInDoubt bool
	inDoubt        bool
	rollAttempted  bool
	state          TxnState
}

// Create Transaction, assign random transaction id and initialize reads/writes hashmaps with default capacities.
//
// The default client Transaction timeout is zero. This means use the server configuration transaction-duration
// as the Transaction timeout. The default transaction-duration is 10 seconds.
func NewTxn() *Txn {
	return &Txn{
		id:      createTxnId(),
		reads:   *sm.New[*Key, *uint64](16),
		writes:  *sm.New[*Key, struct{}](16),
		timeout: 0,
		state:   TxnStateOpen,
	}
}

// Create Transaction, assign random transaction id and initialize reads/writes hashmaps with given capacities.
//
// readsCapacity     expected number of record reads in the Transaction. Minimum value is 16.
// writesCapacity    expected number of record writes in the Transaction. Minimum value is 16.
func NewTxnWithCapacity(readsCapacity, writesCapacity int) *Txn {
	if readsCapacity < 16 {
		readsCapacity = 16
	}

	if writesCapacity < 16 {
		writesCapacity = 16
	}

	return &Txn{
		id:      createTxnId(),
		reads:   *sm.New[*Key, *uint64](readsCapacity),
		writes:  *sm.New[*Key, struct{}](writesCapacity),
		timeout: 0,
		state:   TxnStateOpen,
	}
}

func createTxnId() int64 {
	// xorshift64* doesn't generate zeroes.
	var oldState, newState int64

	oldState = txnRandomState.Load()
	newState = oldState
	newState ^= newState >> 12
	newState ^= newState << 25
	newState ^= newState >> 27

	for !txnRandomState.CompareAndSwap(oldState, newState) {
		oldState = txnRandomState.Load()
		newState = oldState
		newState ^= newState >> 12
		newState ^= newState << 25
		newState ^= newState >> 27
	}

	return newState // 0x2545f4914f6cdd1dl;
}

// Return Transaction ID.
func (txn *Txn) Id() int64 {
	return txn.id
}

// Return Transaction ID.
func (txn *Txn) State() TxnState {
	return txn.state
}

// Set Transaction ID.
func (txn *Txn) SetState(state TxnState) {
	txn.state = state
}

// Process the results of a record read. For internal use only.
func (txn *Txn) OnRead(key *Key, version *uint64) {
	if version != nil {
		txn.reads.Set(key, version)
	}
}

// Get record version for a given key.
func (txn *Txn) GetReadVersion(key *Key) *uint64 {
	return txn.reads.Get(key)
}

// Get all read keys and their versions.
func (txn *Txn) ReadExistsForKey(key *Key) bool {
	return txn.reads.Exists(key)
}

// Get all read keys and their versions.
func (txn *Txn) GetReads() map[*Key]*uint64 {
	return txn.reads.Clone()
}

// Process the results of a record write. For internal use only.
func (txn *Txn) OnWrite(key *Key, version *uint64, resultCode types.ResultCode) {
	if version != nil {
		txn.reads.Set(key, version)
	} else if resultCode == 0 {
		txn.reads.Delete(key)
		txn.writes.Set(key, struct{}{})
	}
}

// Add key to write hash when write command is in doubt (usually caused by timeout).
func (txn *Txn) OnWriteInDoubt(key *Key) {
	txn.reads.Delete(key)
	txn.writes.Set(key, struct{}{})
}

// Get all write keys and their versions.
func (txn *Txn) GetWrites() []*Key {
	return txn.writes.Keys()
}

// Get all write keys and their versions.
func (txn *Txn) WriteExistsForKey(key *Key) bool {
	return txn.writes.Exists(key)
}

// Return Transaction namespace.
func (txn *Txn) GetNamespace() string {
	return *txn.namespace
}

// Verify current Transaction state and namespace for a future read command.
func (txn *Txn) prepareRead(ns string) Error {
	if err := txn.VerifyCommand(); err != nil {
		return err
	}
	return txn.SetNamespace(ns)
}

// Verify current Transaction state and namespaces for a future batch read command.
func (txn *Txn) prepareReadForKeys(keys []*Key) Error {
	if err := txn.VerifyCommand(); err != nil {
		return err
	}
	return txn.setNamespaceForKeys(keys)
}

// Verify current Transaction state and namespaces for a future batch read command.
func (txn *Txn) prepareBatchReads(records []*BatchRead) Error {
	if err := txn.VerifyCommand(); err != nil {
		return err
	}
	return txn.setNamespaceForBatchReads(records)
}

// Verify current Transaction state and namespaces for a future batch read command.
func (txn *Txn) prepareReadForBatchRecordsIfc(records []BatchRecordIfc) Error {
	if err := txn.VerifyCommand(); err != nil {
		return err
	}
	return txn.setNamespaceForBatchRecordsIfc(records)
}

// Verify that the Transaction state allows future commands.
func (txn *Txn) VerifyCommand() Error {
	if txn.state != TxnStateOpen {
		return newError(types.COMMON_ERROR, "Issuing commands to this transaction is forbidden because it has been ended by a commit or abort")
	}
	return nil
}

// Set Transaction namespace only if doesn't already exist.
// If namespace already exists, verify new namespace is the same.
func (txn *Txn) SetNamespace(ns string) Error {
	if txn.namespace == nil {
		txn.namespace = &ns
	} else if *txn.namespace != ns {
		return newError(types.COMMON_ERROR, "Namespace must be the same for all commands in the Transaction. orig: "+
			*txn.namespace+" new: "+ns)
	}
	return nil
}

// Set Transaction namespaces for each key only if doesn't already exist.
// If namespace already exists, verify new namespace is the same.
func (txn *Txn) setNamespaceForKeys(keys []*Key) Error {
	for _, key := range keys {
		if err := txn.SetNamespace(key.namespace); err != nil {
			return err
		}
	}
	return nil
}

// Set Transaction namespaces for each key only if doesn't already exist.
// If namespace already exists, verify new namespace is the same.
func (txn *Txn) setNamespaceForBatchReads(records []*BatchRead) Error {
	for _, br := range records {
		if err := txn.SetNamespace(br.key().namespace); err != nil {
			return err
		}
	}
	return nil
}

// Set Transaction namespaces for each key only if doesn't already exist.
// If namespace already exists, verify new namespace is the same.
func (txn *Txn) setNamespaceForBatchRecordsIfc(records []BatchRecordIfc) Error {
	for _, br := range records {
		if err := txn.SetNamespace(br.key().namespace); err != nil {
			return err
		}
	}
	return nil
}

// Get Transaction deadline.
func (txn *Txn) GetTimeout() time.Duration {
	return time.Duration(txn.timeout) * time.Second
}

// Set Transaction timeout in seconds. The timer starts when the Transaction monitor record is
// created.
// This occurs when the first command in the Transaction is executed. If the timeout is
// reached before
// a commit or abort is called, the server will expire and rollback the Transaction.
//
// If the Transaction timeout is zero, the server configuration transaction-duration is used.
// The default transaction-duration is 10 seconds.
func (txn *Txn) SetTimeout(timeout time.Duration) {
	txn.timeout = int(timeout / time.Second)
}

// Get Transaction inDoubt.
func (txn *Txn) GetInDoubt() bool {
	return txn.inDoubt
}

// Set Transaction inDoubt. For internal use only.
func (txn *Txn) SetInDoubt(inDoubt bool) {
	txn.inDoubt = inDoubt
}

// Set that the Transaction monitor existence is in doubt.
func (txn *Txn) SetMonitorInDoubt() {
	txn.monitorInDoubt = true
}

// Does Transaction monitor record exist or is in doubt.
func (txn *Txn) MonitorMightExist() bool {
	return txn.deadline != 0 || txn.monitorInDoubt
}

// Does Transaction monitor record exist.
func (txn *Txn) MonitorExists() bool {
	return txn.deadline != 0
}

// Verify that commit/abort is only attempted once. For internal use only.
func (txn *Txn) SetRollAttempted() bool {
	if txn.rollAttempted {
		return false
	}
	txn.rollAttempted = true
	return true
}

// Clear Transaction. Remove all tracked keys.
func (txn *Txn) Clear() {
	txn.namespace = nil
	txn.deadline = 0
	txn.reads.Clear()
	txn.writes.Clear()
}
