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

import "time"

type TxnMonitor struct{}

var txnMonitor = new(TxnMonitor)

var txnOrderedListPolicy = &ListPolicy{
	attributes: ListOrderOrdered,
	flags:      ListWriteFlagsAddUnique | ListWriteFlagsNoFail | ListWriteFlagsPartial,
}

const binNameId = "id"
const binNameDigests = "keyds"

func (tm *TxnMonitor) addKey(cluster *Cluster, policy *WritePolicy, cmdKey *Key) {
	txn := policy.Txn

	if txn.WriteExistsForKey(cmdKey) {
		// Transaction monitor already contains this key.
		return
	}

	ops := tm.getTranOps(txn, cmdKey)
	tm.addWriteKeys(cluster, policy.GetBasePolicy(), ops)
}

func (tm *TxnMonitor) addKeys(cluster *Cluster, policy *BatchPolicy, keys []*Key) {
	ops := tm.getTranOpsFromKeys(policy.Txn, keys)
	tm.addWriteKeys(cluster, policy.GetBasePolicy(), ops)
}

func (tm *TxnMonitor) addKeysFromRecords(cluster *Cluster, policy *BatchPolicy, records []BatchRecordIfc) {
	ops := tm.getTranOpsFromBatchRecords(policy.Txn, records)

	if ops != nil {
		tm.addWriteKeys(cluster, policy.GetBasePolicy(), ops)
	}
}

func (tm *TxnMonitor) getTranOps(txn *Txn, cmdKey *Key) []*Operation {
	txn.SetNamespace(cmdKey.namespace)

	if txn.MonitorExists() {
		return []*Operation{
			ListAppendWithPolicyOp(txnOrderedListPolicy, binNameDigests, NewBytesValue(cmdKey.Digest())),
		}
	} else {
		return []*Operation{
			PutOp(NewBin(binNameId, txn.Id())),
			ListAppendWithPolicyOp(txnOrderedListPolicy, binNameDigests, NewBytesValue(cmdKey.Digest())),
		}
	}
}

func (tm *TxnMonitor) getTranOpsFromKeys(txn *Txn, keys []*Key) []*Operation {
	list := make([]Value, 0, len(keys))

	for _, key := range keys {
		txn.SetNamespace(key.namespace)
		list = append(list, NewBytesValue(key.Digest()))
	}
	return tm.getTranOpsFromValueList(txn, list)
}

func (tm *TxnMonitor) getTranOpsFromBatchRecords(txn *Txn, records []BatchRecordIfc) []*Operation {
	list := make([]Value, 0, len(records))

	for _, br := range records {
		txn.SetNamespace(br.key().namespace)

		if br.BatchRec().hasWrite {
			list = append(list, NewBytesValue(br.key().Digest()))
		}
	}

	if len(list) == 0 {
		// Readonly batch does not need to add key digests.
		return nil
	}
	return tm.getTranOpsFromValueList(txn, list)
}

func (tm *TxnMonitor) getTranOpsFromValueList(txn *Txn, list []Value) []*Operation {
	vals := make([]interface{}, len(list))
	for i := range list {
		vals[i] = list[i]
	}

	if txn.MonitorExists() {
		return []*Operation{
			ListAppendWithPolicyOp(txnOrderedListPolicy, binNameDigests, vals...),
		}
	} else {
		return []*Operation{
			PutOp(NewBin(binNameId, txn.Id())),
			ListAppendWithPolicyOp(txnOrderedListPolicy, binNameDigests, vals...),
		}
	}
}

func (tm *TxnMonitor) addWriteKeys(cluster *Cluster, policy *BasePolicy, ops []*Operation) Error {
	txnKey := getTxnMonitorKey(policy.Txn)
	wp := tm.copyTimeoutPolicy(policy)
	args := operateArgs{writePolicy: wp, operations: ops}
	cmd, err := newTxnAddKeysCommand(cluster, txnKey, args)
	if err != nil {
		return err
	}
	return cmd.Execute()
}

func (tm *TxnMonitor) copyTimeoutPolicy(policy *BasePolicy) *WritePolicy {
	// Inherit some fields from the original command's policy.
	wp := NewWritePolicy(0, 0)
	wp.Txn = policy.Txn
	// wp.ConnectTimeout = policy.ConnectTimeout
	wp.SocketTimeout = policy.SocketTimeout
	wp.TotalTimeout = policy.TotalTimeout
	// wp.TimeoutDelay = policy.TimeoutDelay
	wp.MaxRetries = policy.MaxRetries
	wp.SleepBetweenRetries = policy.SleepBetweenRetries
	wp.UseCompression = policy.UseCompression
	wp.RespondPerEachOp = true

	// Note that the server only accepts the timeout on MRT monitor record create.
	// The server ignores the MRT timeout field on successive MRT monitor record
	// updates.
	wp.Expiration = uint32(policy.Txn.timeout / time.Second)

	return wp
}

func getTxnMonitorKey(txn *Txn) *Key {
	key, _ := NewKey(txn.GetNamespace(), "<ERO~MRT", txn.Id())
	return key
}
