// Copyright 2014-2022 Aerospike, Inc.
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
	"github.com/aerospike/aerospike-client-go/v7/types"
)

type batchIndexCommandGet struct {
	batchCommandOperate
}

func newBatchIndexCommandGet(
	client clientIfc,
	batch *batchNode,
	policy *BatchPolicy,
	records []*BatchRead,
	isOperation bool,
) batchIndexCommandGet {
	recIfcs := make([]BatchRecordIfc, len(records))
	for i := range records {
		recIfcs[i] = records[i]
	}

	res := batchIndexCommandGet{
		batchCommandOperate: newBatchCommandOperate(client, batch, policy, recIfcs),
	}
	res.txn = policy.Txn
	return res
}

func (cmd *batchIndexCommandGet) cloneBatchCommand(batch *batchNode) batcher {
	res := *cmd
	res.batch = batch
	res.node = batch.Node

	return &res
}

func (cmd *batchIndexCommandGet) Execute() Error {
	if len(cmd.batch.offsets) == 1 {
		return cmd.executeSingle(cmd.client)
	}
	return cmd.execute(cmd)
}

func (cmd *batchIndexCommandGet) executeSingle(client clientIfc) Error {
	for _, br := range cmd.records {
		br := br.(*BatchRead)
		var ops []*Operation
		if br.headerOnly() {
			ops = []*Operation{GetHeaderOp()}
		} else if len(br.BinNames) > 0 {
			for i := range br.BinNames {
				ops = append(ops, GetBinOp(br.BinNames[i]))
			}
		} else {
			ops = br.Ops
		}
		res, err := client.operate(cmd.policy.toWritePolicy(), br.Key, true, ops...)
		br.setRecord(res)
		if err != nil {
			br.setRawError(err)

			// Key not found is NOT an error for batch requests
			if err.resultCode() == types.KEY_NOT_FOUND_ERROR {
				continue
			}

			if err.resultCode() == types.FILTERED_OUT {
				cmd.filteredOutCnt++
				continue
			}

			if cmd.policy.AllowPartialResults {
				continue
			}
			return err
		}
	}
	return nil
}
