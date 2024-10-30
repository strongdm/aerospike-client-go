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
	"fmt"

	"github.com/aerospike/aerospike-client-go/v7/logger"
	"github.com/aerospike/aerospike-client-go/v7/types"

	Buffer "github.com/aerospike/aerospike-client-go/v7/utils/buffer"
)

// guarantee batchSingleTxnRollCommand implements command interface
var _ command = &batchSingleTxnRollCommand{}

type batchSingleTxnRollCommand struct {
	singleCommand

	txn    *Txn
	record *BatchRecord
	attr   int
	policy *BatchPolicy
}

func newBatchSingleTxnRollCommand(
	client *Client,
	policy *BatchPolicy,
	txn *Txn,
	record *BatchRecord,
	node *Node,
	attr int,
) (batchSingleTxnRollCommand, Error) {
	var partition *Partition
	var err Error
	if client.cluster != nil {
		partition, err = PartitionForWrite(client.cluster, &policy.BasePolicy, record.Key)
		if err != nil {
			return batchSingleTxnRollCommand{}, err
		}
	}

	res := batchSingleTxnRollCommand{
		singleCommand: newSingleCommand(client.cluster, record.Key, partition),
		txn:           txn,
		record:        record,
		attr:          attr,
		policy:        policy,
	}
	res.node = node

	return res, nil
}

func (cmd *batchSingleTxnRollCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *batchSingleTxnRollCommand) writeBuffer(ifc command) Error {
	return cmd.setTxnRoll(cmd.key, cmd.txn, cmd.attr)
}

func (cmd *batchSingleTxnRollCommand) getNode(ifc command) (*Node, Error) {
	return cmd.node, nil
}

func (cmd *batchSingleTxnRollCommand) prepareRetry(ifc command, isTimeout bool) bool {
	cmd.partition.PrepareRetryWrite(isTimeout)
	node, err := cmd.partition.GetNodeWrite(cmd.cluster)
	if err != nil {
		return false
	}

	cmd.node = node
	return true
}

func (cmd *batchSingleTxnRollCommand) parseResult(ifc command, conn *Connection) Error {
	// Read proto and check if compressed
	if _, err := conn.Read(cmd.dataBuffer, 8); err != nil {
		logger.Logger.Debug("Connection error reading data for ReadCommand: %s", err.Error())
		return err
	}

	if compressedSize := cmd.compressedSize(); compressedSize > 0 {
		// Read compressed size
		if _, err := conn.Read(cmd.dataBuffer, 8); err != nil {
			logger.Logger.Debug("Connection error reading data for ReadCommand: %s", err.Error())
			return err
		}

		if err := cmd.conn.initInflater(true, compressedSize); err != nil {
			return newError(types.PARSE_ERROR, fmt.Sprintf("Error setting up zlib inflater for size `%d`: %s", compressedSize, err.Error()))
		}

		// Read header.
		if _, err := conn.Read(cmd.dataBuffer, int(_MSG_TOTAL_HEADER_SIZE)); err != nil {
			logger.Logger.Debug("Connection error reading data for ReadCommand: %s", err.Error())
			return err
		}
	} else {
		// Read header.
		if _, err := conn.Read(cmd.dataBuffer[8:], int(_MSG_TOTAL_HEADER_SIZE)-8); err != nil {
			logger.Logger.Debug("Connection error reading data for ReadCommand: %s", err.Error())
			return err
		}
	}

	// A number of these are commented out because we just don't care enough to read
	// that section of the header. If we do care, uncomment and check!
	sz := Buffer.BytesToInt64(cmd.dataBuffer, 0)

	// Validate header to make sure we are at the beginning of a message
	if err := cmd.validateHeader(sz); err != nil {
		return err
	}

	headerLength := int(cmd.dataBuffer[8])
	resultCode := types.ResultCode(cmd.dataBuffer[13] & 0xFF)
	// generation := Buffer.BytesToUint32(cmd.dataBuffer, 14)
	// expiration := types.TTL(Buffer.BytesToUint32(cmd.dataBuffer, 18))
	// fieldCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 26)) // almost certainly 0
	// opCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 28))
	receiveSize := int((sz & 0xFFFFFFFFFFFF) - int64(headerLength))

	// Read remaining message bytes.
	if receiveSize > 0 {
		if err := cmd.sizeBufferSz(receiveSize, false); err != nil {
			return err
		}
		if _, err := conn.Read(cmd.dataBuffer, receiveSize); err != nil {
			logger.Logger.Debug("Connection error reading data for ReadCommand: %s", err.Error())
			return err
		}

	}

	if resultCode == 0 {
		cmd.record.ResultCode = types.OK
	} else {
		err := newError(resultCode)
		err.setInDoubt(cmd.isRead(), cmd.commandSentCounter)
	}

	return nil
}

func (cmd *batchSingleTxnRollCommand) setInDoubt() bool {
	if cmd.record.ResultCode == types.NO_RESPONSE {
		cmd.record.InDoubt = true
		return true
	}
	return false
}

func (cmd *batchSingleTxnRollCommand) isRead() bool {
	return false
}

func (cmd *batchSingleTxnRollCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *batchSingleTxnRollCommand) commandType() commandType {
	return ttPut
}
