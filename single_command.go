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

	"github.com/aerospike/aerospike-client-go/v7/types"
	Buffer "github.com/aerospike/aerospike-client-go/v7/utils/buffer"
)

type singleCommand struct {
	baseCommand

	cluster   *Cluster
	key       *Key
	partition *Partition
}

func newSingleCommand(cluster *Cluster, key *Key, partition *Partition) singleCommand {
	return singleCommand{
		baseCommand: baseCommand{},
		cluster:     cluster,
		key:         key,
		partition:   partition,
	}
}

func (cmd *singleCommand) getConnection(policy Policy) (*Connection, Error) {
	return cmd.node.getConnectionWithHint(policy.GetBasePolicy().deadline(), policy.GetBasePolicy().socketTimeout(), cmd.key.digest[0])
}

func (cmd *singleCommand) putConnection(conn *Connection) {
	cmd.node.putConnectionWithHint(conn, cmd.key.digest[0])
}

func (cmd *singleCommand) emptySocket(conn *Connection) Error {
	// There should not be any more bytes.
	// Empty the socket to be safe.
	sz := Buffer.BytesToInt64(cmd.dataBuffer, 0)
	headerLength := cmd.dataBuffer[8]
	receiveSize := int(sz&0xFFFFFFFFFFFF) - int(headerLength)

	// Read remaining message bytes.
	if receiveSize > 0 {
		if err := cmd.sizeBufferSz(receiveSize, false); err != nil {
			return err
		}
		if _, err := conn.Read(cmd.dataBuffer, receiveSize); err != nil {
			return err
		}
	}
	return nil
}

func (cmd *singleCommand) skipFields(fieldCount int) (err Error) {
	// There can be fields in the response (setname etc).
	// But for now, ignore them. Expose them to the API if needed in the future.
	for i := 0; i < fieldCount; i++ {
		fieldLen := Buffer.BytesToUint32(cmd.dataBuffer, cmd.dataOffset)
		cmd.dataOffset += 4 + int(fieldLen)
	}
	return nil
}

func (cmd *singleCommand) parseFields(
	txn *Txn,
	resultCode types.ResultCode,
	fieldCount int,
	key *Key,
	hasWrite bool,
) Error {
	if txn == nil {
		if err := cmd.skipFields(fieldCount); err != nil {
			return err
		}
	}

	var version *uint64

	for i := 0; i < fieldCount; i++ {
		len := Buffer.BytesToInt32(cmd.dataBuffer, cmd.dataOffset)
		cmd.dataOffset += 4

		typ := cmd.dataBuffer[cmd.dataOffset]
		cmd.dataOffset++
		size := len - 1

		if FieldType(typ) == RECORD_VERSION {
			if size == 7 {
				version = Buffer.VersionBytesToUint64(cmd.dataBuffer, cmd.dataOffset)
			} else {
				return newError(types.PARSE_ERROR, fmt.Sprintf("Record version field has invalid size: %v", size))
			}
		}
		cmd.dataOffset += int(size)
	}

	if hasWrite {
		txn.OnWrite(key, version, resultCode)
	} else {
		txn.OnRead(key, version)
	}

	return nil
}
