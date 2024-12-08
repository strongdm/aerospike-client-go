//go:build !app_engine

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

package aerospike_test

import (
	as "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/aerospike-client-go/v7/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Aerospike", func() {

	gg.Describe("Multi Record Transaction (MRT) operations", gg.Ordered, func() {
		var ns = *namespace
		var set = randString(50)
		const binName = "bin"

		gg.BeforeAll(func() {
			// skip the tests if the cluster is not in SC mode or the server is older than v8
			if serverIsOlderThan("8") {
				gg.Skip("Not supported in server before v8")
			}

			if !as.ConfiguredAsStrongConsistency(client.(*as.Client), ns) {
				gg.Skip("Not supported in namespaces without Strong Consistency support")
			}

			const luaFunc = `
				local function putBin(r,name,value)
					if not aerospike:exists(r) then aerospike:create(r) end
					r[name] = value
					aerospike:update(r)
				end

				-- Set a particular bin
				function writeBin(r,name,value)
					putBin(r,name,value)
				end
			`

			regTask, err := client.RegisterUDF(nil, []byte(luaFunc), "record_example.lua", as.LUA)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(<-regTask.OnComplete()).ToNot(gm.HaveOccurred())
		})

		gg.It("must write and commit", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err := client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()

			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			err = client.PutBins(wp, key, as.NewBin(binName, "val2"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val2"))
		}) // it

		gg.It("must write twice", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			txn := as.NewTxn()

			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			err = client.PutBins(wp, key, as.NewBin(binName, "val2"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val2"))
		}) // it

		gg.It("must write correctly during conflict", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			txn1 := as.NewTxn()
			wp1 := as.NewWritePolicy(0, 0)
			wp1.Txn = txn1

			txn2 := as.NewTxn()
			wp2 := as.NewWritePolicy(0, 0)
			wp2.Txn = txn2

			err = client.PutBins(wp1, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			err = client.PutBins(wp2, key, as.NewBin(binName, "val2"))
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.(*as.AerospikeError).ResultCode).To(gm.Equal(types.MRT_BLOCKED))

			status, err := client.Commit(txn1)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			status, err = client.Commit(txn2)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val1"))
		}) // it

		gg.It("must be blocked before other transaction is committed", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			defer client.Commit(txn)

			err = client.PutBins(wp, key, as.NewBin(binName, "val2"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			err = client.PutBins(nil, key, as.NewBin(binName, "val3"))
			gm.Expect(err.(*as.AerospikeError).ResultCode).To(gm.Equal(types.MRT_BLOCKED))
		}) // it

		gg.It("must support write and read", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			err = client.PutBins(wp, key, as.NewBin(binName, "val2"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val2"))
		}) // it

		gg.It("must support write and abort", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			err = client.PutBins(wp, key, as.NewBin(binName, "val2"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			rp := as.NewPolicy()
			rp.Txn = txn

			record, err := client.Get(rp, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val2"))

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))

			record, err = client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val1"))
		}) // it

		gg.It("must support delete", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.DurableDelete = true
			wp.Txn = txn

			existed, err := client.Delete(wp, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(existed).To(gm.BeTrue())

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(types.KEY_NOT_FOUND_ERROR)).To(gm.BeTrue())
			gm.Expect(record).To(gm.BeNil())
		}) // it

		gg.It("must support delete and abort", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.DurableDelete = true
			wp.Txn = txn

			existed, err := client.Delete(wp, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(existed).To(gm.BeTrue())

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val1"))
		}) // it

		gg.It("must support delete twice", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.DurableDelete = true
			wp.Txn = txn

			existed, err := client.Delete(wp, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(existed).To(gm.BeTrue())

			existed, err = client.Delete(wp, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(existed).To(gm.BeFalse())

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(types.KEY_NOT_FOUND_ERROR)).To(gm.BeTrue())
			gm.Expect(record).To(gm.BeNil())
		}) // it

		gg.It("must support touch", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			err := client.Touch(wp, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val1"))
			gm.Expect(record.Generation).To(gm.BeNumerically(">", 1))
		}) // it

		gg.It("must support touch and abort", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			err := client.Touch(wp, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val1"))
			gm.Expect(record.Generation).To(gm.Equal(uint32(3)))
		}) // it

		gg.It("must support operate write", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err := client.PutBins(nil, key, as.NewBin(binName, "val1"), as.NewBin("bin2", "bal1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()

			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			record, err := client.Operate(wp, key,
				as.PutOp(as.NewBin(binName, "val2")),
				as.GetBinOp("bin2"),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins["bin2"]).To(gm.Equal("bal1"))

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err = client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val2"))
		}) // it

		gg.It("must support operate write abort", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err := client.PutBins(nil, key, as.NewBin(binName, "val1"), as.NewBin("bin2", "bal1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()

			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			record, err := client.Operate(wp, key,
				as.PutOp(as.NewBin(binName, "val2")),
				as.GetBinOp("bin2"),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins["bin2"]).To(gm.Equal("bal1"))

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))

			record, err = client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val1"))
		}) // it

		gg.It("must support UDF", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err := client.PutBins(nil, key, as.NewBin(binName, "val1"), as.NewBin("bin2", "bal1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()

			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			_, err = client.Execute(
				wp,
				key,
				"record_example",
				"writeBin",
				as.NewValue(binName),
				as.NewValue("val2"),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val2"))
		}) // it

		gg.It("must support UDF and abort", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err := client.PutBins(nil, key, as.NewBin(binName, "val1"), as.NewBin("bin2", "bal1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()

			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			_, err = client.Execute(
				wp,
				key,
				"record_example",
				"writeBin",
				as.NewValue(binName),
				as.NewValue("val2"),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val1"))
		}) // it

		gg.It("must support batchDelete", func() {
			bin := as.NewBin(binName, 1)
			keys := make([]*as.Key, 10)

			for i := range keys {
				key, _ := as.NewKey(ns, set, i)
				keys[i] = key

				err := client.PutBins(nil, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}

			records, err := client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(1))
			}

			txn := as.NewTxn()

			bin = as.NewBin(binName, 2)

			bp := as.NewBatchPolicy()
			bp.Txn = txn

			dp := as.NewBatchDeletePolicy()
			dp.DurableDelete = true

			_, err = client.BatchDelete(bp, dp, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			records, err = client.BatchGet(bp, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).To(gm.BeNil())
			}

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			records, err = client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).To(gm.BeNil())
			}
		}) // it

		gg.It("must support batchDelete and abort", func() {
			bin := as.NewBin(binName, 1)
			keys := make([]*as.Key, 10)

			for i := range keys {
				key, _ := as.NewKey(ns, set, i)
				keys[i] = key

				err := client.PutBins(nil, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}

			records, err := client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(1))
			}

			txn := as.NewTxn()

			bp := as.NewBatchPolicy()
			bp.Txn = txn

			dp := as.NewBatchDeletePolicy()
			dp.DurableDelete = true

			_, err = client.BatchDelete(bp, dp, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))

			records, err = client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(1))
			}
		}) // it

		gg.It("must support batch", func() {
			bin := as.NewBin(binName, 1)
			keys := make([]*as.Key, 10)

			for i := range keys {
				key, _ := as.NewKey(ns, set, i)
				keys[i] = key

				err := client.PutBins(nil, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}

			records, err := client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(1))
			}

			txn := as.NewTxn()

			bin = as.NewBin(binName, 2)

			bp := as.NewBatchPolicy()
			bp.Txn = txn

			brecs := make([]as.BatchRecordIfc, len(keys))
			for i := range brecs {
				brecs[i] = as.NewBatchWrite(nil, keys[i], as.PutOp(bin))
			}

			err = client.BatchOperate(bp, brecs)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			records, err = client.BatchGet(bp, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(2))
			}

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			records, err = client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(2))
			}
		}) // it

		gg.It("must support batch and abort", func() {
			bin := as.NewBin(binName, 1)
			keys := make([]*as.Key, 10)

			for i := range keys {
				key, _ := as.NewKey(ns, set, i)
				keys[i] = key
				err := client.PutBins(nil, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}

			records, err := client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(1))
			}

			txn := as.NewTxn()

			bin = as.NewBin(binName, 2)

			pp := as.NewBatchPolicy()
			pp.Txn = txn

			brecs := make([]as.BatchRecordIfc, len(keys))
			for i := range brecs {
				brecs[i] = as.NewBatchWrite(nil, keys[i], as.PutOp(bin))
			}

			err = client.BatchOperate(pp, brecs)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))

			records, err = client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(1))
			}
		}) // it
	}) // describe
})
