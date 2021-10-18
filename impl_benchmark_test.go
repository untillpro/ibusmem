/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package ibusmem

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ibus "github.com/untillpro/airs-ibus"
)

// BenchmarkSectionedRequestResponse/#00-8         	   43231	     27062 ns/op	     36952 rps

func BenchmarkSectionedRequestResponse(b *testing.B) {
	bus := bus{}
	requestHandler := func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := bus.SendParallelResponse2(ctx, sender)
		go func() {
			require.Nil(b, rs.ObjectSection("secObj", []string{"meta"}, "elem"))
			rs.StartMapSection("secMap", []string{"classifier", "2"})
			require.Nil(b, rs.SendElement("id1", "elem"))
			require.Nil(b, rs.SendElement("id2", "elem"))
			rs.StartArraySection("secArr", []string{"classifier", "4"})
			require.Nil(b, rs.SendElement("", "arrEl1"))
			require.Nil(b, rs.SendElement("", "arrEl2"))
			rs.StartMapSection("deps", []string{"classifier", "3"})
			require.Nil(b, rs.SendElement("id3", "elem"))
			require.Nil(b, rs.SendElement("id4", "elem"))
			rs.Close(errors.New("test error"))
		}()
	}
	bus.requestHandler = requestHandler

	b.Run("", func(b *testing.B) {
		start := time.Now()
		for i := 0; i < b.N; i++ {
			_, sections, _, _ := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)

			section := <-sections
			secObj := section.(ibus.IObjectSection)
			secObj.Value()

			section = <-sections
			secMap := section.(ibus.IMapSection)
			secMap.Next()
			secMap.Next()

			section = <-sections
			secArr := section.(ibus.IArraySection)
			secArr.Next()
			secArr.Next()

			section = <-sections
			secMap = section.(ibus.IMapSection)
			secMap.Next()
			secMap.Next()

			if _, ok := <-sections; ok {
				b.Fatal()
			}
		}
		elapsed := time.Since(start).Seconds()
		b.ReportMetric(float64(b.N)/elapsed, "rps")
	})
}
