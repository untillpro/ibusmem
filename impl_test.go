/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package ibusmem

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ibus "github.com/untillpro/airs-ibus"
)

func TestBasicUsage_Bus(t *testing.T) {
	t.Run("Should handle single response", func(t *testing.T) {
		bus := bus{}
		requestHandler := func(ctx context.Context, sender interface{}, request ibus.Request) {
			bus.SendResponse(ctx, sender, ibus.Response{Data: []byte("hello world")})
		}
		bus.requestHandler = requestHandler

		response, _, _, _ := bus.SendRequest2(context.Background(), ibus.Request{}, time.Minute)

		require.Equal(t, "hello world", string(response.Data))
	})
	t.Run("Should handle section response", func(t *testing.T) {
		arrayEntry := func(value []byte, ok bool) string {
			return string(value)
		}
		mapEntry := func(name string, value []byte, ok bool) string {
			return name + ":" + string(value)
		}
		bus := bus{}
		requestHandler := func(ctx context.Context, sender interface{}, request ibus.Request) {
			rsender := bus.SendParallelResponse2(ctx, sender)
			go func() {
				rsender.StartArraySection("array", []string{"array-path"})
				require.Nil(t, rsender.SendElement("", "element1"))
				require.Nil(t, rsender.SendElement("", "element2"))
				rsender.StartMapSection("map", []string{"map-path"})
				require.Nil(t, rsender.SendElement("key1", "value1"))
				require.Nil(t, rsender.SendElement("key2", "value2"))
				require.Nil(t, rsender.ObjectSection("object", []string{"object-path"}, "value"))
				rsender.Close(nil)
			}()
		}
		bus.requestHandler = requestHandler

		_, sections, secErr, _ := bus.SendRequest2(context.Background(), ibus.Request{}, time.Minute)

		section := <-sections
		arraySection := section.(arraySection)
		require.Equal(t, "array", arraySection.Type())
		require.Equal(t, []string{"array-path"}, arraySection.Path())
		require.Equal(t, `"element1"`, arrayEntry(arraySection.Next()))
		require.Equal(t, `"element2"`, arrayEntry(arraySection.Next()))
		value, ok := arraySection.Next()
		require.Nil(t, value)
		require.False(t, ok)
		section = <-sections
		mapSection := section.(mapSection)
		require.Equal(t, "map", mapSection.Type())
		require.Equal(t, []string{"map-path"}, mapSection.Path())
		require.Equal(t, `key1:"value1"`, mapEntry(mapSection.Next()))
		require.Equal(t, `key2:"value2"`, mapEntry(mapSection.Next()))
		name, value, ok := mapSection.Next()
		require.Equal(t, "", name)
		require.Nil(t, value)
		require.False(t, ok)
		section = <-sections
		objectSection := section.(objectSection)
		require.Equal(t, "object", objectSection.Type())
		require.Equal(t, []string{"object-path"}, objectSection.Path())
		require.Equal(t, `"value"`, string(objectSection.Value()))
		require.Nil(t, secErr)
	})
}
