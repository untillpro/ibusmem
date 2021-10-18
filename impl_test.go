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

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)

		require.Equal(t, "hello world", string(response.Data))
		require.Nil(t, sections)
		require.Nil(t, secErr)
		require.Nil(t, err)
	})
	t.Run("Should handle section response", func(t *testing.T) {
		arrayEntry := func(value []byte, ok bool) string {
			return string(value)
		}
		mapEntry := func(name string, value []byte, ok bool) string {
			return name + ":" + string(value)
		}
		closed := func(c <-chan ibus.ISection) bool {
			_, ok := <-c
			return !ok
		}
		bus := bus{}
		requestHandler := func(ctx context.Context, sender interface{}, request ibus.Request) {
			rs := bus.SendParallelResponse2(ctx, sender)
			go func() {
				rs.StartArraySection("array", []string{"array-path"})
				require.Nil(t, rs.SendElement("", "element1"))
				require.Nil(t, rs.SendElement("", "element2"))
				rs.StartMapSection("map", []string{"map-path"})
				require.Nil(t, rs.SendElement("key1", "value1"))
				require.Nil(t, rs.SendElement("key2", "value2"))
				require.Nil(t, rs.ObjectSection("object", []string{"object-path"}, "value"))
				rs.Close(nil)
			}()
		}
		bus.requestHandler = requestHandler

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)

		require.Nil(t, err)
		require.Empty(t, response)
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
		require.Nil(t, *secErr)
		require.True(t, closed(sections))
	})
}

func TestBus_SendRequest2(t *testing.T) {
	t.Run("Should return timeout error on single response", func(t *testing.T) {
		bus := bus{}
		requestHandler := func(ctx context.Context, sender interface{}, request ibus.Request) {
			time.Sleep(10 * time.Millisecond)
			bus.SendResponse(ctx, sender, ibus.Response{Data: []byte("data")})
		}
		bus.requestHandler = requestHandler

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, 5*time.Millisecond)

		require.Equal(t, ibus.ErrTimeoutExpired, err)
		require.Empty(t, response)
		require.Nil(t, sections)
		require.Nil(t, secErr)
	})
	t.Run("Should return timeout error on section response", func(t *testing.T) {
		bus := bus{}
		requestHandler := func(ctx context.Context, sender interface{}, request ibus.Request) {
			time.Sleep(10 * time.Millisecond)
			_ = bus.SendParallelResponse2(ctx, sender)
		}
		bus.requestHandler = requestHandler

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, 5*time.Millisecond)

		require.Equal(t, ibus.ErrTimeoutExpired, err)
		require.Empty(t, response)
		require.Nil(t, sections)
		require.Nil(t, secErr)
	})
}

func TestBus_SendParallelResponse2(t *testing.T) {
	bus := bus{}
	t.Run("Should return result sender", func(t *testing.T) {
		rs := bus.SendParallelResponse2(context.Background(), newSender(time.Millisecond))

		require.NotNil(t, rs)
	})
	t.Run("Should return nil because channel closed", func(t *testing.T) {
		s := newSender(time.Millisecond)
		close(s.c)

		rs := bus.SendParallelResponse2(context.Background(), s)

		require.Nil(t, rs)
	})
}

func TestSender_TryToSend(t *testing.T) {
	t.Run("Should be ok", func(t *testing.T) {
		s := newSender(ibus.DefaultTimeout)

		err := s.tryToSend(42)

		require.Nil(t, err)
	})
	t.Run("Should return error when channel closed", func(t *testing.T) {
		s := newSender(ibus.DefaultTimeout)
		close(s.c)

		err := s.tryToSend(42)

		require.Equal(t, "send on closed channel", err.Error())
	})
	t.Run("Should return error when client reads too long", func(t *testing.T) {
		s := newSender(time.Millisecond)

		_ = s.tryToSend(42)
		err := s.tryToSend(42)

		require.Equal(t, ibus.ErrNoConsumer, err)
	})
}

func TestCheckSender(t *testing.T) {
	t.Run("Should be ok", func(t *testing.T) {
		s := checkSender(new(senderImpl))

		require.True(t, s.used)
	})
	t.Run("Should panic on second try to use", func(t *testing.T) {
		s := checkSender(new(senderImpl))

		require.PanicsWithValue(t, "sender channel already used", func() { checkSender(s) })
	})
}

func TestResultSenderClosable_SendElement(t *testing.T) {
	t.Run("Should be ok", func(t *testing.T) {

	})
}
