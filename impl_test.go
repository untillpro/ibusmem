/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package ibusmem

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ibus "github.com/untillpro/airs-ibus"
)

const (
	testTimeout           = 50 * time.Millisecond
	quadrupledTestTimeout = 4 * testTimeout
)

func TestBasicUsage_Bus(t *testing.T) {
	require := require.New(t)
	t.Run("Should handle single response", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			bus.SendResponse(ctx, sender, ibus.Response{Data: []byte("hello world")})
		})

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)

		require.Equal("hello world", string(response.Data))
		require.Nil(sections)
		require.Nil(secErr)
		require.Nil(err)
	})
	t.Run("Should handle section response", func(t *testing.T) {
		testErr := errors.New("error from result sender")
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
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			rs := bus.SendParallelResponse2(ctx, sender)
			go func() {
				rs.StartArraySection("array", []string{"array-path"})
				require.Nil(rs.SendElement("", "element1"))
				require.Nil(rs.SendElement("", "element2"))
				rs.StartMapSection("map", []string{"map-path"})
				require.Nil(rs.SendElement("key1", "value1"))
				require.Nil(rs.SendElement("key2", "value2"))
				require.Nil(rs.ObjectSection("object", []string{"object-path"}, "value"))
				rs.Close(testErr)
			}()
		})

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)

		require.Nil(err)
		require.Empty(response)
		section := <-sections
		arraySection := section.(ibus.IArraySection)
		require.Equal("array", arraySection.Type())
		require.Equal([]string{"array-path"}, arraySection.Path())
		require.Equal(`"element1"`, arrayEntry(arraySection.Next()))
		require.Equal(`"element2"`, arrayEntry(arraySection.Next()))
		value, ok := arraySection.Next()
		require.Nil(value)
		require.False(ok)
		section = <-sections
		mapSection := section.(ibus.IMapSection)
		require.Equal("map", mapSection.Type())
		require.Equal([]string{"map-path"}, mapSection.Path())
		require.Equal(`key1:"value1"`, mapEntry(mapSection.Next()))
		require.Equal(`key2:"value2"`, mapEntry(mapSection.Next()))
		name, value, ok := mapSection.Next()
		require.Equal("", name)
		require.Nil(value)
		require.False(ok)
		section = <-sections
		objectSection := section.(ibus.IObjectSection)
		require.Equal("object", objectSection.Type())
		require.Equal([]string{"object-path"}, objectSection.Path())
		require.Equal(`"value"`, string(objectSection.Value()))
		require.True(closed(sections))
		require.ErrorIs(testErr, *secErr)
	})
	t.Run("Provide should panic on nil requestHandler provided", func(t *testing.T) {
		require.Panics(func() { Provide(nil) })
	})
}

func TestBus_SendRequest2(t *testing.T) {
	require := require.New(t)
	t.Run("Should return timeout error", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			time.Sleep(quadrupledTestTimeout)
			bus.SendResponse(ctx, sender, ibus.Response{Data: []byte("data")})
		})

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, testTimeout)

		require.Equal(ibus.ErrTimeoutExpired, err)
		require.Empty(response)
		require.Nil(sections)
		require.Nil(secErr)
	})
	t.Run("Should return error on ctx done", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			time.Sleep(quadrupledTestTimeout)
			bus.SendResponse(ctx, sender, ibus.Response{Data: []byte("data")})
		})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		response, sections, secErr, err := bus.SendRequest2(ctx, ibus.Request{}, testTimeout)

		require.Equal("context canceled", err.Error())
		require.Empty(response)
		require.Nil(sections)
		require.Nil(secErr)
	})
	t.Run("Should handle panic in request handler with message", func(t *testing.T) {
		bus := Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			panic("boom")
		})

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)

		require.Equal("boom", err.Error())
		require.Empty(response)
		require.Nil(sections)
		require.Nil(secErr)
	})
	t.Run("Should handle panic in request handler with error", func(t *testing.T) {
		testErr := errors.New("boom")
		bus := Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			panic(testErr)
		})

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)

		require.ErrorIs(err, testErr)
		require.Empty(response)
		require.Nil(sections)
		require.Nil(secErr)
	})
}

func TestBus_SendParallelResponse2(t *testing.T) {
	require := require.New(t)
	t.Run("Should panic on second sender usage", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			_ = bus.SendParallelResponse2(ctx, sender)

			require.PanicsWithError("send on closed channel", func() { _ = bus.SendParallelResponse2(ctx, sender) })
		})
		_, _, _, _ = bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)
	})
	t.Run("Should panic on wrong sender", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, _ interface{}, request ibus.Request) {
			require.Panics(func() { _ = bus.SendParallelResponse2(ctx, "wrong sender") })
		})
		_, _, _, _ = bus.SendRequest2(context.Background(), ibus.Request{}, testTimeout)
	})
}

func TestBus_SendResponse(t *testing.T) {
	require := require.New(t)
	ch := make(chan interface{})
	t.Run("Should panic on second sender usage", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			bus.SendResponse(ctx, sender, ibus.Response{})

			require.PanicsWithError("send on closed channel", func() { bus.SendResponse(ctx, sender, ibus.Response{}) })
			go func() { ch <- nil }()
		})
		resp, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)
		require.Nil(err)
		require.Nil(sections)
		require.Empty(resp)
		require.Nil(secErr)
		<-ch
	})
	t.Run("Should panic on wrong sender", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, _ interface{}, request ibus.Request) {
			require.Panics(func() { bus.SendResponse(ctx, "wrong sender", ibus.Response{}) })
		})
		resp, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, testTimeout)
		require.ErrorIs(ibus.ErrTimeoutExpired, err)
		require.Nil(sections)
		require.Empty(resp)
		require.Nil(secErr)
	})
}

func TestResultSenderClosable_StartArraySection(t *testing.T) {
	require := require.New(t)
	t.Run("Should return error when client reads section too long", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			rs := bus.SendParallelResponse2(ctx, sender)
			go func() {
				err := rs.ObjectSection("", nil, 42)

				require.ErrorIs(err, ibus.ErrNoConsumer)
				rs.Close(nil)
			}()
		})
		resp, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, testTimeout)
		require.Nil(err)
		require.NotNil(sections)
		require.Empty(resp)
		time.Sleep(quadrupledTestTimeout)
		_, ok := <-sections
		require.False(ok)
		require.Nil(*secErr)

	})
	t.Run("Should return error when ctx done on send section", func(t *testing.T) {
		var bus ibus.IBus
		ch := make(chan interface{})
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			rs := bus.SendParallelResponse2(ctx, sender)
			go func() {
				<-ch // wait for cancel
				err := rs.ObjectSection("", nil, 42)
				ch <- nil // signal ok to start read sections. That forces ctx.Done case fire at tryToSendSection
				require.Equal("context canceled", err.Error())
				rs.Close(nil)
			}()
		})
		ctx, cancel := context.WithCancel(context.Background())
		response, sections, secErr, err := bus.SendRequest2(ctx, ibus.Request{}, ibus.DefaultTimeout)
		require.Nil(err)
		require.Empty(response)
		require.NotNil(sections)
		cancel()
		ch <- nil // signal cancelled
		<-ch      // delay to read sections to make ctx.Done() branch at tryToSendSection fire.
		// note: section could be sent on ctx.Done() because cases order is undefined at tryToSendSection. But ObjectSection() will return error in any case
		for range sections {
		}
		require.Nil(*secErr)
	})
}

func TestResultSenderClosable_SendElement(t *testing.T) {
	require := require.New(t)
	t.Run("Should not send to bus when element is nil", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			rs := bus.SendParallelResponse2(ctx, sender)
			go func() {
				require.Nil(rs.ObjectSection("", nil, nil))
				rs.Close(nil)
			}()
		})

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)

		require.Nil(err)
		require.Empty(response)
		_, ok := <-sections
		require.False(ok)
		require.Nil(*secErr)
	})
	t.Run("Should panic when section is not started", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			rs := bus.SendParallelResponse2(ctx, sender)
			go func() {
				require.PanicsWithValue("section is not started", func() {
					_ = rs.SendElement("", []byte("hello world"))
				})
				rs.Close(nil)
			}()
		})
		resp, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, testTimeout)
		require.Nil(err)
		_, ok := <-sections
		require.False(ok)
		require.Empty(resp)
		require.Nil(*secErr)
	})
	t.Run("Should return error when element is invalid", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			rs := bus.SendParallelResponse2(ctx, sender)
			go func() {
				require.NotNil(rs.ObjectSection("", nil, func() {}))
				rs.Close(nil)
			}()
		})
		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)
		require.Nil(err)
		require.Empty(response)
		require.Nil(<-sections)
		require.Nil(*secErr)
	})
	t.Run("Should panic after close", func(t *testing.T) {
		ch := make(chan interface{})
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			rs := bus.SendParallelResponse2(ctx, sender)
			go func() {
				require.Nil(rs.ObjectSection("", nil, 42))
				rs.Close(nil)

				require.Panics(func() {
					_ = rs.ObjectSection("", nil, []byte("hello world"))
				})
				ch <- nil
			}()
		})
		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)
		require.Nil(err)
		require.Empty(response)
		require.NotNil((<-sections).(ibus.IObjectSection).Value())
		<-ch
		_, ok := <-sections
		require.False(ok)
		require.Nil(*secErr)
	})
	t.Run("Should accept object", func(t *testing.T) {
		type article struct {
			ID   int64  `json:"id"`
			Name string `json:"name"`
		}
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			rs := bus.SendParallelResponse2(ctx, sender)
			go func() {
				require.Nil(rs.ObjectSection("", nil, article{ID: 100, Name: "Cola"}))
				rs.Close(nil)
			}()
		})
		a := article{}

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)

		require.Nil(err)
		require.Empty(response)
		require.Nil(json.Unmarshal((<-sections).(ibus.IObjectSection).Value(), &a))
		require.Equal(int64(100), a.ID)
		require.Equal("Cola", a.Name)
		_, ok := <-sections
		require.False(ok)
		require.Nil(*secErr)
	})
	t.Run("Should accept JSON", func(t *testing.T) {
		type point struct {
			X int64 `json:"x"`
			Y int64 `json:"y"`
		}
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			rs := bus.SendParallelResponse2(ctx, sender)
			go func() {
				require.Nil(rs.ObjectSection("", nil, []byte(`{"x":52,"y":89}`)))
				rs.Close(nil)
			}()
		})
		p := point{}

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)
		require.Nil(err)
		require.Empty(response)
		require.Nil(json.Unmarshal((<-sections).(ibus.IObjectSection).Value(), &p))
		require.Equal(int64(52), p.X)
		require.Equal(int64(89), p.Y)
		_, ok := <-sections
		require.False(ok)
		require.Nil(*secErr)
	})
	t.Run("Should return error when client reads element too long", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			rs := bus.SendParallelResponse2(ctx, sender)
			go func() {
				rs.StartArraySection("", nil)
				_ = rs.SendElement("", 0)
				err := rs.SendElement("", 1)

				require.ErrorIs(err, ibus.ErrNoConsumer)
				rs.Close(nil)
			}()
		})

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, testTimeout)

		require.Nil(err)
		require.Empty(response)
		array := (<-sections).(ibus.IArraySection)
		val, ok := array.Next()
		require.True(ok)
		require.Equal([]byte("0"), val)
		_, ok = <-sections
		require.False(ok)
		require.Nil(*secErr)
	})
	t.Run("Should return error when ctx done on send element", func(t *testing.T) {
		var bus ibus.IBus
		ch := make(chan interface{})
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			rs := bus.SendParallelResponse2(ctx, sender)
			go func() {
				rs.StartArraySection("", nil)
				require.Nil(rs.SendElement("", 0))
				<-ch // wait for cancel
				err := rs.SendElement("", 1)
				ch <- nil // signal ok to read next element. That forces ctx.Done() case fire at tryToSendElement

				require.Equal("context canceled", err.Error())
				rs.Close(nil)
			}()
		})
		ctx, cancel := context.WithCancel(context.Background())
		response, sections, secErr, err := bus.SendRequest2(ctx, ibus.Request{}, ibus.DefaultTimeout)
		require.Nil(err)
		require.Empty(response)
		array := (<-sections).(ibus.IArraySection)
		val, ok := array.Next()
		require.True(ok)
		require.Equal([]byte("0"), val)
		cancel()
		ch <- nil           // signal cancelled
		<-ch                // wait for ok to read next element to force ctx.Done case fire at tryToSendElement
		_, _ = array.Next() // note: element could be sent on ctx.Done() because cases order is undefined at tryToSendElement. But SendElement() will return error in any case
		_, ok = <-sections
		require.False(ok)
		require.Nil(*secErr)
	})
}

func TestObjectSection_Value(t *testing.T) {
	var bus ibus.IBus
	require := require.New(t)
	bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := bus.SendParallelResponse2(ctx, sender)
		go func() {
			require.Nil(rs.ObjectSection("", nil, []byte("bb")))
			rs.Close(nil)
		}()
	})

	response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)

	object := (<-sections).(ibus.IObjectSection)
	require.Nil(err)
	require.Empty(response)
	require.Equal([]byte("bb"), object.Value())
	require.Nil(object.Value())
	require.Nil(*secErr)
}
