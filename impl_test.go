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

func TestBasicUsage_Bus(t *testing.T) {
	t.Run("Should handle single response", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			bus.SendResponse(ctx, sender, ibus.Response{Data: []byte("hello world")})
		})

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)

		require.Equal(t, "hello world", string(response.Data))
		require.Nil(t, sections)
		require.Nil(t, secErr)
		require.Nil(t, err)
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
				require.Nil(t, rs.SendElement("", "element1"))
				require.Nil(t, rs.SendElement("", "element2"))
				rs.StartMapSection("map", []string{"map-path"})
				require.Nil(t, rs.SendElement("key1", "value1"))
				require.Nil(t, rs.SendElement("key2", "value2"))
				require.Nil(t, rs.ObjectSection("object", []string{"object-path"}, "value"))
				rs.Close(testErr)
			}()
		})

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)

		require.Nil(t, err)
		require.Empty(t, response)
		section := <-sections
		arraySection := section.(ibus.IArraySection)
		require.Equal(t, "array", arraySection.Type())
		require.Equal(t, []string{"array-path"}, arraySection.Path())
		require.Equal(t, `"element1"`, arrayEntry(arraySection.Next()))
		require.Equal(t, `"element2"`, arrayEntry(arraySection.Next()))
		value, ok := arraySection.Next()
		require.Nil(t, value)
		require.False(t, ok)
		section = <-sections
		mapSection := section.(ibus.IMapSection)
		require.Equal(t, "map", mapSection.Type())
		require.Equal(t, []string{"map-path"}, mapSection.Path())
		require.Equal(t, `key1:"value1"`, mapEntry(mapSection.Next()))
		require.Equal(t, `key2:"value2"`, mapEntry(mapSection.Next()))
		name, value, ok := mapSection.Next()
		require.Equal(t, "", name)
		require.Nil(t, value)
		require.False(t, ok)
		section = <-sections
		objectSection := section.(ibus.IObjectSection)
		require.Equal(t, "object", objectSection.Type())
		require.Equal(t, []string{"object-path"}, objectSection.Path())
		require.Equal(t, `"value"`, string(objectSection.Value()))
		require.True(t, closed(sections))
		require.ErrorIs(t, testErr, *secErr)
	})
}

func TestBus_SendRequest2(t *testing.T) {
	t.Run("Should return timeout error", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			time.Sleep(50 * time.Millisecond)
			bus.SendResponse(ctx, sender, ibus.Response{Data: []byte("data")})
		})

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, 10*time.Millisecond)

		require.Equal(t, ibus.ErrTimeoutExpired, err)
		require.Empty(t, response)
		require.Nil(t, sections)
		require.Nil(t, secErr)
	})
	t.Run("Should return error on ctx done", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			time.Sleep(10 * time.Millisecond)
			bus.SendResponse(ctx, sender, ibus.Response{Data: []byte("data")})
		})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		response, sections, secErr, err := bus.SendRequest2(ctx, ibus.Request{}, 5*time.Millisecond)

		require.Equal(t, "context canceled", err.Error())
		require.Empty(t, response)
		require.Nil(t, sections)
		require.Nil(t, secErr)
	})
	t.Run("Should handle panic in request handler with message", func(t *testing.T) {
		bus := Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			panic("boom")
		})

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)

		require.Equal(t, "boom", err.Error())
		require.Empty(t, response)
		require.Nil(t, sections)
		require.Nil(t, secErr)
	})
	t.Run("Should handle panic in request handler with error", func(t *testing.T) {
		testErr := errors.New("boom")
		bus := Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			panic(testErr)
		})

		response, sections, secErr, err := bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)

		require.ErrorIs(t, err, testErr)
		require.Empty(t, response)
		require.Nil(t, sections)
		require.Nil(t, secErr)
	})
}

func TestBus_SendParallelResponse2(t *testing.T) {
	t.Run("Should panic on second sender usage", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			_ = bus.SendParallelResponse2(ctx, sender)

			require.PanicsWithError(t, "send on closed channel", func() { _ = bus.SendParallelResponse2(ctx, sender) })
		})
		_, _, _, _ = bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)
	})
	t.Run("Should panic on wrong sender", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, _ interface{}, request ibus.Request) {
			require.Panics(t, func() { _ = bus.SendParallelResponse2(ctx, "wrong sender") })
		})
		_, _, _, _ = bus.SendRequest2(context.Background(), ibus.Request{}, 5*time.Millisecond)
	})
}

func TestBus_SendResponse(t *testing.T) {
	t.Run("Should panic on second sender usage", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {
			bus.SendResponse(ctx, sender, ibus.Response{})

			require.PanicsWithError(t, "send on closed channel", func() { bus.SendResponse(ctx, sender, ibus.Response{}) })
		})
		_, _, _, _ = bus.SendRequest2(context.Background(), ibus.Request{}, ibus.DefaultTimeout)
	})
	t.Run("Should panic on wrong sender", func(t *testing.T) {
		var bus ibus.IBus
		bus = Provide(func(ctx context.Context, _ interface{}, request ibus.Request) {
			require.Panics(t, func() { bus.SendResponse(ctx, "wrong sender", ibus.Response{}) })
		})
		_, _, _, _ = bus.SendRequest2(context.Background(), ibus.Request{}, 5*time.Millisecond)
	})
}

func TestResultSenderClosable_SendElement(t *testing.T) {
	t.Run("Should return error when internal error is not nil", func(t *testing.T) {
		rs := resultSenderClosable{
			elements:    make(chan element),
			internalErr: ibus.ErrNoConsumer,
		}

		err := rs.SendElement("", 42)

		require.ErrorIs(t, err, ibus.ErrNoConsumer)
	})
	t.Run("Should not send to bus when element is nil", func(t *testing.T) {
		rs := resultSenderClosable{
			elements: make(chan element, 1),
			ctx:      context.Background(),
		}

		err := rs.SendElement("", nil)

		require.Nil(t, err)
		require.Empty(t, rs.elements)
	})
	t.Run("Should panic when section is not started", func(t *testing.T) {
		rs := resultSenderClosable{}

		require.PanicsWithValue(t, "section is not started", func() {
			_ = rs.SendElement("", []byte("hello world"))
		})
	})
	t.Run("Should return error when element is invalid", func(t *testing.T) {
		rs := resultSenderClosable{elements: make(chan element, 1)}

		err := rs.SendElement("", func() {})

		require.NotNil(t, err)
	})
	t.Run("Should return error when client reads too long", func(t *testing.T) {
		rs := resultSenderClosable{
			sections: make(chan ibus.ISection, 1),
			timeout:  time.Millisecond,
			ctx:      context.Background(),
		}
		rs.StartArraySection("", nil)
		require.Nil(t, rs.SendElement("", element{}))

		err := rs.SendElement("", element{})

		require.ErrorIs(t, err, ibus.ErrNoConsumer)
		require.ErrorIs(t, rs.internalErr, ibus.ErrNoConsumer)
	})
	t.Run("Should return error when ctx done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		rs := resultSenderClosable{
			sections: make(chan ibus.ISection, 1),
			timeout:  ibus.DefaultTimeout,
			ctx:      ctx,
		}
		rs.StartArraySection("", nil)
		cancel()

		_ = rs.SendElement("", element{})
		err := rs.SendElement("", element{})

		require.Equal(t, "context canceled", err.Error())
		require.Equal(t, "context canceled", rs.internalErr.Error())
	})
	t.Run("Should panic after close", func(t *testing.T) {
		rs := resultSenderClosable{
			sections: make(chan ibus.ISection, 1),
			timeout:  ibus.DefaultTimeout,
			ctx:      context.Background(),
		}
		rs.StartArraySection("", nil)
		rs.Close(nil)

		require.PanicsWithError(t, "send on closed channel", func() {
			_ = rs.SendElement("", []byte("hello world"))
		})
	})
	t.Run("Should accept object", func(t *testing.T) {
		type article struct {
			ID   int64  `json:"id"`
			Name string `json:"name"`
		}
		rs := resultSenderClosable{
			sections: make(chan ibus.ISection, 1),
			timeout:  ibus.DefaultTimeout,
			ctx:      context.Background(),
		}
		a := article{}

		require.Nil(t, rs.ObjectSection("", nil, article{ID: 100, Name: "Cola"}))
		rs.Close(nil)

		require.Nil(t, json.Unmarshal((<-rs.sections).(ibus.IObjectSection).Value(), &a))
		require.Equal(t, int64(100), a.ID)
		require.Equal(t, "Cola", a.Name)
	})
	t.Run("Should accept JSON", func(t *testing.T) {
		type point struct {
			X int64 `json:"x"`
			Y int64 `json:"y"`
		}
		rs := resultSenderClosable{
			sections: make(chan ibus.ISection, 1),
			timeout:  ibus.DefaultTimeout,
			ctx:      context.Background(),
		}
		p := point{}

		require.Nil(t, rs.ObjectSection("", nil, []byte(`{"x":52,"y":89}`)))
		rs.Close(nil)

		require.Nil(t, json.Unmarshal((<-rs.sections).(ibus.IObjectSection).Value(), &p))
		require.Equal(t, int64(52), p.X)
		require.Equal(t, int64(89), p.Y)
	})
	t.Run("Should return error when client reads section too long", func(t *testing.T) {
		rs := resultSenderClosable{
			sections: make(chan ibus.ISection),
			timeout:  time.Millisecond,
			ctx:      context.Background(),
		}

		rs.StartArraySection("", nil)
		err := rs.SendElement("", 42)

		require.ErrorIs(t, err, ibus.ErrNoConsumer)
		require.ErrorIs(t, rs.internalErr, ibus.ErrNoConsumer)
	})
	t.Run("Should return error when ctx done on send section", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		rs := resultSenderClosable{
			sections: make(chan ibus.ISection),
			timeout:  ibus.DefaultTimeout,
			ctx:      ctx,
		}
		cancel()

		rs.StartArraySection("", nil)
		err := rs.SendElement("", 42)

		require.Equal(t, "context canceled", rs.internalErr.Error())
		require.Equal(t, "context canceled", err.Error())
	})
}

func TestResultSenderClosable_StartArraySection(t *testing.T) {
	t.Run("Should be ok", func(t *testing.T) {
		rs := resultSenderClosable{
			sections: make(chan ibus.ISection, 1),
			timeout:  ibus.DefaultTimeout,
			ctx:      context.Background(),
		}

		rs.StartArraySection("", nil)

		require.Nil(t, rs.internalErr)
	})
	t.Run("Should panic after close", func(t *testing.T) {
		rs := resultSenderClosable{
			sections: make(chan ibus.ISection),
			timeout:  ibus.DefaultTimeout,
			ctx:      context.Background(),
		}

		rs.Close(nil)

		require.PanicsWithError(t, "send on closed channel", func() {
			rs.StartArraySection("", nil)
			_ = rs.SendElement("", 42)
		})
	})
}

func TestObjectSection_Value(t *testing.T) {
	section := objectSection{
		elements:        make(chan element, 1),
		elementReceived: false,
	}
	section.elements <- element{value: []byte("bb")}

	require.Equal(t, []byte("bb"), section.Value())
	require.Nil(t, section.Value())
}
