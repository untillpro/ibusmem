/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package ibusmem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	ibus "github.com/untillpro/airs-ibus"
)

func TestProvide(t *testing.T) {
	t.Run("Should be ok", func(t *testing.T) {
		bus := Provide(func(ctx context.Context, sender interface{}, request ibus.Request) {})

		require.NotNil(t, bus)
	})
	t.Run("Should panic when request handler is nil", func(t *testing.T) {
		require.PanicsWithValue(t, "request handler must be not nil", func() {
			Provide(nil)
		})
	})
}
