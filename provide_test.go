/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package ibusmem

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProvide(t *testing.T) {
	require.PanicsWithValue(t, "request handler must be not nil", func() {
		Provide(nil)
	})
}
