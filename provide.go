/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package ibusmem

import (
	"context"

	ibus "github.com/untillpro/airs-ibus"
)

func Provide(requestHandler func(ctx context.Context, sender interface{}, request ibus.Request)) ibus.IBus {
	return &bus{requestHandler}
}
