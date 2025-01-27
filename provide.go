/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package ibusmem

import (
	"context"
	"time"

	ibus "github.com/untillpro/airs-ibus"
)

// requestCtx is already contained by sender but exposed also as a separate param because it is more useful in request handlers
func Provide(requestHandler func(requestCtx context.Context, sender ibus.ISender, request ibus.Request)) ibus.IBus {
	return provide(requestHandler, time.After, time.After, time.After)
}

func NewISender(bus ibus.IBus, sender interface{}) ibus.ISender {
	return &implISender{
		bus:    bus,
		sender: sender,
	}
}

func provide(requestHandler func(requestCtx context.Context, sender ibus.ISender, request ibus.Request),
	timerResponse func(time.Duration) <-chan time.Time,
	timerSection func(time.Duration) <-chan time.Time,
	timerElement func(time.Duration) <-chan time.Time,
) ibus.IBus {
	if requestHandler == nil {
		panic("request handler must be not nil")
	}
	return &bus{
		requestHandler: requestHandler,
		timerResponse:  timerResponse,
		timerSection:   timerSection,
		timerElement:   timerElement,
	}
}
