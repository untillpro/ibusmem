/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package ibusmem

import (
	"time"

	ibus "github.com/untillpro/airs-ibus"
)

func Provide(requestHandler func(sender interface{}, request ibus.Request)) ibus.IBus {
	return provide(requestHandler, time.After, time.After, time.After)
}

func provide(requestHandler func(sender interface{}, request ibus.Request),
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
