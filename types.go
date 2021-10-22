package ibusmem

import (
	"context"
	"time"

	ibus "github.com/untillpro/airs-ibus"
)

type bus struct {
	requestHandler func(ctx context.Context, sender interface{}, request ibus.Request)
}

type channelSender struct {
	c       chan interface{}
	timeout time.Duration
}

type resultSenderClosable struct {
	currentSection ibus.ISection
	sections       chan ibus.ISection
	elements       chan element
	err            *error
	timeout        time.Duration
	ctx            context.Context
}

type arraySection struct {
	sectionType string
	path        []string
	elems       chan element
}

type mapSection struct {
	sectionType string
	path        []string
	elems       chan element
}

type objectSection struct {
	sectionType     string
	path            []string
	elements        chan element
	elementReceived bool
}

type element struct {
	name  string
	value []byte
}
