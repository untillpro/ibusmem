/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package ibusmem

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	ibus "github.com/untillpro/airs-ibus"
)

type bus struct {
	requestHandler func(ctx context.Context, sender interface{}, request ibus.Request)
}

func (b *bus) SendRequest2(ctx context.Context, request ibus.Request, timeout time.Duration) (res ibus.Response, sections <-chan ibus.ISection, secError *error, err error) {
	wg := sync.WaitGroup{}
	s := newSender(timeout)
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case result := <-s.c:
			switch result := result.(type) {
			case ibus.Response:
				res = result
			case *resultSenderClosable:
				rsender := result
				sections = rsender.sections
				secError = rsender.err
			}
			return
		case <-ctx.Done():
			return
		case <-time.After(timeout):
			err = ibus.ErrTimeoutExpired
			return
		}
	}()
	b.requestHandler(ctx, s, request)
	wg.Wait()
	return
}

func (b *bus) SendResponse(_ context.Context, sender interface{}, response ibus.Response) {
	s := checkSender(sender)
	s.c <- response
}

func (b *bus) SendParallelResponse2(ctx context.Context, sender interface{}) (rsender ibus.IResultSenderClosable) {
	s := checkSender(sender)
	var err error
	rsender = &resultSenderClosable{
		sections: make(chan ibus.ISection, 1),
		err:      &err,
		timeout:  s.timeout,
		ctx:      ctx,
	}
	s.c <- rsender
	return rsender
}

func checkSender(sender interface{}) *senderImpl {
	s := sender.(*senderImpl)
	if s.used {
		panic("sender channel already used")
	}
	s.used = true
	return s
}

type senderImpl struct {
	c       chan interface{}
	used    bool
	timeout time.Duration
}

func newSender(timeout time.Duration) *senderImpl {
	return &senderImpl{
		c:       make(chan interface{}, 1),
		used:    false,
		timeout: timeout,
	}
}

type resultSenderClosable struct {
	sections    chan ibus.ISection
	elements    chan element
	err         *error
	timeout     time.Duration
	internalErr error
	ctx         context.Context
}

func (s *resultSenderClosable) StartArraySection(sectionType string, path []string) {
	s.tryToSendSection(arraySection{
		sectionType: sectionType,
		path:        path,
		elems:       s.updateElemsChannel(),
	})
}

func (s *resultSenderClosable) StartMapSection(sectionType string, path []string) {
	s.tryToSendSection(mapSection{
		sectionType: sectionType,
		path:        path,
		elems:       s.updateElemsChannel(),
	})
}

func (s *resultSenderClosable) ObjectSection(sectionType string, path []string, element interface{}) (err error) {
	s.tryToSendSection(objectSection{
		sectionType: sectionType,
		path:        path,
		elements:    s.updateElemsChannel(),
	})
	return s.SendElement("", element)
}

func (s resultSenderClosable) SendElement(name string, el interface{}) (err error) {
	if s.internalErr != nil {
		return s.internalErr
	}
	if el == nil {
		return nil
	}
	if s.elements == nil {
		panic("section is not started")
	}
	bb, ok := el.([]byte)
	if !ok {
		if bb, err = json.Marshal(el); err != nil {
			return
		}
	}
	element := element{
		name:  name,
		value: bb,
	}
	return s.tryToSendElement(element)
}

func (s *resultSenderClosable) Close(err error) {
	close(s.sections)
	close(s.elements)
	if err != nil {
		*s.err = err
	}
}

func (s *resultSenderClosable) updateElemsChannel() chan element {
	if s.elements != nil {
		close(s.elements)
	}
	s.elements = make(chan element, 1)
	return s.elements
}

func (s *resultSenderClosable) tryToSendSection(value ibus.ISection) {
	select {
	case s.sections <- value:
	case <-s.ctx.Done():
		s.internalErr = s.ctx.Err()
	case <-time.After(s.timeout):
		s.internalErr = ibus.ErrNoConsumer
	}
}

func (s *resultSenderClosable) tryToSendElement(value element) (err error) {
	select {
	case s.elements <- value:
		return nil
	case <-s.ctx.Done():
		s.internalErr = s.ctx.Err()
		return s.internalErr
	case <-time.After(s.timeout):
		s.internalErr = ibus.ErrNoConsumer
		return s.internalErr
	}
}

type arraySection struct {
	sectionType string
	path        []string
	elems       chan element
}

func (s arraySection) Type() string {
	return s.sectionType
}

func (s arraySection) Path() []string {
	return s.path
}

func (s arraySection) Next() (value []byte, ok bool) {
	for e := range s.elems {
		return e.value, true
	}
	return nil, false
}

type mapSection struct {
	sectionType string
	path        []string
	elems       chan element
}

func (s mapSection) Type() string {
	return s.sectionType
}

func (s mapSection) Path() []string {
	return s.path
}

func (s mapSection) Next() (name string, value []byte, ok bool) {
	for e := range s.elems {
		return e.name, e.value, true
	}
	return "", nil, false
}

type objectSection struct {
	sectionType string
	path        []string
	elements    chan element
	element     *element
}

func (s objectSection) Type() string {
	return s.sectionType
}

func (s objectSection) Path() []string {
	return s.path
}

func (s *objectSection) Value() []byte {
	if s.element == nil {
		e := <-s.elements
		s.element = &e
	}
	return s.element.value
}

type element struct {
	name  string
	value []byte
}
