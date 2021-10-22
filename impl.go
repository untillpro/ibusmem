/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package ibusmem

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	ibus "github.com/untillpro/airs-ibus"
)

type bus struct {
	requestHandler func(ctx context.Context, sender interface{}, request ibus.Request)
}

func (b *bus) SendRequest2(ctx context.Context, request ibus.Request, timeout time.Duration) (res ibus.Response, sections <-chan ibus.ISection, secError *error, err error) {
	defer func() {
		if e := recover(); e != nil {
			switch recoveryResult := e.(type) {
			case string:
				err = errors.New(recoveryResult)
			case error:
				err = recoveryResult
			}
		}
	}()
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
			err = ctx.Err()
			return
		case <-time.After(timeout):
			err = ibus.ErrTimeoutExpired
			return
		}
	}()
	b.requestHandler(ctx, s, request)
	wg.Wait()
	//Check ctx here again for case when ctx.Done and value received simultaneously
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return res, sections, secError, err
}

func (b *bus) SendResponse(_ context.Context, sender interface{}, response ibus.Response) {
	s := sender.(*channelSender)
	s.send(response)
}

func (b *bus) SendParallelResponse2(ctx context.Context, sender interface{}) (rsender ibus.IResultSenderClosable) {
	s := sender.(*channelSender)
	var err error
	rsender = &resultSenderClosable{
		sections: make(chan ibus.ISection),
		err:      &err,
		timeout:  s.timeout,
		ctx:      ctx,
	}
	s.send(rsender)
	return rsender
}

type channelSender struct {
	c       chan interface{}
	timeout time.Duration
}

func newSender(timeout time.Duration) *channelSender {
	return &channelSender{
		c:       make(chan interface{}, 1),
		timeout: timeout,
	}
}

func (s *channelSender) send(value interface{}) {
	s.c <- value
	close(s.c)
}

type resultSenderClosable struct {
	currentSection ibus.ISection
	sections       chan ibus.ISection
	elements       chan element
	err            *error
	timeout        time.Duration
	ctx            context.Context
}

func (s *resultSenderClosable) StartArraySection(sectionType string, path []string) {
	s.currentSection = arraySection{
		sectionType: sectionType,
		path:        path,
		elems:       s.updateElemsChannel(),
	}
}

func (s *resultSenderClosable) StartMapSection(sectionType string, path []string) {
	s.currentSection = mapSection{
		sectionType: sectionType,
		path:        path,
		elems:       s.updateElemsChannel(),
	}
}

func (s *resultSenderClosable) ObjectSection(sectionType string, path []string, element interface{}) (err error) {
	s.currentSection = &objectSection{
		sectionType: sectionType,
		path:        path,
		elements:    s.updateElemsChannel(),
	}
	return s.SendElement("", element)
}

func (s *resultSenderClosable) SendElement(name string, el interface{}) (err error) {
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
	err = s.tryToSendSection()
	if err != nil {
		return
	}
	element := element{
		name:  name,
		value: bb,
	}
	return s.tryToSendElement(element)
}

func (s *resultSenderClosable) Close(err error) {
	if err != nil {
		*s.err = err
	}
	close(s.sections)
	if s.elements != nil {
		close(s.elements)
	}
}

func (s *resultSenderClosable) updateElemsChannel() chan element {
	if s.elements != nil {
		close(s.elements)
	}
	s.elements = make(chan element)
	return s.elements
}

func (s *resultSenderClosable) tryToSendSection() (err error) {
	if s.currentSection != nil {
		select {
		case s.sections <- s.currentSection:
			s.currentSection = nil
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-time.After(s.timeout):
			return ibus.ErrNoConsumer
		}
	}
	return nil
}

func (s *resultSenderClosable) tryToSendElement(value element) (err error) {
	select {
	case s.elements <- value:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-time.After(s.timeout):
		return ibus.ErrNoConsumer
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
	sectionType     string
	path            []string
	elements        chan element
	elementReceived bool
}

func (s *objectSection) Type() string {
	return s.sectionType
}

func (s *objectSection) Path() []string {
	return s.path
}

func (s *objectSection) Value() []byte {
	if !s.elementReceived {
		s.elementReceived = true
		element := <-s.elements
		return element.value
	}
	return nil
}

type element struct {
	name  string
	value []byte
}
