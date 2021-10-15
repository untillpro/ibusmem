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
	c := make(chan interface{}, 1)
	wg.Add(1)
	go func() {
		defer close(c)
		for {
			select {
			case result := <-c:
				switch result := result.(type) {
				case ibus.Response:
					res = result
				case *resultSenderClosable:
					rsender := result
					sections = rsender.c
					secError = rsender.err
				}
				wg.Done()
				return
			case <-time.After(timeout):
				err = ErrTimeout
				wg.Done()
				return
			}
		}
	}()
	b.requestHandler(ctx, c, request)
	wg.Wait()
	return
}

func (b *bus) SendResponse(_ context.Context, sender interface{}, response ibus.Response) {
	sender.(chan interface{}) <- response
}

func (b *bus) SendParallelResponse2(_ context.Context, sender interface{}) (rsender ibus.IResultSenderClosable) {
	c := sender.(chan interface{})
	rsender = &resultSenderClosable{c: make(chan ibus.ISection, 1)}
	c <- rsender
	return rsender
}

type resultSenderClosable struct {
	c     chan ibus.ISection
	elems chan elem
	err   *error
}

func (s *resultSenderClosable) StartArraySection(sectionType string, path []string) {
	s.c <- arraySection{
		sectionType: sectionType,
		path:        path,
		elems:       s.updateElemsChannel(),
	}
}

func (s *resultSenderClosable) StartMapSection(sectionType string, path []string) {
	s.c <- mapSection{
		sectionType: sectionType,
		path:        path,
		elems:       s.updateElemsChannel(),
	}
}

func (s *resultSenderClosable) ObjectSection(sectionType string, path []string, element interface{}) (err error) {
	s.c <- objectSection{
		sectionType: sectionType,
		path:        path,
		elems:       s.updateElemsChannel(),
	}
	return s.SendElement("", element)
}

func (s resultSenderClosable) SendElement(name string, element interface{}) (err error) {
	if element == nil {
		return nil
	}
	if s.elems == nil {
		return ErrSectionIsNotStarted
	}
	bb, ok := element.([]byte)
	if !ok {
		if bb, err = json.Marshal(element); err != nil {
			return
		}
	}
	s.elems <- elem{
		name:  name,
		value: bb,
	}
	return
}

func (s *resultSenderClosable) Close(err error) {
	close(s.c)
	if err != nil {
		s.err = &err
	}
}

func (s *resultSenderClosable) updateElemsChannel() chan elem {
	if s.elems != nil {
		close(s.elems)
	}
	s.elems = make(chan elem, 1)
	return s.elems
}

type arraySection struct {
	sectionType string
	path        []string
	elems       chan elem
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
	elems       chan elem
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
	elems       chan elem
}

func (s objectSection) Type() string {
	return s.sectionType
}

func (s objectSection) Path() []string {
	return s.path
}

func (s objectSection) Value() []byte {
	e := <-s.elems
	return e.value
}

type elem struct {
	name  string
	value []byte
}
