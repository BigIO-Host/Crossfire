// Copyright 2017 BigIO.host. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"sort"
)

type byString []string

func (s byString) Len() int           { return len(s) }
func (s byString) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s byString) Less(i, j int) bool { return s[i] < s[j] }

type byStringLength []string

func (s byStringLength) Len() int           { return len(s) }
func (s byStringLength) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s byStringLength) Less(i, j int) bool { return len(s[i]) < len(s[j]) }

/* sort map[string]float64 by value */
type pairF64 struct {
	Key   string
	Value float64
}
type pairF64List []pairF64

func (p pairF64List) Len() int           { return len(p) }
func (p pairF64List) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p pairF64List) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func rankByFloatValue(m map[string]float64) pairF64List {
	pl := make(pairF64List, len(m))
	i := 0
	for k, v := range m {
		pl[i] = pairF64{k, v}
		i++
	}
	sort.Sort(sort.Reverse(pl))
	return pl
}

/* sort map[string]int64 by value */
type pairI64 struct {
	Key   string
	Value int64
}
type pairI64List []pairI64

func (p pairI64List) Len() int           { return len(p) }
func (p pairI64List) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p pairI64List) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func rankByInt64Value(m map[string]int64) pairI64List {
	pl := make(pairI64List, len(m))
	i := 0
	for k, v := range m {
		pl[i] = pairI64{k, v}
		i++
	}
	sort.Sort(sort.Reverse(pl))
	return pl
}

const validEventTypeRegx = "^(event|atr|goal|act)$"
