package proj

import (
	_ "bytes"
	_ "encoding/gob"
	_ "fmt"
	"math/rand"
)

type SkipListNode struct {
	Vrednost int
	Value    interface{}
	Body     []*SkipListNode
}

type SkipList struct {
	Head        *SkipListNode
	MaxLevel    int
	CurrentLvel int
}

func NewSkipListNode(vrednost int, value interface{}, level int) *SkipListNode {
	return &SkipListNode{
		Vrednost: vrednost,
		Value:    value,
		Body:     make([]*SkipListNode, level),
	}
}

func NewSkipList(maxLevel int) *SkipList {
	return &SkipList{
		Head:        NewSkipListNode(0, nil, maxLevel),
		MaxLevel:    maxLevel,
		CurrentLvel: 1,
	}
}

func (SL *SkipList) Search(key int) *SkipListNode {
	cur := SL.Head
	for i := SL.CurrentLvel - 1; i >= 0; i-- {
		for cur.Body[i] != nil && cur.Body[i].Vrednost <= key {
			if cur.Body[i].Vrednost == key {
				return cur.Body[i]
			}
			cur = cur.Body[i]
		}
	}
	return nil
}

func (SL *SkipList) Add(key int, value interface{}) *SkipListNode {
	cur := SL.Head
	temp = make([]*SkipListNod, SL.MaxLevel)

	for i := SL.CurrentLvel - 1; i >= 0; i-- {
		for cur.Body[i] != nil && key >= cur.Body[i].vrednost {
			cur = cur.body[i]
		}
		temp[i] = cur
	}
	cur = cur.body[0]

	if cur == nil || cur.Vrednost != key {
		level := 0
		for flip() == "HEAD" {
			level++
		}

		if level > SL.CurrentLvel {
			for i := SL.CurrentLvel; i < level; i++ {
				temp[i] = SL.Head
			}
			SL.level = level
		}
	}
}

// NE MOGU VISE OVO STA JE BRE OVO VISE.
//SRECNO!
func flip() String {
	if rand.Float64() < 0.5 {
		return "HEAD"
	}
	return "TAILS"
}
