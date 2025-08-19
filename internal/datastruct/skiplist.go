package datastruct

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"

	obj "github.com/sunminx/RDB/internal/object"
)

// Skiplist is a collection of multiple ordered linked lists.
// The higher the level, the sparser the linked list becomes. The lowest-level linked-
// list contains all the elements.
// Node in a skiplist support backtracking, jumping to the lower-level linked list.
// Fast query is the most significant advantage of a skiplist. It uses a higher-level
// linked list to quickly approach the target element, and a lower-level linked list to
// find the target element.
type Skiplist struct {
	header, tail *skiplistNode
	level        int
	length       uint64
	compare      Compare
}

func (s *Skiplist) Length() uint64 { return s.length }
func (s *Skiplist) Level() int     { return s.level }

// Compare compares e1 and e2. The result will be 0 if e1 == e2,
// -1 if e1 < e2, and +1 if e1 > e2.
type Compare func(e1, e2 any) int

func BytesCompare(e1, e2 any) int {
	r1, r2 := e1.([]byte), e2.([]byte)
	return bytes.Compare(r1, r2)
}

func StrCompare(e1, e2 any) int {
	r1, r2 := e1.(string), e2.(string)
	return strings.Compare(r1, r2)
}

// ZSKIPLIST_MAXLEVEL defines the max level of skiplist.
const ZSKIPLIST_MAXLEVEL = 64

func NewSkiplist(compare Compare) *Skiplist {
	zsl := new(Skiplist)
	zsl.header = createSkiplistNode(ZSKIPLIST_MAXLEVEL, 0, nil)
	for i := 0; i < ZSKIPLIST_MAXLEVEL; i++ {
		zsl.header.levels[i].forward = nil
		zsl.header.levels[i].span = 0
	}
	zsl.level = 1
	zsl.length = 0
	zsl.compare = compare
	return zsl
}

// Insert insert an new node.
func (s *Skiplist) Insert(score float64, ele any) bool {
	if ele == nil {
		return false
	}
	update, rank := s.findPosByScoreEle(score, ele, false)
	return s.insertNode(score, ele, update, rank) != nil
}

func (s *Skiplist) insertNode(score float64, ele any,
	update []*skiplistNode, rank []uint64) *skiplistNode {
	level := randomLevel()
	if level > s.level {
		for i := level - 1; i >= s.level; i-- {
			rank[i] = 0
			update[i] = s.header
			update[i].levels[i].span = s.length
		}
		s.level = level
	}
	x := createSkiplistNode(level, score, ele)
	for i := 0; i < level; i++ {
		x.levels[i].forward = update[i].levels[i].forward
		update[i].levels[i].forward = x
		x.levels[i].span = update[i].levels[i].span - (rank[0] - rank[i])
		update[i].levels[i].span = (rank[0] - rank[i]) + 1
	}
	for i := level; i < s.level; i++ {
		update[i].levels[i].span += 1
	}
	if update[0] != s.header {
		x.backward = update[0]
	} else {
		x.backward = nil
	}
	if x.levels[0].forward != nil {
		x.levels[0].forward.backward = x
	} else {
		s.tail = x
	}
	s.length++
	return x
}

// ZSKIPLIST_P defines the threshold to add level.
var ZSKIPLIST_P = uint64(math.Round(0.25 * 0xFFFF))

// randomLevel returns a random level for the new skiplist node that we going to create.
func randomLevel() int {
	level := 1
	for rand.Uint64()&0xFFFF < ZSKIPLIST_P {
		level++
	}
	if level > ZSKIPLIST_MAXLEVEL {
		return ZSKIPLIST_MAXLEVEL
	}
	return level
}

// Delete delete a node indicated by both score and ele.
func (s *Skiplist) Delete(score float64, ele any) bool {
	update, _ := s.findPosByScoreEle(score, ele, false)
	return s.deleteNode(score, ele, update) != nil
}

func (s *Skiplist) deleteNode(score float64, ele any,
	update []*skiplistNode) *skiplistNode {
	x := update[0].levels[0].forward
	if x != nil && x.score == score && s.compare(x.ele, ele) == 0 {
		for i := 0; i < s.level; i++ {
			if update[i].levels[i].forward == x {
				update[i].levels[i].span += (x.levels[i].span - 1)
				update[i].levels[i].forward = x.levels[i].forward
			} else {
				update[i].levels[i].span -= 1
			}
		}
		s.length--
		return x
	}
	// Not found.
	return nil
}

// Update updates the score of an node. It is needed to ensure
// the orderly of skiplist after update operation.
func (s *Skiplist) Update(score float64, ele any, newscore float64) *skiplistNode {
	update, _ := s.findPosByScoreEle(score, ele, false)
	x := update[0].levels[0].forward
	if x == nil || x.score != score || s.compare(x.ele, ele) != 0 {
		return nil
	}
	if (x.levels[0].forward == nil || x.levels[0].forward.score > newscore) &&
		(x.backward == nil || x.backward.score < newscore) {
		x.score = newscore
		return x
	}
	x = s.deleteNode(score, ele, update)

	// Due to the update of score, the position of the element in the skiplist
	// will change. So the update needs to be re-searched.
	_ = s.Insert(newscore, ele)
	return x
}

type aux struct {
	score     float64
	ele       any
	ex        bool
	rank      uint64
	traversed uint64
	level     int
}

// cmp compares node and argv to decide if forwards in skiplist.
type cmp func(node *skiplistNode, aux aux) bool

func (s *Skiplist) findPosByScoreEle(score float64, ele any,
	ex bool) ([]*skiplistNode, []uint64) {
	aux := aux{score: score, ele: ele, ex: ex}
	return s.findPos(s.cmpByScoreEle, aux)
}

func (s *Skiplist) cmpByScoreEle(node *skiplistNode, aux aux) bool {
	next := node.levels[aux.level].forward
	return next.score < aux.score ||
		(next.score == aux.score && s.compare(next.ele, aux.ele) < 0)
}

func (s *Skiplist) findPosByScore(score float64, ex bool) ([]*skiplistNode, []uint64) {
	aux := aux{score: score, ex: ex}
	return s.findPos(s.cmpByScore, aux)
}

func (s *Skiplist) cmpByScore(node *skiplistNode, aux aux) bool {
	next := node.levels[aux.level].forward
	if aux.ex {
		return next.score <= aux.score
	}
	return next.score < aux.score
}

func (s *Skiplist) findPosByRank(rank uint64, ex bool) ([]*skiplistNode, []uint64) {
	aux := aux{rank: rank, ex: ex}
	return s.findPos(s.cmpByRank, aux)
}

func (s *Skiplist) cmpByRank(node *skiplistNode, aux aux) bool {
	return aux.traversed+node.levels[aux.level].span < aux.rank
}

func (s *Skiplist) findPos(cmp cmp, aux aux) ([]*skiplistNode, []uint64) {
	var (
		traversed = uint64(0)
		update    = make([]*skiplistNode, ZSKIPLIST_MAXLEVEL)
		rank      = make([]uint64, ZSKIPLIST_MAXLEVEL)
	)

	x := s.header
	for i := s.level - 1; i >= 0; i-- {
		if i == s.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		aux.level = i
		for x.levels[i].forward != nil && cmp(x, aux) {
			rank[i] += x.levels[i].span
			traversed += x.levels[i].span
			aux.traversed = traversed
			x = x.levels[i].forward
		}
		update[i] = x
	}
	return update, rank
}

// GetRank get rank for an element by both score and ele.
func (s *Skiplist) GetRank(score float64, ele any) uint64 {
	rank := uint64(0)
	x := s.header
	for i := s.level - 1; i >= 0; i-- {
		for x.levels[i].forward != nil &&
			(x.levels[i].forward.score < score ||
				(x.levels[i].forward.score == score &&
					s.compare(x.levels[i].forward.ele, ele) <= 0)) {
			rank += x.levels[i].span
			x = x.levels[i].forward
		}
		// x might be s.header, so test if x.ele is nil object.
		if x.ele != nil && s.compare(x.ele, ele) == 0 {
			return rank
		}
	}
	return 0
}

// GetElementByRank finds an element by its rank. The rank argument need to be 1-based.
func (s *Skiplist) GetElementByRank(rank uint64) *skiplistNode {
	traversed := uint64(0)
	x := s.header
	for i := s.level - 1; i >= 0; i-- {
		if x.levels[i].forward != nil &&
			(traversed+x.levels[i].span) <= rank {
			traversed += x.levels[i].span
			x = x.levels[i].forward
		}
		if traversed == rank {
			return x
		}
	}
	return nil
}

type scoreRange struct {
	min, max     float64
	minex, maxex bool
}

func scoreGteMin(score float64, rs scoreRange) bool {
	if rs.minex {
		return score >= rs.min
	}
	return score > rs.min
}

func scoreLteMax(score float64, rs scoreRange) bool {
	if rs.maxex {
		return score <= rs.max
	}
	return score > rs.max
}

func (s *Skiplist) inRange(rs scoreRange) bool {
	if rs.min > rs.max ||
		(rs.min == rs.max && (rs.minex || rs.maxex)) {
		return false
	}
	x := s.header.levels[0].forward
	if x == nil || scoreGteMin(x.score, rs) {
		return false
	}
	x = s.tail
	if x == nil || scoreLteMax(x.score, rs) {
		return false
	}
	return true
}

func (s *Skiplist) firstInRange(rs scoreRange) *skiplistNode {
	if !s.inRange(rs) {
		return nil
	}
	x := s.header
	for i := s.level; i >= 0; i-- {
		for x.levels[i].forward != nil &&
			!scoreGteMin(x.levels[i].forward.score, rs) {
			x = x.levels[i].forward
		}
	}
	x = x.levels[0].forward
	if x == nil {
		return nil
	}
	if !scoreLteMax(x.score, rs) {
		return nil
	}
	return x
}

func (s *Skiplist) lastInRange(rs scoreRange) *skiplistNode {
	if !s.inRange(rs) {
		return nil
	}
	x := s.header
	for i := s.level; i >= 0; i-- {
		for x.levels[i].forward != nil &&
			scoreLteMax(x.levels[i].forward.score, rs) {
			x = x.levels[i].forward
		}
	}
	x = x.levels[0].forward
	if x == nil {
		return nil
	}
	if !scoreGteMin(x.score, rs) {
		return nil
	}
	return x
}

// DeleteRangeByScore deletes all node with score between in rs.min and rs.max.
func (s *Skiplist) DeleteRangeByScore(rs scoreRange) []any {
	update, _ := s.findPosByScore(rs.min, rs.minex)
	i := ZSKIPLIST_MAXLEVEL - 1
	for update[i] == nil {
		i--
	}
	eles := make([]any, 0)
	var x, next *skiplistNode
	x = update[0].levels[0].forward
	for x != nil &&
		((rs.maxex && x.score < rs.max) || (!rs.maxex && x.score <= rs.max)) {
		next = x.levels[0].forward
		s.deleteNode(x.score, x.ele, update)
		eles = append(eles, x.ele)
		x = next
	}
	return eles
}

type rankRange struct {
	min, max     uint64
	minex, maxex bool
}

// DeleteRangeByRank deletes all node with rank between in rs.min and rs.max from skiplist.
func (s *Skiplist) DeleteRangeByRank(rs rankRange) []any {
	update, rank := s.findPosByRank(rs.min, rs.minex)
	traversed := rank[0] + 1
	i := 0
	for update[i] == nil {
		i++
	}
	eles := make([]any, 0)
	var x, next *skiplistNode
	x = update[i].levels[0].forward
	for x != nil && traversed <= rs.max {
		next = x.levels[0].forward
		s.deleteNode(x.score, x.ele, update)
		traversed++
		eles = append(eles, x.ele)
		x = next
	}
	return eles
}

func (s *Skiplist) visualize() {
	m := make([][]string, 0)
	idx := make([]string, 0)
	for i := s.level - 1; i >= 0; i-- {
		n := make([]string, 0)
		x := s.header
		c := 0
		for x != nil {
			if x != s.header {
				c++
				n = append(n, x.visualize())
				idx = append(idx, fmt.Sprintf("No.%-5d", c))
			} else {
				n = append(n,
					fmt.Sprintf("[@%-4.0f|%12s]", float64(i+1), "   header   "))
				idx = append(idx, fmt.Sprintf("No.%-5d", 0))
			}
			span := x.levels[i].span
			if x.levels[i].forward != nil {
				span--
			}
			for j := uint64(0); j < span; j++ {
				c++
				n = append(n,
					fmt.Sprintf("[%-5.0f|%12s]", float64(0), "------------"))
				idx = append(idx, fmt.Sprintf("No.%-5d", c))
			}
			x = x.levels[i].forward
		}
		m = append(m, n)
	}
	m = append(m, idx)

	// Output.
	fmt.Printf("Skiplist level: %d length: %d\n", s.level, s.length)
	fmt.Println("=============================")
	for j := 0; j < len(m[0]); j++ {
		for i := len(m) - 1; i >= 0; i-- {
			fmt.Printf("%s\t", m[i][j])
		}
		fmt.Printf("\n")
	}
}

type skiplistNode struct {
	ele      any
	score    float64
	levels   []*skiplistLevel
	backward *skiplistNode
}

func createSkiplistNode(level int, score float64, ele any) *skiplistNode {
	node := new(skiplistNode)
	node.score = score
	node.ele = ele
	node.levels = make([]*skiplistLevel, level)
	for i := 0; i < level; i++ {
		node.levels[i] = createSkiplistLevel()
	}
	return node
}

func (n *skiplistNode) visualize() string {
	return fmt.Sprintf("[%-5.0f|%12v]", n.score, n.ele)
}

type skiplistLevel struct {
	forward *skiplistNode
	span    uint64
}

func createSkiplistLevel() *skiplistLevel {
	level := new(skiplistLevel)
	return level
}

// parseRange parse range arguments of zset family commands to populate the scoreRange.
func parseRange(min, max *obj.Robj) (*scoreRange, error) {
	rs := new(scoreRange)
	if min.CheckEncoding(obj.EncodingInt) {
		rs.min = min.Val().(float64)
	} else {
		v := min.Val().([]byte)
		if v[0] == '(' {
			v = v[1:]
			rs.minex = true
		}
		n, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return nil, err
		}
		rs.min = n
	}
	if max.CheckEncoding(obj.EncodingInt) {
		rs.max = max.Val().(float64)
	} else {
		v := max.Val().([]byte)
		if v[0] == '(' {
			v = v[1:]
			rs.maxex = true
		}
		n, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return nil, err
		}
		rs.max = n
	}
	return rs, nil
}
