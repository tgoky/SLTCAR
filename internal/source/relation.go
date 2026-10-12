package source

import (
	"container/list"
	"fmt"
	"strconv"
	"strings"

	"github.com/YOURNAME/slotcar/internal/decode"
	"github.com/YOURNAME/slotcar/internal/metrics"
	"github.com/YOURNAME/slotcar/internal/pipeline"
	"github.com/jackc/pglogrepl"
)

const colFlagKey uint8 = 1 // pgoutput: column is part of the replica identity

// Only values at least this long can have been moved out-of-line by Postgres
// TOAST, so only rows containing one are worth remembering.
const toastMinLen = 1024

// Rel is a cached relation schema from a pgoutput Relation message.
type Rel struct {
	OID     uint32
	Schema  string
	Table   string
	Columns []*pglogrepl.RelationMessageColumn
}

func newRel(m *pglogrepl.RelationMessage) *Rel {
	return &Rel{OID: m.RelationID, Schema: m.Namespace, Table: m.RelationName, Columns: m.Columns}
}

// KeyString builds a cache key from the relation OID plus the replica-identity
// columns of a tuple. The OID prefix keeps tables with equal key values apart.
func (r *Rel) KeyString(t *pglogrepl.TupleData) string {
	var b strings.Builder
	b.WriteString(strconv.FormatUint(uint64(r.OID), 10))
	b.WriteByte('|')
	for i, col := range r.Columns {
		if col.Flags&colFlagKey == 0 {
			continue
		}
		b.WriteString(col.Name)
		b.WriteByte('=')
		if i < len(t.Columns) && t.Columns[i].DataType == pglogrepl.TupleDataTypeText {
			b.Write(t.Columns[i].Data)
		} else {
			b.WriteByte('?')
		}
		b.WriteByte(';')
	}
	return b.String()
}

// RelationCache maps relation OIDs to schemas. Only touched by the single
// source goroutine, so no locking is required (documented invariant).
type RelationCache struct{ m map[uint32]*Rel }

func NewRelationCache() *RelationCache { return &RelationCache{m: make(map[uint32]*Rel)} }

func (c *RelationCache) Store(m *pglogrepl.RelationMessage) { c.m[m.RelationID] = newRel(m) }
func (c *RelationCache) Get(oid uint32) *Rel                { return c.m[oid] }
func (c *RelationCache) Reset()                             { c.m = make(map[uint32]*Rel) }

type cacheEntry struct {
	key  string
	row  []pipeline.Value
	size int
}

// ToastCache is an LRU of the last complete row image per key, bounded both
// by entry count and by approximate bytes. It reconstructs unchanged-TOAST
// columns ('u' placeholders) on updates; without it, a big column Postgres did
// not re-send would be written as NULL.
type ToastCache struct {
	maxEntries int
	maxBytes   int
	bytes      int
	order      *list.List
	els        map[string]*list.Element
}

func NewToastCache(maxEntries, maxBytes int) *ToastCache {
	return &ToastCache{maxEntries: maxEntries, maxBytes: maxBytes, order: list.New(), els: make(map[string]*list.Element)}
}

func rowSize(row []pipeline.Value) (size int, large bool) {
	for _, v := range row {
		size += 16
		if s, ok := v.Val.(string); ok {
			size += len(s)
			if len(s) >= toastMinLen {
				large = true
			}
		}
	}
	return size, large
}

func (c *ToastCache) Get(key string) ([]pipeline.Value, bool) {
	if el, ok := c.els[key]; ok {
		c.order.MoveToFront(el)
		return el.Value.(*cacheEntry).row, true
	}
	return nil, false
}

// Remember stores a complete row image, but only if it contains a value large
// enough to be TOASTed — this keeps the cache small on ordinary tables.
func (c *ToastCache) Remember(key string, row []pipeline.Value) {
	if size, large := rowSize(row); large {
		c.set(key, row, size)
	}
}

func (c *ToastCache) set(key string, row []pipeline.Value, size int) {
	if el, ok := c.els[key]; ok {
		e := el.Value.(*cacheEntry)
		c.bytes += size - e.size
		e.row, e.size = row, size
		c.order.MoveToFront(el)
	} else {
		c.els[key] = c.order.PushFront(&cacheEntry{key: key, row: row, size: size})
		c.bytes += size
	}
	for c.order.Len() > 1 && (c.order.Len() > c.maxEntries || c.bytes > c.maxBytes) {
		c.removeEl(c.order.Back())
	}
}

func (c *ToastCache) removeEl(el *list.Element) {
	e := el.Value.(*cacheEntry)
	c.order.Remove(el)
	delete(c.els, e.key)
	c.bytes -= e.size
}

func (c *ToastCache) Del(key string) {
	if el, ok := c.els[key]; ok {
		c.removeEl(el)
	}
}

func (c *ToastCache) Reset() {
	c.els = make(map[string]*list.Element)
	c.order.Init()
	c.bytes = 0
}

// buildRow decodes a tuple into a row, restoring unchanged-TOAST columns from
// the cache. oldTuple (when present) supplies the key for cache lookups on
// key-changing updates. complete=false means at least one TOAST value could
// not be restored (it is written as NULL — a documented limitation on the
// first touch of a key after restart; REPLICA IDENTITY FULL avoids it).
func buildRow(rel *Rel, newTuple, oldTuple *pglogrepl.TupleData, toast *ToastCache) ([]pipeline.Value, bool, error) {
	if newTuple == nil || len(newTuple.Columns) != len(rel.Columns) {
		return nil, false, fmt.Errorf("tuple/relation column count mismatch for %s.%s", rel.Schema, rel.Table)
	}
	keyFrom := newTuple
	if oldTuple != nil {
		keyFrom = oldTuple
	}
	cached, hasCached := toast.Get(rel.KeyString(keyFrom))

	out := make([]pipeline.Value, len(rel.Columns))
	incomplete := false
	for i, col := range rel.Columns {
		v := pipeline.Value{Name: col.Name, OID: col.DataType, IsKey: col.Flags&colFlagKey != 0}
		tc := newTuple.Columns[i]
		switch tc.DataType {
		case pglogrepl.TupleDataTypeNull:
			v.Val = nil
		case pglogrepl.TupleDataTypeText:
			val, err := decode.ByOID(col.DataType, tc.Data)
			if err != nil {
				return nil, false, fmt.Errorf("col %s: %w", col.Name, err)
			}
			v.Val = val
		case pglogrepl.TupleDataTypeToast:
			if hasCached && len(cached) == len(rel.Columns) && cached[i].Name == col.Name {
				v.Val = cached[i].Val
				metrics.ToastCacheHits.Inc()
			} else {
				v.Val = nil
				incomplete = true
				metrics.ToastCacheMisses.Inc()
			}
		default:
			return nil, false, fmt.Errorf("unsupported tuple data type %q for col %s", string(rune(tc.DataType)), col.Name)
		}
		out[i] = v
	}
	return out, !incomplete, nil
}
