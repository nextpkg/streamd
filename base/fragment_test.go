package base

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFragment(t *testing.T) {
	at := assert.New(t)

	f := NewFragment(0)

	f.Update(10)
	at.Equal(int64(0), f.Duration())

	f.Update(20)
	at.Equal(int64(10), f.Duration())

	// case 回退的时间不会被应用
	f.Update(5)
	at.Equal(int64(10), f.Duration())

	f.Create()
	f.Update(50)
	at.Equal(int64(0), f.Duration())

	f.Update(80)
	at.Equal(int64(30), f.Duration())

	at.Equal("1", f.Sequence())
}

func TestIsNewer(t *testing.T) {
	at := assert.New(t)

	f := NewFragment(0)
	at.True(f.isNewer(2, 1))
	at.False(f.isNewer(1, 2))

	var t1 uint32 = math.MaxUint32
	at.True(f.isNewer(t1+10, t1))
	at.True(f.isNewer(t1+20, t1+10))

	t1 = math.MaxUint32 - 10
	at.True(f.isNewer(t1+10, t1))
}

func TestMaxFragment(t *testing.T) {
	at := assert.New(t)
	f := NewFragment(0)

	var t1 uint32 = math.MaxUint32 - 20
	f.Update(t1)
	at.Equal(int64(0), f.Duration())

	f.Update(t1 + 10)
	at.Equal(int64(10), f.Duration())

	f.Update(t1 + 20)
	at.Equal(int64(20), f.Duration())
}
