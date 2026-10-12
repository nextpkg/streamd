package base

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFragmentStartsAtZeroTimestamp(t *testing.T) {
	fragment := NewFragment(0)
	fragment.Update(0)
	fragment.Update(2000)
	require.Equal(t, int64(2000), fragment.Duration())
	fragment.Update(4000)
	require.Equal(t, int64(4000), fragment.Duration())
	fragment.Create()
	fragment.Update(0)
	fragment.Update(1000)
	require.Equal(t, int64(1000), fragment.Duration())
}
