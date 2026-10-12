package driver

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIsExpired(t *testing.T) {
	at := require.New(t)

	now := time.Unix(2000000000, 0)
	key := fmt.Sprintf("test/test/big/%d.flv", now.Unix()-1800)
	at.False(isExpiredAt(key, 900, now))
	at.True(isExpiredAt(key, 899, now))
}
