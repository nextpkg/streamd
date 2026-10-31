// clock reports time from inside the test service's container, without depending
// on a particular VM manager or the precision of its shell's date command.
package main

import (
	"fmt"
	"time"
)

func main() { fmt.Println(time.Now().UnixNano()) }
