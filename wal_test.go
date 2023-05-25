package wal

import (
	"fmt"
	"testing"
)

func TestName(t *testing.T) {
	name := "000900101.wal"

	var id int
	_, err := fmt.Sscanf(name, "%d"+segmentFileSuffix, &id)
	t.Log(err)
	t.Log("id = ", id)
}
