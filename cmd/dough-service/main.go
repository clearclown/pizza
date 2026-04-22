// Command dough-service は M1 Seed の gRPC サーバ。
// Phase 0: 骨格のみ。listen しない。
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "dough-service: not implemented (Phase 1 target)")
	os.Exit(0)
}
