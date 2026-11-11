// Command wfe is the server and CLI (SPEC §13.2). Subcommands arrive with WI-025.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "wfe: not built yet; see docs/WORK-ITEMS.md")
	os.Exit(2)
}
