// Command telesrv-sfu starts the dedicated standalone SFU media role.
package main

import (
	"telesrv/internal/node/common"
	sfunode "telesrv/internal/node/sfu"
)

func main() {
	common.Main("telesrv-sfu", sfunode.Run)
}
