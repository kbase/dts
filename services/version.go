package services

import (
	"fmt"
)

// Version numbers
var majorVersion = 0
var minorVersion = 12
var patchVersion = 1

// Version string
var version = fmt.Sprintf("%d.%d.%d", majorVersion, minorVersion, patchVersion)
