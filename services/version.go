package services

import (
	"fmt"
)

// Version numbers
var majorVersion = 0
var minorVersion = 13
var patchVersion = 4

// Version string
var version = fmt.Sprintf("%d.%d.%d", majorVersion, minorVersion, patchVersion)
