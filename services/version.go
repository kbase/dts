package services

import (
	"fmt"
)

// Version numbers
var majorVersion = 0
var minorVersion = 10
var patchVersion = 2

// Version string
var version = fmt.Sprintf("%d.%d.%d", majorVersion, minorVersion, patchVersion)
