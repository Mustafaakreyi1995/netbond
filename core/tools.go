//go:build tools

package core

// Keeps golang.org/x/mobile in go.mod; gomobile bind needs it there.
import _ "golang.org/x/mobile/bind"
