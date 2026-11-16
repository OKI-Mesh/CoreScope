package types

import (
	"encoding/hex"
	"fmt"
	"strings"
)

type RawFrame []byte

func ParseRawFrame(s string) (RawFrame, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("raw frame: %w", err)
	}
	if b == nil {
		b = []byte{}
	}
	return RawFrame(b), nil
}

func (f RawFrame) Hex() string {
	return strings.ToUpper(hex.EncodeToString(f))
}
