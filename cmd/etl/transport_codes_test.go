package main

import (
	"reflect"
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func TestTransportCodes(t *testing.T) {
	const payload = "0102030405"
	codes := func(first, second string) *[2]string { return &[2]string{first, second} }

	cases := []struct {
		name    string
		rawHex  string
		route   types.RouteType
		want    *[2]string
		wantErr bool
	}{
		{"transport flood", "14" + "AABB" + "CCDD" + "00" + payload, types.RouteTransportFlood, codes("AABB", "CCDD"), false},
		{"transport direct", "17" + "1234" + "5678" + "00" + payload, types.RouteTransportDirect, codes("1234", "5678"), false},
		{"flood carries no codes", "15" + "00" + payload, types.RouteFlood, nil, false},
		{"direct carries no codes", "16" + "00" + payload, types.RouteDirect, nil, false},
		{"unknown route carries no codes", "14" + "AABB" + "CCDD" + "00" + payload, types.RouteUnknown, nil, false},
		{"stored transport route, header says flood", "15" + "00" + payload, types.RouteTransportFlood, nil, false},
		{"transport frame cut off inside the codes", "14AABB", types.RouteTransportFlood, nil, true},
		{"frame too short for a header and path byte", "14", types.RouteTransportFlood, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			frame, err := types.ParseRawFrame(c.rawHex)
			if err != nil {
				t.Fatal(err)
			}
			code1, code2, err := transportCodes(frame, c.route)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, want error: %v", err, c.wantErr)
			}
			var got *[2]string
			if code1 != nil && code2 != nil {
				got = &[2]string{*code1, *code2}
			}
			if (code1 == nil) != (code2 == nil) {
				t.Fatalf("code1 = %v, code2 = %v: want both set or both nil", code1, code2)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("codes = %v, want %v", got, c.want)
			}
		})
	}
}
