package decoder

import "testing"

func TestPayloadChannelReference(t *testing.T) {
	cases := []struct {
		name          string
		payload       Payload
		wantRef       string
		wantEncrypted bool
	}{
		{"decrypted channel", Payload{Type: "CHAN", Channel: "#wardriving", ChannelHashHex: "81"}, "#wardriving", false},
		{"decrypted without a name", Payload{Type: "CHAN", ChannelHashHex: "81"}, "", false},
		{"undecrypted group text", Payload{Type: "GRP_TXT", ChannelHashHex: "81"}, "81", true},
		{"undecrypted without a hash byte", Payload{Type: "GRP_TXT"}, "", false},
		{"other payload type", Payload{Type: "GRP_DATA", Channel: "#x", ChannelHashHex: "81"}, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ref, encrypted := c.payload.ChannelReference()
			if ref != c.wantRef || encrypted != c.wantEncrypted {
				t.Errorf("ChannelReference() = (%q, %v), want (%q, %v)", ref, encrypted, c.wantRef, c.wantEncrypted)
			}
		})
	}
}
