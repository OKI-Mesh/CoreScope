package decoder

// ChannelHash returns the value stored in transmissions.channel_hash for a
// group-text payload, and whether that value is only the one-byte channel hash
// of a channel whose name is still unknown (the payload could not be decrypted).
// A payload that is neither returns "" and false.
func (p Payload) ChannelReference() (hash string, encrypted bool) {
	switch {
	case p.Type == "CHAN" && p.Channel != "":
		return p.Channel, false
	case p.Type == "GRP_TXT" && p.ChannelHashHex != "":
		return p.ChannelHashHex, true
	}
	return "", false
}
