package types

import "fmt"

type PayloadType int32

const (
	PayloadUnknown    PayloadType = -1 // DB sentinel for legacy NULL rows
	PayloadREQ        PayloadType = 0x00
	PayloadRESPONSE   PayloadType = 0x01
	PayloadTXT_MSG    PayloadType = 0x02
	PayloadACK        PayloadType = 0x03
	PayloadADVERT     PayloadType = 0x04
	PayloadGRP_TXT    PayloadType = 0x05
	PayloadGRP_DATA   PayloadType = 0x06
	PayloadANON_REQ   PayloadType = 0x07
	PayloadPATH       PayloadType = 0x08
	PayloadTRACE      PayloadType = 0x09
	PayloadMULTIPART  PayloadType = 0x0A
	PayloadCONTROL    PayloadType = 0x0B
	PayloadRAW_CUSTOM PayloadType = 0x0F
)

var payloadTypeNames = map[PayloadType]string{
	PayloadREQ:        "REQ",
	PayloadRESPONSE:   "RESPONSE",
	PayloadTXT_MSG:    "TXT_MSG",
	PayloadACK:        "ACK",
	PayloadADVERT:     "ADVERT",
	PayloadGRP_TXT:    "GRP_TXT",
	PayloadGRP_DATA:   "GRP_DATA",
	PayloadANON_REQ:   "ANON_REQ",
	PayloadPATH:       "PATH",
	PayloadTRACE:      "TRACE",
	PayloadMULTIPART:  "MULTIPART",
	PayloadCONTROL:    "CONTROL",
	PayloadRAW_CUSTOM: "RAW_CUSTOM",
}

func (p PayloadType) String() string {
	if name, ok := payloadTypeNames[p]; ok {
		return name
	}
	if p == PayloadUnknown {
		return "UNKNOWN"
	}
	return fmt.Sprintf("PayloadType(%d)", int32(p)) // reserved or not-yet-defined values
}

// Name returns the wire and API name for p: the firmware name for a defined
// type and "UNKNOWN" for anything else, so clients never see Go's
// PayloadType(n) form.
func (p PayloadType) Name() string {
	if name, ok := payloadTypeNames[p]; ok {
		return name
	}
	return "UNKNOWN"
}

// PathBytesAreHops reports whether the header path bytes of a packet of this
// type are route hop hashes (the normal case). TRACE repurposes them as
// per-hop SNR values.
func (p PayloadType) PathBytesAreHops() bool { return p != PayloadTRACE }

// Valid reports whether p is a payload type defined by the firmware.
// PayloadUnknown (the DB sentinel for legacy NULL rows) is not valid.
func (p PayloadType) Valid() bool {
	_, ok := payloadTypeNames[p]
	return ok
}
