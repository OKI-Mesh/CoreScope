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

func (p PayloadType) String() string {
	switch p {
	case PayloadREQ:
		return "REQ"
	case PayloadRESPONSE:
		return "RESPONSE"
	case PayloadTXT_MSG:
		return "TXT_MSG"
	case PayloadACK:
		return "ACK"
	case PayloadADVERT:
		return "ADVERT"
	case PayloadGRP_TXT:
		return "GRP_TXT"
	case PayloadGRP_DATA:
		return "GRP_DATA"
	case PayloadANON_REQ:
		return "ANON_REQ"
	case PayloadPATH:
		return "PATH"
	case PayloadTRACE:
		return "TRACE"
	case PayloadMULTIPART:
		return "MULTIPART"
	case PayloadCONTROL:
		return "CONTROL"
	case PayloadRAW_CUSTOM:
		return "RAW_CUSTOM"
	case PayloadUnknown:
		return "UNKNOWN"
	default:
		return fmt.Sprintf("PayloadType(%d)", int32(p)) // reserved or not-yet-defined values
	}
}

// PathBytesAreHops reports whether the header path bytes of a packet of this
// type are route hop hashes (the normal case). TRACE repurposes them as
// per-hop SNR values.
func (p PayloadType) PathBytesAreHops() bool { return p != PayloadTRACE }
