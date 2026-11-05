package types

type MultibyteSupport int32

const (
	MultibyteUnknown   MultibyteSupport = 0
	MultibyteSuspected MultibyteSupport = 1
	MultibyteConfirmed MultibyteSupport = 2
)

func (m MultibyteSupport) Valid() bool { return m >= MultibyteUnknown && m <= MultibyteConfirmed }
func (m MultibyteSupport) String() string {
	switch m {
	case MultibyteUnknown:
		return "unknown"
	case MultibyteSuspected:
		return "suspected"
	case MultibyteConfirmed:
		return "confirmed"
	default:
		return "invalid"
	}
}
func ParseMultibyteSupport(s string) (MultibyteSupport, bool) {
	switch s {
	case "unknown":
		return MultibyteUnknown, true
	case "suspected":
		return MultibyteSuspected, true
	case "confirmed":
		return MultibyteConfirmed, true
	default:
		return MultibyteUnknown, false
	}

}

// MultibyteEvidence is how a node's multi-byte capability was established.
type MultibyteEvidence string

const (
	EvidenceNone   MultibyteEvidence = ""
	EvidenceAdvert MultibyteEvidence = "advert"
	EvidencePath   MultibyteEvidence = "path"
)

func (e MultibyteEvidence) Valid() bool {
	return e == EvidenceNone || e == EvidenceAdvert || e == EvidencePath
}
