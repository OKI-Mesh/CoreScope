package types

// NodeRole is one of the four roles MeshCore defines. A standalone observer
// has no role (NULL in nodes.role); an observer that is also a node has that node's role.
type NodeRole string

const (
	RoleRepeater  NodeRole = "repeater"
	RoleCompanion NodeRole = "companion"
	RoleRoom      NodeRole = "room"
	RoleSensor    NodeRole = "sensor"
)

// API-only value for a pubkey known only from packets; not a real MeshCore role
const RoleUnknown NodeRole = "unknown"
const RoleObserver NodeRole = "observer"

var roleAdvertTypes = [...]struct {
	role       NodeRole
	advertType int
}{
	{RoleCompanion, 1},
	{RoleRepeater, 2},
	{RoleRoom, 3},
	{RoleSensor, 4},
}

func (r NodeRole) Valid() bool {
	switch r {
	case RoleRepeater, RoleCompanion, RoleRoom, RoleSensor:
		return true
	}
	return false
}

func (r NodeRole) Filterable() bool {
	return r.Valid() || r == RoleObserver
}

func (r NodeRole) AdvertType() (int, bool) {
	for _, e := range roleAdvertTypes {
		if e.role == r {
			return e.advertType, true
		}
	}
	return 0, false
}

func NodeRoleFromAdvertType(advertType int) (NodeRole, bool) {
	for _, e := range roleAdvertTypes {
		if e.advertType == advertType {
			return e.role, true
		}
	}
	return "", false
}

func (r NodeRole) RelaysTraffic() bool {
	return r == RoleRepeater || r == RoleRoom
}
