package main

import "context"

// strictRegionObservers returns the observers in region, or nil when no region
// was asked for. A region that is unknown, or that has no observers, gives an
// empty non-nil set, so the caller filters everything out. resolveRegionObservers
// returns nil in those cases, which callers read as "no filter" and so show
// unfiltered data.
func (s *PacketStore) strictRegionObservers(ctx context.Context, region string) map[string]bool {
	if region == "" {
		return nil
	}
	if observers := s.resolveRegionObservers(ctx, region); observers != nil {
		return observers
	}
	return map[string]bool{}
}

// strictAreaNodes returns the node pubkeys inside area, or nil when no area was
// asked for. An area that is not configured, or that holds no nodes, gives an
// empty non-nil set, so the caller filters everything out.
func (s *PacketStore) strictAreaNodes(ctx context.Context, area string) map[string]bool {
	if area == "" {
		return nil
	}
	if nodes := s.resolveAreaNodes(ctx, area); nodes != nil {
		return nodes
	}
	return map[string]bool{}
}
