package core

import (
	"fmt"

	panel "github.com/wyx2685/v2node/api/v2board"
)

func (v *V2Core) AddNode(tag string, info *panel.NodeInfo) error {
	if info.Type == "mieru" {
		m, err := newMieruInbound(v, tag, info)
		if err != nil {
			return fmt.Errorf("build mieru inbound error: %s", err)
		}
		if old, loaded := v.mieru.Swap(tag, m); loaded {
			_ = old.(*mieruInbound).Close()
		}
		return nil
	}
	inBoundConfig, err := buildInbound(info, tag)
	if err != nil {
		return fmt.Errorf("build inbound error: %s", err)
	}
	err = v.addInbound(inBoundConfig)
	if err != nil {
		return fmt.Errorf("add inbound error: %s", err)
	}
	return nil
}

func (v *V2Core) DelNode(tag string) error {
	if m, ok := v.mieru.LoadAndDelete(tag); ok {
		if err := m.(*mieruInbound).Close(); err != nil {
			return fmt.Errorf("close mieru inbound error: %s", err)
		}
		return nil
	}
	err := v.removeInbound(tag)
	if err != nil {
		return fmt.Errorf("remove in error: %s", err)
	}
	return nil
}
