package deviceinventory

import (
	"encoding/json"
	"errors"
	"os"

	"everything-go/internal/protocol"
)

type Release struct {
	protocol.ClientInfo
	Order int `json:"order"`
}
type Catalog struct {
	Schema   int                   `json:"schema_version"`
	Releases []Release             `json:"releases"`
	Targets  []protocol.ClientInfo `json:"targets"`
}

func LoadCatalog(path string) (Catalog, error) {
	var catalog Catalog
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return catalog, nil
	}
	if err != nil {
		return catalog, err
	}
	if len(data) > 1<<20 {
		return catalog, errors.New("catalog too large")
	}
	if err = json.Unmarshal(data, &catalog); err != nil {
		return Catalog{}, err
	}
	if catalog.Schema != 1 {
		return Catalog{}, errors.New("unsupported catalog schema")
	}
	seen := map[protocol.ClientInfo]bool{}
	orders := map[string]bool{}
	for _, r := range catalog.Releases {
		if !ValidInfo(&r.ClientInfo) || r.Order < 1 || seen[r.ClientInfo] {
			return Catalog{}, errors.New("invalid release catalog")
		}
		seen[r.ClientInfo] = true
		order, _ := json.Marshal([]any{r.Platform, r.AppID, r.Channel, r.Order})
		if orders[string(order)] {
			return Catalog{}, errors.New("duplicate release order")
		}
		orders[string(order)] = true
	}
	targets := map[string]bool{}
	for _, target := range catalog.Targets {
		key, _ := json.Marshal([]string{target.Platform, target.AppID, target.Channel})
		if !seen[target] || targets[string(key)] {
			return Catalog{}, errors.New("invalid release target")
		}
		targets[string(key)] = true
	}
	return catalog, nil
}
func (c Catalog) Status(info *protocol.ClientInfo) string {
	if info == nil {
		return "not_reported"
	}
	if info.Channel == "unknown" {
		return "channel_unknown"
	}
	for _, target := range c.Targets {
		if target.Platform != info.Platform || target.AppID != info.AppID || target.Channel != info.Channel {
			continue
		}
		if target == *info {
			return "matches_target"
		}
		var currentOrder, targetOrder int
		for _, r := range c.Releases {
			if r.ClientInfo == *info {
				currentOrder = r.Order
			}
			if r.ClientInfo == target {
				targetOrder = r.Order
			}
		}
		if currentOrder == 0 {
			return "unrecognized_version"
		}
		if currentOrder < targetOrder {
			return "older_version"
		}
		return "newer_than_target"
	}
	return "target_not_set"
}
