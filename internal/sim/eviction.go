package sim

// GDSF uses an aging key H=L+frequency*recreation_cost_ms/resident_bytes.
// The port treats a safe resident Actor as an item and C/R as recreation cost.
// All candidates share the same cost units. LRU changes only the pressure score.
// Source: FaaSCache (ASPLOS 2021), LambdaScheduler.calc_priority, revision
// 25110b6f378cdc0384adc712d7885509ecd1450e of github.com/aFuerst/faascache-sim.
type cacheItem struct {
	key       float64
	frequency int
}
type pressureEvictor struct {
	mode  string
	age   float64
	items map[string]cacheItem
}
type victim struct {
	index                     int
	id                        string
	frequency                 int
	lastAccess, costMS, bytes float64
}

func (p *pressureEvictor) touch(c victim) {
	if p.items == nil {
		p.items = map[string]cacheItem{}
	}
	p.items[c.id] = cacheItem{key: p.age + float64(max(1, c.frequency))*c.costMS/c.bytes, frequency: c.frequency}
}

func (p *pressureEvictor) evict(id string, pressure bool) {
	if pressure && p.mode != "lru" {
		if item, ok := p.items[id]; ok {
			p.age = item.key
		}
	}
	delete(p.items, id)
}

func (p *pressureEvictor) choose(candidates []victim) int {
	if len(candidates) == 0 {
		return -1
	}
	if p.items == nil {
		p.items = map[string]cacheItem{}
	}
	selected, lowest := -1, 0.0
	var selectedID string
	for _, c := range candidates {
		item, exists := p.items[c.id]
		if !exists || item.frequency != c.frequency {
			item = cacheItem{key: p.age + float64(max(1, c.frequency))*c.costMS/c.bytes, frequency: c.frequency}
			p.items[c.id] = item
		}
		score := item.key
		if p.mode == "lru" {
			score = c.lastAccess
		}
		if selected < 0 || score < lowest || score == lowest && c.id < selectedID {
			selected, lowest, selectedID = c.index, score, c.id
		}
	}
	return selected
}
