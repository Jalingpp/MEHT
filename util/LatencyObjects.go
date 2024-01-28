package util

import "time"

type UpdateLO struct {
	Duration time.Duration
}

type GetKeyLO struct {
	Duration time.Duration
}

type GetValueLO struct {
	Duration time.Duration
}

type GetProofLO struct {
	Duration time.Duration
}

type CacheAdjustLO struct {
	Duration time.Duration
}

type PhaseLatency struct {
	UpdateLOs      []UpdateLO
	GetKeyLOs      []GetKeyLO
	GetValueLOs    []GetValueLO
	GetProofLOs    []GetProofLO
	CacheAdjustLOs []CacheAdjustLO
}

// NewKVPair creates a new KVPair object
func NewPhaseLatency() *PhaseLatency {
	UpdateLOs := make([]UpdateLO, 0)
	GetKeyLOs := make([]GetKeyLO, 0)
	GetValueLOs := make([]GetValueLO, 0)
	GetProofLOs := make([]GetProofLO, 0)
	CacheAdjustLOs := make([]CacheAdjustLO, 0)
	return &PhaseLatency{UpdateLOs: UpdateLOs, GetKeyLOs: GetKeyLOs, GetValueLOs: GetValueLOs, GetProofLOs: GetProofLOs, CacheAdjustLOs: CacheAdjustLOs}
}

func (pl *PhaseLatency) RecordLatencyObject(lo_type string, dur time.Duration) {
	switch lo_type {
	case "update":
		lo := UpdateLO{Duration: dur}
		pl.UpdateLOs = append(pl.UpdateLOs, lo)
	case "getkey":
		lo := GetKeyLO{Duration: dur}
		pl.GetKeyLOs = append(pl.GetKeyLOs, lo)
	case "getvalue":
		lo := GetValueLO{Duration: dur}
		pl.GetValueLOs = append(pl.GetValueLOs, lo)
	case "getproof":
		lo := GetProofLO{Duration: dur}
		pl.GetProofLOs = append(pl.GetProofLOs, lo)
	case "cacheadjust":
		lo := CacheAdjustLO{Duration: dur}
		pl.CacheAdjustLOs = append(pl.CacheAdjustLOs, lo)
	}
}

//将pl中各阶段的latecy求和后放入第0号元素
func (pl *PhaseLatency) CompPhaseLatency() {
	//update
	sum := time.Duration(0)
	for i := 0; i < len(pl.UpdateLOs); i++ {
		sum += pl.UpdateLOs[i].Duration
	}
	if len(pl.UpdateLOs) > 0 {
		pl.UpdateLOs[0].Duration = sum
	} else {
		pl.UpdateLOs = append(pl.UpdateLOs, UpdateLO{Duration: sum})
	}
	//getkey
	sum = time.Duration(0)
	for i := 0; i < len(pl.GetKeyLOs); i++ {
		sum += pl.GetKeyLOs[i].Duration
	}
	pl.GetKeyLOs[0].Duration = sum
	//getvalue
	sum = time.Duration(0)
	for i := 0; i < len(pl.GetValueLOs); i++ {
		sum += pl.GetValueLOs[i].Duration
	}
	pl.GetValueLOs[0].Duration = sum
	//getproof
	sum = time.Duration(0)
	for i := 0; i < len(pl.GetProofLOs); i++ {
		sum += pl.GetProofLOs[i].Duration
	}
	pl.GetProofLOs[0].Duration = sum
	//cacheadjust
	sum = time.Duration(0)
	for i := 0; i < len(pl.CacheAdjustLOs); i++ {
		sum += pl.CacheAdjustLOs[i].Duration
	}
	if len(pl.CacheAdjustLOs) > 0 {
		pl.CacheAdjustLOs[0].Duration = sum
	} else {
		pl.CacheAdjustLOs = append(pl.CacheAdjustLOs, CacheAdjustLO{Duration: sum})
	}

}
