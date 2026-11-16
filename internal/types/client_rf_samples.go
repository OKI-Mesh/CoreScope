package types

import "time"

type RfSampleWithPrev struct {
	SampledAt                                      time.Time
	Lat, Lon                                       float64
	Stationary                                     bool
	UptimeSecs                                     int64
	RxAirSecs, TxAirSecs, Recv, RecvErrors         *int64
	PrevAt                                         *time.Time
	PrevUptime, PrevRx, PrevTx, PrevRecv, PrevErrs *int64
}
