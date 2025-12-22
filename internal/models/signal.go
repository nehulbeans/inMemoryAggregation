package models

type SignalRequest struct {
	App     string `json:"app"`
	Country string `json:"country"`
	Signal  string `json:"signal"`
	UserID  string `json:"user_id,omitempty"`
}

type AggregateKey struct {
	Date    string
	App     string
	Country string
}

type Metrics struct {
	AdRequest    int64
	AdResponse   int64
	AdImpression int64
	AdClicks     int64
	DAU          int64
	UniqueUsers  map[string]struct{}
}
