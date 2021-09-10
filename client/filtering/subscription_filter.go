package filtering

type FilterType int

const (
	EventFilter       FilterType = 0
	StreamFilter      FilterType = 1
	NoMaxSearchWindow int        = -1
)

type SubscriptionFilterOptions struct {
	MaxSearchWindowValue    int
	CheckpointIntervalValue int
	SubscriptionFilter      SubscriptionFilter
}
type SubscriptionFilter struct {
	FilterType FilterType
	Prefixes   []string
	RegexValue string
}

func SubscriptionFilterOptionsDefault(filter SubscriptionFilter) SubscriptionFilterOptions {
	return SubscriptionFilterOptions{
		MaxSearchWindowValue:    32,
		CheckpointIntervalValue: 1,
		SubscriptionFilter:      filter,
	}
}

func (opts SubscriptionFilterOptions) MaxSearchWindow(value int) SubscriptionFilterOptions {
	opts.MaxSearchWindowValue = value
	return opts
}

func (opts SubscriptionFilterOptions) CheckpointInterval(value int) SubscriptionFilterOptions {
	opts.CheckpointIntervalValue = value
	return opts
}

func NewSubscriptionFilterOptions(maxSearchWindow int, checkpointInterval int, filter SubscriptionFilter) SubscriptionFilterOptions {
	return SubscriptionFilterOptions{
		MaxSearchWindowValue:    maxSearchWindow,
		CheckpointIntervalValue: checkpointInterval,
		SubscriptionFilter:      filter,
	}
}

func FilterOnEventType() *SubscriptionFilter {
	return &SubscriptionFilter{
		FilterType: StreamFilter,
		Prefixes:   []string{},
	}
}

func FilterOnStreamName() *SubscriptionFilter {
	return &SubscriptionFilter{
		FilterType: StreamFilter,
	}
}

func (filter SubscriptionFilter) Regex(value string) SubscriptionFilter {
	filter.RegexValue = value
	return filter
}

func (filter SubscriptionFilter) AddPrefixes(values ...string) SubscriptionFilter {
	filter.Prefixes = append(filter.Prefixes, values...)

	return filter
}
