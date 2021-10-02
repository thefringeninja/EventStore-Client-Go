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

func (o *SubscriptionFilterOptions) SetDefaults() {
	if o.MaxSearchWindowValue == 0 {
		o.MaxSearchWindowValue = 32
	}

	if o.CheckpointIntervalValue == 0 {
		o.CheckpointIntervalValue = 1
	}
}

func (o SubscriptionFilterOptions) SetMaxSearchWindow(value int) SubscriptionFilterOptions {
	o.MaxSearchWindowValue = value
	return o
}

func (o SubscriptionFilterOptions) SetCheckpointInterval(value int) SubscriptionFilterOptions {
	o.CheckpointIntervalValue = value
	return o
}

func NewSubscriptionFilterOptions(maxSearchWindow int, checkpointInterval int, filter SubscriptionFilter) SubscriptionFilterOptions {
	return SubscriptionFilterOptions{
		MaxSearchWindowValue:    maxSearchWindow,
		CheckpointIntervalValue: checkpointInterval,
		SubscriptionFilter:      filter,
	}
}

func FilterOnEventType() SubscriptionFilter {
	return SubscriptionFilter{
		FilterType: EventFilter,
		Prefixes:   []string{},
	}
}

func FilterOnStreamName() SubscriptionFilter {
	return SubscriptionFilter{
		FilterType: StreamFilter,
	}
}

func (f *SubscriptionFilter) SetRegex(value string) {
	f.RegexValue = value
}

func (f *SubscriptionFilter) AddPrefixes(values ...string) {
	f.Prefixes = append(f.Prefixes, values...)
}

func (f *SubscriptionFilter) Regex() *string {
	if f.RegexValue == "" {
		return nil
	}

	return &f.RegexValue
}

func (f *SubscriptionFilter) PrefixesI() []string {
	return f.Prefixes
}
