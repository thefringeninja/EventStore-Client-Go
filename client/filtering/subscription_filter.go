package filtering

type FilterType int

const (
	EventFilter       FilterType = 0
	StreamFilter      FilterType = 1
	NoMaxSearchWindow int        = -1
)

type SubscriptionFilterOptions struct {
	MaxSearchWindow    int
	CheckpointInterval int
	SubscriptionFilter *SubscriptionFilter
}
type SubscriptionFilter struct {
	FilterType FilterType
	Prefixes   []string
	RegexValue string
}

func NewFilterOnEventType() SubscriptionFilter {
	return SubscriptionFilter{
		FilterType: EventFilter,
	}
}

func NewFilterOnStreamName() SubscriptionFilter {
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
