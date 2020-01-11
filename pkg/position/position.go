package position

// Position ...
type Position struct {
	Commit  int64
	Prepare int64
}

// StartPosition ...
func StartPosition() Position {
	return Position{
		Commit:  0,
		Prepare: 0,
	}
}

// EndPosition ...
func EndPosition() Position {
	return Position{
		Commit:  -1,
		Prepare: -1,
	}
}

// NewPosition ...
func NewPosition(commit int64, prepare int64) Position {
	return Position{
		Commit:  commit,
		Prepare: prepare,
	}
}
