package streamrevision

type RevisionAny struct {}
type RevisionStreamExists struct {}
type RevisionNoStream struct {}
type RevisionExact struct {
	value uint64
}

type ExpectedRevision interface {
	AcceptExpectedRevision(visitor RevisionVisitor)
}

type RevisionVisitor interface {
	VisitAny()
	VisitStreamExists()
	VisitNoStream()
	VisitExact(value uint64)
}

func (any RevisionAny) AcceptExpectedRevision(visitor RevisionVisitor) {
	visitor.VisitAny()
}

func (streamExist RevisionStreamExists) AcceptExpectedRevision(visitor RevisionVisitor) {
	visitor.VisitStreamExists()
}

func (noStream RevisionNoStream) AcceptExpectedRevision(visitor RevisionVisitor) {
	visitor.VisitNoStream()
}

func (exact RevisionExact) AcceptExpectedRevision(visitor RevisionVisitor) {
	visitor.VisitExact(exact.value)
}

func Any() ExpectedRevision {
	return RevisionAny{}
}

func StreamExists() ExpectedRevision {
	return RevisionStreamExists{}
}

func NoStream() ExpectedRevision {
	return RevisionNoStream{}
}

func Exact(value uint64) ExpectedRevision {
	return RevisionExact { value: value }
}
