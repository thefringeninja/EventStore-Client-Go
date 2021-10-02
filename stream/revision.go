package stream

type RevisionAny struct{}
type RevisionStreamExists struct{}
type RevisionNoStream struct{}

type ExpectedRevision interface {
	AcceptExpectedRevision(visitor RevisionVisitor)
}

type RevisionVisitor interface {
	VisitAny()
	VisitStreamExists()
	VisitNoStream()
	VisitExact(value uint64)
}

func (r RevisionAny) AcceptExpectedRevision(visitor RevisionVisitor) {
	visitor.VisitAny()
}

func (r RevisionStreamExists) AcceptExpectedRevision(visitor RevisionVisitor) {
	visitor.VisitStreamExists()
}

func (r RevisionNoStream) AcceptExpectedRevision(visitor RevisionVisitor) {
	visitor.VisitNoStream()
}

func (r RevisionExact) AcceptExpectedRevision(visitor RevisionVisitor) {
	visitor.VisitExact(r.Value)
}

func Any() ExpectedRevision {
	return RevisionAny{}
}

func Exists() ExpectedRevision {
	return RevisionStreamExists{}
}

func NoStream() ExpectedRevision {
	return RevisionNoStream{}
}

func Exact(value uint64) ExpectedRevision {
	return RevisionExact{Value: value}
}
