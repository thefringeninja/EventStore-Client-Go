package stream

import "github.com/EventStore/EventStore-Client-Go/types"

type RevisionExact struct {
	Value uint64
}

type RevisionPosition struct {
	Value types.Position
}

type RevisionStart struct {
}

type RevisionEnd struct {
}

type StreamPosition interface {
	AcceptRegularVisitor(visitor RegularStreamVisitor)
}

type AllStreamPosition interface {
	AcceptAllVisitor(visitor AllStreamVisitor)
}

type RegularStreamVisitor interface {
	VisitRevision(value uint64)
	VisitStart()
	VisitEnd()
}

type AllStreamVisitor interface {
	VisitPosition(value types.Position)
	VisitStart()
	VisitEnd()
}

func (r RevisionExact) AcceptRegularVisitor(visitor RegularStreamVisitor) {
	visitor.VisitRevision(r.Value)
}

func (r RevisionPosition) AcceptAllVisitor(visitor AllStreamVisitor) {
	visitor.VisitPosition(r.Value)
}

func (r RevisionStart) AcceptRegularVisitor(visitor RegularStreamVisitor) {
	visitor.VisitStart()
}

func (r RevisionEnd) AcceptRegularVisitor(visitor RegularStreamVisitor) {
	visitor.VisitEnd()
}

func (r RevisionStart) AcceptAllVisitor(visitor AllStreamVisitor) {
	visitor.VisitStart()
}

func (r RevisionEnd) AcceptAllVisitor(visitor AllStreamVisitor) {
	visitor.VisitEnd()
}

func Start() RevisionStart {
	return RevisionStart{}
}

func End() RevisionEnd {
	return RevisionEnd{}
}

func Position(value types.Position) AllStreamPosition {
	return RevisionPosition{
		Value: value,
	}
}

func Revision(value uint64) StreamPosition {
	return RevisionExact{
		Value: value,
	}
}
