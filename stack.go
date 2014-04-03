package main

import ()

type Node struct {
	Value string
}

// Stack is a basic LIFO stack that resizes as needed.
type Stack struct {
	nodes []*Node
	count int
}

func NewStack() Stack {
	s := Stack{count: 0}
	return s
}

// Push adds a node to the stack.
func (s *Stack) Push(n *Node) {
	if s.count >= len(s.nodes) {
		nodes := make([]*Node, len(s.nodes)*2)
		copy(nodes, s.nodes)
		s.nodes = nodes
	}
	//	log.Printf("%v:%v:%v", s.nodes, len(s.nodes), s.count)
	s.nodes[s.count] = n
	s.count++
}
func (s *Stack) PeekNodes() []*Node {
	return s.nodes
}

// Pop removes and returns a node from the stack in last to first order.
func (s *Stack) Pop() *Node {
	if s.count == 0 {
		return nil
	}
	node := s.nodes[s.count-1]
	s.count--
	s.nodes = s.nodes[:s.count]
	return node
}

func (s *Stack) Clear() {
	s.count = 1
	nodes := make([]*Node, s.count*2)
	s.nodes = nodes
}

// Pop removes and returns a node from the stack in last to first order.
func (s *Stack) Peek() *Node {
	if s.count == 0 {
		return nil
	}
	return s.nodes[s.count-1]
}
