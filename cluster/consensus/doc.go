// Package consensus implements the cluster consensus module agent.
//
// Agent owns the consensus module duty cycle. LogPublisher appends cluster log
// frames. LogAdapter polls the cluster log. Package cluster assembles these
// parts. Package election runs the election state machine.
package consensus
