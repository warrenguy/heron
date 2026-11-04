// Package recordinglog reads and writes the cluster recording.log file.
//
// Log holds an open recording.log and its decoded entries. Log.PlanRecovery
// computes the RecoveryPlan a member uses at startup.
package recordinglog
