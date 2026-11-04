//go:build integration && cluster_java

// Package clusterjava is the Java cluster integration contract oracle. It
// holds the ports, channels, stream ids, and goldens that the Go tests and the
// Java cluster nodes must agree on.
//
// Edit these values and ClusterConstants.java together. ClusterConstantsTest,
// ClusterConstantsFailoverTest, and ClusterConstantsMixedTest assert the same
// goldens on the Java side.
package clusterjava
