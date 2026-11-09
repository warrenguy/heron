// Package soakrun builds a seeded fault schedule and checks a live soak.
//
// The schedule names roles. The caller resolves a role to a member. Rules are
// nftables bridge commands. The caller runs them.
package soakrun
