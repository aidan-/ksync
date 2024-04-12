# ksync

`cli-utils`'s `kapply` implementation with some additional features:
- Support for sync-waves driven by resource annotations.


## Goal
`kapply` validates the existence of `Kind`s during apply time.  This is done up front to identify namespace vs non-namespaced resources to determine if default namespace values need to be applied.

This strict requirement also helps determine apply time ordering by applying CRDs before CRs.

A problem occurs in this situation, when trying to execute a single monolithic `kapply` execution for an entire cluster and certain applications that create CRDs in-cluster on Pod deployment (ie, Cilium).  In this situation, `kapply` will fail to apply the CRs because the CRD definitions are not available within the plan phase.  This could be solved by breaking the `kapply` execution into multiple bespoke invocations (optionally with separate inventories).  Or, through the implementation of sync-waves.

## Design
Sync-waves are a way to group resources together that should be applied together.  This is done by annotating resources with a `sync-wave` annotation.  The value of the annotation is a string that represents the wave number.  Resources with the same wave number will be applied together.

Sync-waves are implemented as an abstraction on top of `kapply`s apply mechanism.
