# 2. design goals

Date: 2024-12-11

## Status

Accepted

## Context

Understanding the high level goals of the project is important information for contributors and users alike. This
allows us to focus on what is important and not get side tracked with implementing features which clash with the
primary goals of the project.

## Decision

The primary goal of the project is to implement an LSM based key value store which is intended to be used
for OLTP workloads backed by object storage systems like S3. 

Primary feature set includes
- Support for [Strict Serializability](https://jepsen.io/consistency/models/strong-serializable) - Which 
  allows users to have the strongest consistency guarantees, ensuring that transactions appear to execute 
  atomically and in a total order that respects real-time ordering of operations.
- Support for Range Queries - Allows a user to search for all keys starting from a certain point and 
  ending at another, effectively retrieving a continuous sequence of data based on key order.
- Support for a Merge Operator - Allows a user to build efficient streaming operations through the use 
  of Atomic Read-Modify-Write operations on key values.

### Configurability
Since slateDB is primarily backed by object storage systems, it will provide configurable options to
tune latency, throughput and durability depending on the user workload.

### Expected use case
For golang, we expect slateDB will be used for event processing and event capture workloads. As such, 
much of the design goals revolves around solving for that use case.

TODO: Other expected use cases?

## Consequences

SlateDB is not intended to solve write synchronization, nor provide a network protocol for interacting
with SlateDB, nor support long-running transactions for use in non database synchronization. Users will need 
to implement multi-client synchronization and partitioning schemes which make sense for their workloads and 
achieve their throughput and availability goals.
