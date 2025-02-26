Flexy: Execution Block-Based Optimization Framework for RDOS

Overview

Flexy is an optimization framework designed for imperative program execution in Relational Database Orchestration Systems (RDOS). It introduces Execution Blocks (EBs) to segment workloads dynamically, ensuring efficient resource utilization, load balancing, and replica allocation in containerized database environments.

This repository provides the implementation of our proposed optimization framework, along with scripts and models to reproduce our experiments.

Implementation Details
1. Control Plane & Task Invocation
The Flexy framework runs on the control plane, interacting with worker nodes and databases via vendor-specific APIs. It gathers runtime statistics using DBMS profiling tools such as EXPLAIN to analyze execution costs.

2. Execution Block (EB) Identification & Logical Planning
Upon task invocation, Flexy generates logical plans for all workload programs. EBs are identified using our cost model, which is available in the Cost_model folder. The logical plans are then transformed into generic physical execution plans based on EB templates.
Files: Cost_model → Contains scripts for EB segmentation and cost evaluation.

3. Load Balancing & Replica Allocation (LP Model Implementation)
Flexy optimizes load balancing and replica allocation using a Linear Programming (LP) model implemented in CPLEX (version 20.1.0.0). The LP model is integrated with Python using docplex.mp.model. The optimal routing and replica allocation decisions are stored for execution scheduling.
Files: LP_Model → Implements LP-based load balancing and replica allocation using CPLEX.

4. Fragment Schema Generation & Replica Execution
Using the replica allocation results, Flexy creates fragment schemas to store only the necessary data in each container. Fragment schemas are immediately executed on worker nodes to ensure replica availability before EB execution.
Files: LP_Model → provide information for Storing replica allocation results.


5. Execution Block Dispatch & Parallel Execution
Flexy dispatches workload programs in parallel using threads to maximize both program-level and EB-level parallelism. Execution Blocks (EBs) are routed based on the LP model's replica and workload allocation decisions.
Files: SampleDispatch → Contains scripts demonstrating EB dispatch using routing and replica allocation information from the LP model.


Workload Programs
The workloads used in our experiments are available in the Workload_Programs folder.
We designed workloads representing real-world RDOS environments, including CPU-bound, memory-bound, and mixed workloads.
