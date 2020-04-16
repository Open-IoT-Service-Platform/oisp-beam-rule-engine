# OISP Beam Rule Engine

This repository contains the OISP component Rule Engine. The implementation thereof is in the `src` directory.

The job is submitted to a Flink cluster at `http://flink-jobmanager-rest:8081`. This is done by the deployer app, which is found in the `deployer` directory.

The `Dockerfile` contains a multi-stage build, the first step build the rule engine, and the final image deploys the jar to Flink.
