This file serves as an initial draft of ideas that will evolve during development, based on the project brief - in particular the **Extensions/Questions** and **Notes** sections.

# Brainstorm Notes

## Core Requirement

- API to storage pipeline
- OpenMeteo as example source

## "Abstractions to make process generic"

- API client not locked to OpenMeteo - should support swapping data sources
- Writer not locked to one format - Parquet, database, cloud storage, etc.
- Config-driven parameters - locations, intervals, variables without code changes

## "Orchestration in production"

- Familiarity with orchestration tools and DAG structures
- Infrastructure constraints - how to demonstrate orchestration without full setup?
- Production concerns - retry handling, failure recovery, backfills

## "State at time of API call might be useful"

- Ingestion timestamp - when did we call the API?
- API response metadata - what did the API tell us about the response?
- Domain context - why might capturing state matter for this type of data?

## "APIs might produce a lot of data"

- Storage format considerations
- Partitioning strategy
- Scalability over time
- Query patterns - how will this data be consumed?
- File size management

## "Make evaluation easier"

- One-command run
- Clear README
- Sample output
- Documentation of design decisions
- Tests
