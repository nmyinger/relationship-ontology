# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Personal Deal Flow Engine** — a private, single-player intelligence system for relationship-driven dealmaking in real estate private equity. It ingests relationship activity (email, calendar, deal data) and outputs daily ranked recommendations of who to contact, with reasons and draft messages. It is a decision engine, not a CRM.

## Current Status

Pre-implementation. The project consists of a product requirements document (`personal_deal_flow_engine_prd_v2.md`) defining the full specification. No source code, build system, or tests exist yet.

## Planned Architecture

- **Workflow Orchestration:** n8n
- **Database:** Postgres
- **AI/Extraction:** LLM API (entity extraction, message drafting)
- **Data Sources:** Gmail API, Google Calendar API
- **Output:** Daily email with print-optimized PDF

## Core Data Model

Five entities: **Person**, **Company**, **Deal**, **Interaction**, **Recommendation**. The scoring model weights: deal relevance (25%), time since last interaction (20%), relationship strength (20%), recent intent signals (20%), user-defined importance (15%).

## Design Principles

- Privacy-first: self-hostable, no third-party data sharing
- Action-first UX: every output should drive a specific next action
- Minimal manual data entry: automate ingestion from existing tools
- Interpretable recommendations: always show why someone is recommended
