# PRD: Personal Deal Flow Engine
**Product Type:** Private intelligence engine for relationship-driven dealmaking  
**Version:** v1  
**Status:** Draft  
**Owner:** Nikolai / Altitude  
**Last Updated:** 2026-03-18

---

## 1. Summary

The Personal Deal Flow Engine is a private, single-player system that ingests relationship activity and deal context, then outputs the highest-leverage people to contact each day, why now, and what to say.

The first version is engine-only. It stays hidden behind interfaces the user already uses, with no standalone product UI.

It is designed for relationship-heavy industries, starting with real estate private equity.

The product is not a CRM, marketplace, collaboration suite, or generalized knowledge OS. It is a decision engine over a private network.

---

## 2. Problem

Real estate private equity teams do not primarily fail from lack of data. They fail from fragmented memory, poor prioritization, weak follow-up timing, and inability to synthesize many small signals into clear next actions.

Current systems are weak:
- CRMs require manual entry and decay quickly
- Market data tools show what exists, not who to call now
- Inbox and calendar history are not operationalized
- Relationship context is trapped in heads, inboxes, and scattered notes

As a result:
- High-value relationships go cold
- Relevant contacts are missed at the right moment
- Deals move slower than necessary
- Team members operate on intuition without a durable memory layer

---

## 3. Vision

Create a private system that converts relationship history plus active deal context into daily action recommendations.

The core output is:

1. Who should I talk to today?
2. Why now?
3. What opportunity is attached?
4. What should I say?

---

## 4. Product Thesis

The winning product in relationship-driven industries is not a network that requires many users. It is a private intelligence engine that provides immediate value to one user or one firm.

This product must:
- work with one user
- require near-zero manual entry
- generate clear actions, not dashboards
- improve as private data accumulates
- align with industries that prefer private edge over shared networks

---

## 5. Goals

### Primary goals
- Increase quality and frequency of high-value relationship touchpoints
- Reduce missed follow-ups and forgotten context
- Improve matching between live deals and relevant contacts
- Compress decision time for outreach

### Secondary goals
- Build a durable private memory layer over a network
- Surface hidden leverage in weak or dormant ties
- Create a foundation for future deal and capital matching

---

## 6. Non-goals

v1 will not:
- replace a full CRM
- provide shared industry-wide network effects
- function as a team collaboration suite
- provide CoStar-like market data coverage
- autonomously send messages without user review
- attempt full enterprise workflow orchestration
- expose a standalone web application or new workflow destination for the user

---

## 7. Target Users

### Primary user
Relationship-driven principal, founder, broker, investor, or operator who manages many contacts and opportunities through email, calls, meetings, and scattered notes.

### Initial ICP
Real estate private equity professionals with:
- active deal sourcing
- recurring broker and capital relationships
- fragmented memory across inbox, calendar, and notes
- skepticism toward manual CRM work
- high value placed on private edge

---

## 8. Jobs to Be Done

### Functional jobs
- Tell me who I should contact today
- Remind me what matters about this person
- Match relevant people to current deals
- Draft a strong follow-up with context
- Surface warm paths and dormant relationships

### Emotional jobs
- Reduce fear of missing important people
- Create confidence before meetings
- Replace vague guilt about follow-ups with clear action
- Make the user feel more prepared and more dangerous

---

## 9. User Stories

- As a principal, I want a ranked daily list of people to contact so I can focus on high-leverage outreach first.
- As an investor, I want each recommendation to include why now so I trust the ranking.
- As an operator, I want relationship context before calls so I do not need to search old threads.
- As a dealmaker, I want current deals matched to relevant contacts so I can move faster.
- As a busy user, I want the system to work with minimal manual input.

---

## 10. Core User Experience

### Daily loop
1. System ingests new email, calendar, and note activity
2. System updates profiles, entities, and relationship scores
3. System evaluates active deals against contact profiles
4. System ranks the top 5 to 10 highest-leverage outreach opportunities
5. User receives a daily brief
6. User reviews recommendations and optionally uses message drafts

### Recommendation card must answer
- Who is this?
- Why now?
- What deal or topic is relevant?
- What do I know about them?
- What should I say next?

---

## 11. Scope for v1

### Included
- Email ingestion
- Calendar ingestion
- Manual deal table ingestion
- Minimal notes ingestion
- Entity extraction
- Contact profile generation
- Relationship scoring
- Daily ranked recommendations
- Draft message generation
- Daily email delivery with print-optimized PDF attachment

### Excluded
- Full telephony integration
- LinkedIn scraping
- Automatic send
- Team-wide permissions model
- Extensive reporting dashboards
- Advanced graph visualization as a core experience

---

## 12. Functional Requirements

### FR1. Ingest communication activity
The system shall ingest:
- inbound and outbound email metadata and body text
- meeting titles, participants, and timestamps from calendar
- user-entered notes linked to people or deals
- basic deal objects from a structured table

### FR2. Extract entities and signals
From each interaction, the system shall extract:
- people
- company
- deal references
- market/geography
- capital amount if present
- timing references
- intent signals
- next-step cues

### FR3. Maintain contact profiles
For each person, the system shall maintain:
- name
- company
- role if known
- interaction timeline
- last contact date
- topics discussed
- inferred interests
- responsiveness signals
- associated deals
- relationship strength score

### FR4. Maintain deal objects
For each deal, the system shall store:
- name or identifier
- geography
- asset type
- stage
- target size
- risk/strategy tags
- key contacts linked to it

### FR5. Score outreach priority
The system shall rank contacts using a scoring model based on:
- recency since last touch
- relationship strength
- current deal relevance
- signal strength from recent interactions
- user importance tagging if available

### FR6. Generate daily recommendations
The system shall produce a ranked daily list with:
- contact
- priority score
- why now summary
- related deal or opportunity
- suggested action
- optional message draft

### FR7. Support reviewable message drafts
The system shall generate:
- short follow-up email draft
- soft re-engagement draft
- deal update draft
- intro request draft

All drafts must require user review before sending.

### FR8. Provide relationship memory before meetings
When a meeting is upcoming, the system shall generate a pre-meeting brief with:
- who the person is
- recent interaction history
- topics previously discussed
- relevant open opportunities
- recommended talking points

---

## 13. Non-functional Requirements

- Low-friction onboarding
- Minimal manual data entry
- Secure private storage
- Fast daily processing
- Interpretable recommendations
- Easy export of core data
- Architecture must support self-hosting

---

## 14. Data Model

### Core entities
#### Person
- person_id
- full_name
- email
- company_id
- title
- last_contact_at
- relationship_strength
- responsiveness_score
- priority_override
- tags

#### Company
- company_id
- name
- type
- geography
- notes

#### Deal
- deal_id
- name
- market
- asset_type
- size
- stage
- strategy_tags
- status
- owner_user_id

#### Interaction
- interaction_id
- type (email, meeting, note)
- timestamp
- direction
- participants
- company_refs
- deal_refs
- summary
- extracted_signals

#### Recommendation
- recommendation_id
- date
- person_id
- related_deal_id
- priority_score
- why_now
- suggested_action
- draft_text
- status

---

## 15. Minimal Ontology

### Entities
- Person
- Company
- Deal
- Interaction

### Relationships
- Person works_at Company
- Person knows Person
- Person interested_in Deal
- Person invested_in Deal
- Interaction involves Person
- Interaction references Deal
- Deal associated_with Company

This ontology is internal only. It should not be part of user-facing positioning.

---

## 16. Scoring Model

### Objective
Rank contacts by expected leverage of contacting them now.

### Initial weighted factors
- Time since last meaningful interaction: 20%
- Relationship strength: 20%
- Match to active deals: 25%
- Presence of recent intent or timing signal: 20%
- User-defined strategic importance: 15%

### Example signals
- “looking to deploy capital”
- “active in Boston”
- “circling back next quarter”
- “send me updates”
- recent inbound engagement
- upcoming meeting
- long period of silence with strong historic relationship

### Output bands
- 80 to 100: contact today
- 60 to 79: contact this week
- 40 to 59: monitor
- below 40: no action

---

## 17. Recommendation Output Format

Each recommendation shall include:

**Contact**  
Name, company, role

**Why now**  
1 to 3 sentence explanation of timing and relevance

**Context**  
Last interaction date, major prior topic, linked deal if any

**Suggested action**  
One clear next step:
- email
- call
- send update
- request intro
- schedule meeting

**Draft**  
A ready-to-edit message draft in the user’s tone

---

## 18. UX Requirements

### Primary interface
The first version shall be delivered only through interfaces the user already uses daily.

Primary delivery method:
- daily email with PDF attachment optimized for printing

Optional secondary delivery later:
- plain-text or HTML email summary in body

### UI principles
- no standalone web app in v1
- no new daily destination for the user to check
- ranking first
- action first
- reason transparency
- print readability
- clean page structure for physical review and annotation

### Output sections in the PDF
1. Daily Recommendations
2. Upcoming Meeting Briefs
3. Active Deals and Matching Contacts
4. Short relationship memory notes where relevant

---

## 18A. PDF Delivery Requirements

The daily PDF is the user-facing artifact in v1.

### Requirements
- optimized for printing on standard office paper
- clear section hierarchy
- minimal visual noise
- concise summaries with enough context for action
- readable in both digital and printed form
- suitable for quick morning review

### Recommended structure
1. Header with date, user, and total recommendations
2. Top 5 contacts to prioritize today
3. Upcoming meetings with memory briefs
4. Deal-to-contact matching section
5. Low-priority watchlist if space allows

### Formatting principles
- strong typography hierarchy
- short paragraphs
- clear whitespace between recommendation cards
- avoid dependency on color for meaning
- print-safe layout in grayscale
- each recommendation should fit within a compact card or block

## 19. System Architecture

### Suggested v1 stack
- n8n for ingestion and workflow orchestration
- Postgres for structured storage
- LLM API for extraction, summarization, and draft generation
- Gmail and Calendar APIs for data ingestion
- email generation and PDF rendering layer

### Processing flow
1. Trigger on new email or scheduled sync
2. Parse and normalize interaction
3. Extract entities and signals
4. Update database
5. Recompute affected contact and deal scores
6. Run daily ranking job
7. Generate recommendation artifacts
8. Render print-optimized PDF and deliver by email

---

## 20. Success Metrics

### Primary metrics
- weekly number of recommended contacts actually contacted
- reply rate on recommended outreach
- number of deals influenced by recommended outreach
- user-reported usefulness score of daily brief

### Secondary metrics
- reduction in stale contacts
- percent of contacts with generated profiles
- time saved preparing for meetings
- number of active deals with at least one matched contact

---

## 21. MVP Definition

The MVP is successful if a single user can connect email and calendar, load active deals, and receive a daily ranked list of at least 5 useful outreach recommendations with acceptable drafts.

### MVP acceptance criteria
- data sync runs reliably each day
- top recommendations are interpretable
- at least 60% of top 5 recommendations are judged useful by the user
- message drafts require only light editing
- onboarding time under 90 minutes
- PDF output is readable when printed in standard office format

---

## 22. Rollout Plan

### Phase 1
Private internal use by one user

### Phase 2
Single-firm pilot with 2 to 5 users and shared deal table, but private relationship memory by default

### Phase 3
Selective team intelligence features, subject to trust and permissions constraints

---

## 23. Key Risks

### Risk 1. Low trust in recommendations
Mitigation:
- show clear “why now”
- allow feedback on each recommendation
- keep model interpretable

### Risk 2. Data quality is messy
Mitigation:
- start with email and calendar only
- prioritize simple extraction
- support manual correction on high-value contacts

### Risk 3. Too much manual setup
Mitigation:
- default schemas
- fast onboarding
- import from existing spreadsheets if needed

### Risk 4. Product drifts into CRM
Mitigation:
- protect action-first UX
- avoid heavy data-entry workflows
- do not prioritize pipeline reporting over recommendations

### Risk 5. Privacy concerns
Mitigation:
- self-hostable architecture
- explicit control over data sources
- no shared network dependency in v1

---

## 24. Strategic Positioning

### Internal truth
This product uses ontology, memory, ranking, and AI reasoning to build a private network intelligence layer.

### External positioning
Never sell it as:
- ontology
- knowledge graph
- relationship OS
- CRM replacement

Sell it as:
- who to call today
- why now
- what to say
- which deal is attached

---

## 25. Future Extensions

After v1 proves daily value, add:
- external signals and news enrichment
- capital profile matching
- broker and owner activity scoring
- warm intro path suggestions
- auto-generated weekly relationship reviews
- firm-wide intelligence with permissions
- optional integration with market data systems

---

## 26. Open Questions

- What minimum signal set most improves recommendation quality?
- Should relationship strength be fully automated or partially user-corrected?
- Should deals remain manual in v1 or support semi-automatic creation from inbox signals?
- What PDF layout best supports rapid executive review and handwritten annotation?
- Which user segment will feel pain most sharply first: principal, acquisitions lead, or capital raiser?

---

## 27. Final Product Statement

The Personal Deal Flow Engine is a private decision system that turns fragmented relationship activity into a daily ranked list of high-leverage actions.

Its job is simple:

Tell the user who to contact, why now, and what to say.

If it does that well, it creates durable edge without requiring network effects, shared adoption, or heavy manual CRM behavior.
