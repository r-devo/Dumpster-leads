# Dumpster Rental Lead Intelligence

Ethically sourced public data → daily dumpster rental lead intelligence.

---

## Overview

This project is designed to identify **new, high-intent dumpster rental leads** by monitoring publicly available web sources that indicate:
- Construction starts
- Renovations
- Cleanouts
- Demolition
- Commercial or residential projects likely to require dumpster services

The goal is to generate **daily actionable lead lists** while avoiding duplicates and respecting ethical data sourcing practices.

---

## Data Sources (Public & Ethical)

This system pulls from **publicly accessible information only**, including:

- Building permits
- Planning & zoning notices
- Municipal open data portals
- Public project announcements
- Classified ads and postings (where permitted)
- Publicly indexed contractor postings

❌ No private data  
❌ No login-gated content  
❌ No scraping behind paywalls  

---

## Core Logic

Each daily run:
1. Collects new public postings
2. Normalizes location + project metadata
3. Filters for dumpster-relevant signals
4. Deduplicates against prior days
5. Outputs a clean daily lead list

Key attributes per lead:
- Address or project location
- Project type
- Source
- Date first observed
- Confidence score (future)

---

## Output

Daily output is structured for:
- SMS outreach
- CRM ingestion
- Manual review
- API consumption (future)

Formats:
- CSV
- JSON
- Plain text (human-readable)

---

## Ethics & Compliance

This project is built with:
- Respect for public data terms
- Conservative crawl rates
- Clear source attribution
- Opt-out compliance if required

The intent is **signal discovery**, not data exploitation.

---

## Status

🚧 Initial repository setup  
🚧 Logic being documented  
🚧 Automation pending

---

## Next Steps

- Document daily run workflow
- Add source-specific collectors
- Define deduplication rules
- Implement scoring heuristics
- Build notification layer
