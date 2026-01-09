# Data Health M&M

**Case Review: Silent Payer Feed Degradation**

## Case Summary

Over a three-week period, commercial payer encounters were undercounted by approximately 8%. The issue was not identified until month-end reconciliation.

No dashboards broke. No alerts fired. Decisions were made using incomplete data.

---

## Timeline

- **Day 0:** Upstream payer interface change deployed
    
- **Day 1–21:** Volumes drift downward within visually plausible range
    
- **Day 22:** Finance flags discrepancy during reconciliation
    
- **Day 24:** Root cause identified (filter condition change)
    

---

## Impact

- Executive utilization reporting understated
    
- Payer mix analysis distorted
    
- Two operational decisions delayed pending clarification
    
- Trust erosion across teams
    

---

## Why this was missed

- No statistical baseline for expected payer volumes
    
- Human review relied on visual inspection
    
- Decline occurred gradually, not as a step change
    
- No explicit ownership for payer-level completeness
    

---

## How MVSM would have helped

- Volume control chart would have flagged deviation on Day 3
    
- Distributional drift check would have identified payer-specific change
    
- Alert would have triggered review before decisions were affected
    

---

## Contributing Factors (No Blame)

- Interface change not communicated downstream
    
- Historical seasonality not documented
    
- Informal reliance on “this looks normal”
    

---

## Corrective Actions

1. Add payer-level volume monitoring
    
2. Assign data owner for payer feeds
    
3. Include interface changes in release checklist
    
4. Review alert thresholds quarterly
    

---

## Key Takeaways

- Silent failures are the most dangerous failures
    
- Visual plausibility is not validation
    
- Early detection preserves trust and decision quality
    

**Focus is on learning and prevention, not fault.**