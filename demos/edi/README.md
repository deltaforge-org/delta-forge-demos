# EDI & Supply Chain Demos

SQL demos for parsing Electronic Data Interchange standards directly in SQL. All queries run inside the DeltaForge GUI with built-in assertions.

## What's Covered

- **HIPAA X12** -- Claims (837), remittance (835), eligibility (270/271), claim status (276/277)
- **X12 Supply Chain** -- Purchase orders, transportation logistics, order lifecycle tracking
- **EDIFACT** -- International trade, customs/border declarations, invoice reconciliation
- **TRADACOMS** -- UK retail purchase orders, utility billing, JSON deep access
- **EANCOM** -- Retail supply chain messaging (ORDERS/DESADV/INVOIC)
- **Advanced** -- Repeating segment handling, JSON segment extraction, compliance validation

## Running a Demo

1. Open the DeltaForge GUI
2. Select a demo from this category
3. Run **setup.sql** to create tables and load seed data
4. Step through **queries.sql** -- assertions verify each result
5. Run **cleanup.sql** to tear down
