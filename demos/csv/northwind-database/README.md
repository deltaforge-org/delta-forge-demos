# Northwind Trading Company Demo

## Overview

The Northwind database is a classic sample dataset representing a fictional specialty
food trading company. It contains 11 interconnected CSV files covering the full
business domain: customers, orders, products, employees, suppliers, and shipping.

This demo focuses on **cross-table queries** — joins, aggregations, and business
analytics across multiple relational tables.

## What This Demo Sets Up

### Infrastructure

| Resource | Name | Description |
| -------- | ---- | ----------- |
| Zone | `external` | Shared namespace for all external/demo tables |
| Schema | `csv` | CSV-backed external tables |
| Role | `northwind_reader` | Read-only access to Northwind data |

### Naming Convention

All objects use 3-part fully qualified names: `external.format.table`

- **Zone** = `external` (shared across all demos)
- **Schema** = `csv` (the file format)
- **Table** = object name (e.g. `customers`, `orders`)

### Tables Created (in `external.csv` schema)

| Table | Records | Description |
| ----- | ------- | ----------- |
| `external.csv.customers` | 91 | Customer companies with contact details |
| `external.csv.employees` | 9 | Sales employees with reporting hierarchy |
| `external.csv.orders` | 830 | Customer orders with shipping details |
| `external.csv.order_details` | 2,155 | Line items linking orders to products |
| `external.csv.products` | 77 | Product catalog with pricing and stock |
| `external.csv.categories` | 8 | Product categories |
| `external.csv.suppliers` | 29 | Product suppliers with contact info |
| `external.csv.shippers` | 3 | Shipping companies |
| `external.csv.regions` | 4 | Geographic sales regions |
| `external.csv.territories` | 53 | Sales territories |
| `external.csv.employee_territories` | 49 | Employee-to-territory assignments |

### Permissions Granted

- `northwind_reader` role gets `USAGE` on the `external.csv` schema
- `northwind_reader` role gets `SELECT` on all 11 tables
- The role is automatically assigned to the user who installs the demo

### Relationships

```text
customers ──< orders ──< order_details >── products >── categories
                │                              │
                └── employees                  └── suppliers
                      │
                      └──< employee_territories >── territories >── regions
```

### Data Format

- **Format**: CSV with semicolon (`;`) delimiter
- **Headers**: First row contains column names
- **Encoding**: UTF-8

## File Structure

```text
northwind-database/
├── demo.toml               Metadata configuration
├── README.md                This file
├── setup.sql                Full setup — all 11 tables at once
├── cleanup.sql              Full teardown — removes all objects
├── queries.sql              10 cross-table demo queries
└── data/                    CSV data files (semicolon-delimited)
    ├── customers.csv
    ├── employees.csv
    ├── orders.csv
    ├── order_details.csv
    ├── products.csv
    ├── categories.csv
    ├── suppliers.csv
    ├── shippers.csv
    ├── regions.csv
    ├── territories.csv
    └── employee_territories.csv
```

## Sample Queries

After running `setup.sql`, see `queries.sql` for 10 ready-to-run cross-table
queries. Here are a few highlights:

```sql
-- Top 10 customers by total order value
SELECT
    c.companyName,
    COUNT(DISTINCT o.orderID) AS order_count,
    ROUND(SUM(od.unitPrice * od.quantity * (1 - od.discount)), 2) AS total_value
FROM external.csv.customers c
JOIN external.csv.orders o ON c.customerID = o.customerID
JOIN external.csv.order_details od ON o.orderID = od.orderID
GROUP BY c.companyName
ORDER BY total_value DESC
LIMIT 10;

-- Revenue by product category
SELECT
    cat.categoryName,
    COUNT(DISTINCT p.productID) AS product_count,
    ROUND(SUM(od.unitPrice * od.quantity * (1 - od.discount)), 2) AS total_revenue
FROM external.csv.order_details od
JOIN external.csv.products p ON od.productID = p.productID
JOIN external.csv.categories cat ON p.categoryID = cat.categoryID
GROUP BY cat.categoryName
ORDER BY total_revenue DESC;

-- Employee territory coverage (4-way join)
SELECT
    e.firstName || ' ' || e.lastName AS employee_name,
    r.regionDescription AS region,
    COUNT(t.territoryID) AS territory_count
FROM external.csv.employees e
JOIN external.csv.employee_territories et ON e.employeeID = et.employeeID
JOIN external.csv.territories t ON et.territoryID = t.territoryID
JOIN external.csv.regions r ON t.regionID = r.regionID
GROUP BY e.firstName, e.lastName, r.regionDescription
ORDER BY employee_name, region;
```

## Cleanup

Run `cleanup.sql` to remove all objects created by this demo. It revokes
permissions, drops schema columns, tables, the role, and optionally the
shared schema and zone (safe even if other demos are using them).

## Data Source

Based on the classic Microsoft Northwind sample database, adapted to CSV format.
