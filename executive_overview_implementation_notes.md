# Executive Overview Implementation Notes

## Measures Used
- Total Revenue: `_Measures[Gross Sales]`
- Net Revenue: `_Measures[Net Revenue]`
- Total Visits: `_Measures[Total Visits]`
- Unique Clients: `_Measures[Total Clients]`
- New Clients: `_Measures[New Clients]`
- Returning Clients: `_Measures[Returning Clients]`
- Average Ticket: `_Measures[Avg Transaction Value]`
- Revenue per Visit: `_Measures[Revenue per Visit]`

## Fields Used
- Date slicer/trends: `Date[CalendarDate]`
- Revenue category slicer/breakdown: `'Purchase Item'[RevenueCategory]`
- Purchase item slicer/breakdown: `'Purchase Item'[PurchaseItemName]`
- Visits by service: `Service[ServiceName]` with `_Measures[Total Visits]`

## Visuals Created
- 8 KPI cards across the top row.
- Revenue Trend line chart using `Date[CalendarDate]` and `_Measures[Net Revenue]`.
- Visits Trend line chart using `Date[CalendarDate]` and `_Measures[Total Visits]`.
- Client Mix Trend line chart using `Date[CalendarDate]`, `_Measures[New Clients]`, and `_Measures[Returning Clients]`.
- Revenue by Revenue Category column chart.
- Top Purchase Items bar chart sorted by net revenue.
- Visits by Service bar chart.
- Slicers for Date, Revenue Category, and Purchase Item.

## Layout Decisions
- 16:9 canvas, 1280 x 720.
- Slicers are placed at the top for executive filtering.
- KPI cards sit directly under slicers for first-glance performance.
- Trend visuals occupy the middle band.
- Breakdown visuals occupy the bottom band.

## Assumptions
- The prior Revenue by Service requirement was intentionally removed.
- Service is retained as an operational visit breakdown, not as revenue attribution.
- Service and Staff slicers were omitted because they do not reliably filter purchase revenue KPIs in the current semantic model.
- Revenue visuals use `_Measures[Net Revenue]` unless explicitly labeled Total Revenue.

## Missing Measures
None of the required KPI measures are missing.

## Fabric Validation Instructions
1. Sync the repo to Fabric.
2. Open the existing `Executive Overview` report artifact.
3. Confirm `definition.pbir` still points to `../../Salt Analytics.SemanticModel`.
4. Confirm the report theme is `saltroom_theme`.
5. Verify each visual renders and that slicers affect the intended Date and Purchase Item / Revenue Category scoped visuals.
6. Confirm no new semantic model or dataset binding was created.
