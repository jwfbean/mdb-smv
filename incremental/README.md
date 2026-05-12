# Incremental Materialized Views in MongoDB

An **incremental materialized view** is a pre-computed aggregation stored as a real collection on disk. Unlike a standard view (which re-runs its pipeline on every read), a materialized view is refreshed on demand and queried directly — making reads fast regardless of source data volume.

This example uses MongoDB's `$merge` aggregation stage to maintain a `monthlybakesales` summary collection derived from a `bakesales` transaction log. Only records newer than a given `startDate` are reprocessed, so the cost of each refresh scales with the volume of *new* data rather than the full dataset.

---

## How it works

The refresh pipeline has three stages:

```
bakesales  ──$match──►  ──$group──►  ──$merge──►  monthlybakesales
             (new data)   (by month)   (upsert)
```

| Stage | Purpose |
|-------|---------|
| `$match` | Filters `bakesales` to only documents on or after `startDate`, skipping months that haven't changed |
| `$group` | Rolls up individual transactions into one document per calendar month, summing `quantity` and `amount` |
| `$merge` | Writes results into `monthlybakesales`. `whenMatched: "replace"` replaces an existing month document with fresh totals; unmatched months are inserted automatically |

Because `$match` runs before `$group`, MongoDB can use an index on `date` to avoid a full collection scan on each refresh.

---

## Collections

| Collection | Role |
|------------|------|
| `bakesales` | Source — individual sale transactions |
| `monthlybakesales` | Materialized view — one document per calendar month |

### `bakesales` document shape

```json
{
  "date":     ISODate("2019-01-28"),
  "item":     "Cake - Chocolate",
  "quantity": 3,
  "amount":   Decimal128("90")
}
```

### `monthlybakesales` document shape

```json
{
  "_id":            "2019-01",
  "sales_quantity": 94,
  "sales_amount":   Decimal128("912")
}
```

---

## Step-by-step walkthrough

### Step 1 — Insert the initial source data

Load the first batch of sales records (December 2018 and January 2019) into the `bakesales` collection.

Using `mongoimport`:

```bash
mongoimport \
  --uri "<connection-string>" \
  --db bakery \
  --collection bakesales \
  --jsonArray \
  --file incremental/data/initial_sales.json
```

Or in `mongosh`:

```js
const initialSales = [/* contents of initial_sales.json */];
db.bakesales.insertMany(initialSales);
```

After this step, `bakesales` contains 12 documents spanning 2018-12 and 2019-01.

---

### Step 2 — Run the initial full refresh

Open `mongosh` connected to your cluster and load the refresh function:

```js
load("incremental/pipelines/refresh.js");
```

Run a full refresh by passing epoch as the start date (matches every document):

```js
updateMonthlySales(new ISODate("1970-01-01"));
```

`monthlybakesales` is created automatically by `$merge` if it doesn't exist yet. After this step it contains:

```json
{ "_id": "2018-12", "sales_quantity": 48, "sales_amount": Decimal128("755") }
{ "_id": "2019-01", "sales_quantity": 63, "sales_amount": Decimal128("630") }
```

Verify:

```js
db.monthlybakesales.find().sort({ _id: 1 });
```

---

### Step 3 — Insert new source data

Simulate new sales arriving — additional January records and the first February records:

Using `mongoimport`:

```bash
mongoimport \
  --uri "<connection-string>" \
  --db bakery \
  --collection bakesales \
  --jsonArray \
  --file incremental/data/incremental_sales.json
```

Or in `mongosh`:

```js
const incrementalSales = [/* contents of incremental_sales.json */];
db.bakesales.insertMany(incrementalSales);
```

After this step, `bakesales` contains 23 documents.

---

### Step 4 — Run an incremental refresh

Because the new data only touches January 2019 and later, pass `2019-01-01` as the start date. December's document in `monthlybakesales` will not be touched at all.

```js
updateMonthlySales(new ISODate("2019-01-01"));
```

`monthlybakesales` now contains three documents:

```json
{ "_id": "2018-12", "sales_quantity": 48,  "sales_amount": Decimal128("755") }  // unchanged
{ "_id": "2019-01", "sales_quantity": 94,  "sales_amount": Decimal128("912") }  // updated
{ "_id": "2019-02", "sales_quantity": 24,  "sales_amount": Decimal128("226") }  // inserted
```

Verify:

```js
db.monthlybakesales.find().sort({ _id: 1 });
```

---

## Choosing the right `startDate`

The `startDate` is the key to making refreshes efficient. The right value depends on your data and update cadence:

| Scenario | Recommended `startDate` |
|----------|------------------------|
| First-time full build | `ISODate("1970-01-01")` (epoch) |
| Daily job | Start of the previous day — covers late-arriving records |
| Event-driven (on insert) | Start of the current month — only the active month can change |
| After a data correction | Start of the earliest corrected month |

The tradeoff: a wider window is safer (catches late-arriving data) but reprocesses more. A tighter window is faster but may miss corrections to older records.

---

## Key properties of this pattern

**What `$merge` gives you that `$out` does not:**
- Selective upsert — only affected months are written; others are untouched
- Change stream support — you can watch `monthlybakesales` for updates
- Output to a different database than the source

**Limitations:**
- The view is only as fresh as the last refresh call — it is not updated in real time
- The caller must track and supply the correct `startDate`; there is no built-in bookmarking
- Late-arriving data (records inserted with a past `date`) requires re-running with an earlier `startDate`

For continuous, real-time updates, see the Atlas Stream Processing approach in [../streaming/README.md](../streaming/README.md).

---

## File reference

```
incremental/
├── data/
│   ├── initial_sales.json       # First 12 bakesales documents (2018-12, 2019-01)
│   └── incremental_sales.json   # Next 11 documents (late 2019-01, 2019-02)
└── pipelines/
    └── refresh.js               # updateMonthlySales(startDate) function for mongosh
```
