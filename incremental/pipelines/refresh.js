// Incremental materialized view refresh
//
// Pass a startDate to limit reprocessing to months that may have changed.
// For a full rebuild, use: updateMonthlySales(new ISODate("1970-01-01"))
// For an incremental refresh starting from a known cutoff, e.g.:
//   updateMonthlySales(new ISODate("2019-01-01"))
//
// $merge with whenMatched: "replace" upserts each grouped month:
//   - Existing months are replaced with fresh totals.
//   - New months are inserted.
//   - Months before startDate are left untouched.

function updateMonthlySales(startDate) {
  db.bakesales.aggregate([
    {
      $match: {
        date: { $gte: startDate }
      }
    },
    {
      $group: {
        _id: { $dateToString: { format: "%Y-%m", date: "$date" } },
        sales_quantity: { $sum: "$quantity" },
        sales_amount:   { $sum: "$amount"   }
      }
    },
    {
      $merge: {
        into: "monthlybakesales",
        whenMatched: "replace"
      }
    }
  ]);
}
