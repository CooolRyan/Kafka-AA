const SOURCE_NAME = "Kafka Failover Summary";
const TABLE = "failover_test_summary";

const existing = db.sources.findOne({ name: SOURCE_NAME });
if (existing) {
  print("already exists: " + SOURCE_NAME);
  quit(0);
}

const template = db.sources.findOne({ name: "Kafka Mirror Messages" });
if (!template) {
  print("ERROR: template Kafka Mirror Messages not found");
  quit(1);
}

db.sources.insertOne({
  name: SOURCE_NAME,
  kind: "log",
  from: { databaseName: "default", tableName: TABLE },
  timestampValueExpression: "ts",
  displayedTimestampValueExpression: "ts",
  implicitColumnExpression:
    "concat(run_id, ' missing=', toString(missing_count), ' dup=', toString(duplicate_consume_count))",
  serviceNameExpression: "status",
  bodyExpression: "notes",
  eventAttributesExpression:
    "map('run_id', run_id, 'produced_count', toString(produced_count), 'consumed_unique', toString(consumed_unique), 'missing_count', toString(missing_count), 'duplicate_consume_count', toString(duplicate_consume_count))",
  resourceAttributesExpression: "map('source', 'failover-test')",
  defaultTableSelectExpression:
    "ts,run_id,status,produced_count,consumed_unique,missing_count,duplicate_consume_count,active_consumer_role",
  severityTextExpression: "if(missing_count = 0 AND duplicate_consume_count = 0, 'ok', 'issue')",
  traceIdExpression: "''",
  spanIdExpression: "''",
  highlightedTraceAttributeExpressions: [],
  highlightedRowAttributeExpressions: [],
  materializedViews: [],
  querySettings: [],
  team: template.team,
  connection: template.connection,
  createdAt: new Date(),
  updatedAt: new Date(),
  __v: 0,
});
print("inserted: " + SOURCE_NAME);
