// HyperDX MongoDB: add Kafka Mirror Delay (mirror_message_compare) if missing.
// Usage:
//   docker exec -i hyperdx-mongo mongosh hyperdx --quiet < add-mirror-delay-source.js

const SOURCE_NAME = "Kafka Mirror Delay";
const TABLE = "mirror_message_compare";

const existing = db.sources.findOne({ name: SOURCE_NAME });
if (existing) {
  print("already exists: " + SOURCE_NAME + " id=" + existing._id);
  quit(0);
}

const template = db.sources.findOne({ name: "Kafka Mirror Messages" });
if (!template) {
  print("ERROR: template source Kafka Mirror Messages not found");
  quit(1);
}

const doc = {
  name: SOURCE_NAME,
  kind: "log",
  from: { databaseName: "default", tableName: TABLE },
  timestampValueExpression: "ts",
  displayedTimestampValueExpression: "ts",
  implicitColumnExpression:
    "concat(value, ' delay=', toString(replication_delay_sec), 's')",
  serviceNameExpression: "concat(source_cluster, '->', mirror_cluster)",
  bodyExpression: "value",
  eventAttributesExpression:
    "map('message_id', message_id, 'source_topic', source_topic, 'mirror_topic', mirror_topic, 'source_offset', toString(source_offset), 'mirror_offset', toString(mirror_offset), 'replication_delay_ms', toString(replication_delay_ms), 'replication_delay_sec', toString(replication_delay_sec), 'end_to_end_delay_ms', toString(end_to_end_delay_ms), 'kafka_timestamp_delta_ms', toString(kafka_timestamp_delta_ms))",
  resourceAttributesExpression: "map('source', 'spring-mirror-compare')",
  defaultTableSelectExpression:
    "ts,message_id,value,source_cluster,mirror_cluster,source_offset,mirror_offset,replication_delay_sec,end_to_end_delay_sec,kafka_timestamp_delta_ms",
  severityTextExpression:
    "if(replication_delay_ms = 0, 'same-scrape', 'delayed')",
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
};

const res = db.sources.insertOne(doc);
print("inserted: " + SOURCE_NAME + " id=" + res.insertedId);
