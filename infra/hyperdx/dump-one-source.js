const s = db.sources.findOne({ name: "Kafka Mirror Messages" });
printjson(s);
