db.sources.find({}, { name: 1, from: 1, connection: 1 }).forEach(function (s) {
  printjson({ name: s.name, table: s.from && s.from.tableName, connection: s.connection });
});
