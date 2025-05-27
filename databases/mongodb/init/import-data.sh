#!/bin/bash
echo "=== Waiting for MongoDB to start ==="
sleep 5

echo "=== Importing Employees Data ==="
mongoimport --host mongo \
  --db cafe_analytics \
  --collection employees \
  --type json \
  --file /docker-entrypoint-initdb.d/data/employees.json \
  --jsonArray

echo "=== Importing Menu Items Data ==="
mongoimport --host mongo \
  --db cafe_analytics \
  --collection menu_items \
  --type json \
  --file /docker-entrypoint-initdb.d/data/menu.json \
  --jsonArray