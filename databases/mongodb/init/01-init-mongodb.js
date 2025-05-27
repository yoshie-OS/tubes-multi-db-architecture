//use cafe_analytics;

// Create employees collection with schema validation
db.createCollection("employees", {
  validator: { $jsonSchema: {
    bsonType: "object",
    required: ["employee_id", "name", "position", "join_date", "shifts", "performance_rating"],
    properties: {
      employee_id: { bsonType: "int" },
      name: { bsonType: "string" },
      position: { bsonType: "string" },
      join_date: { bsonType: "date" },
      shifts: {
        bsonType: "array",
        items: { bsonType: "string" }
      },
      performance_rating: {
        bsonType: "number",
        minimum: 0,
        maximum: 5
      }
    }
  }},
  comment: "Employees collection with schema validation"
});

// Create menu_items collection with schema validation
db.createCollection("menu_items", {
  validator: { $jsonSchema: {
    bsonType: "object",
    required: ["menu_id", "name", "category", "price", "ingredients", "is_seasonal"],
    properties: {
      menu_id: { bsonType: "int" },
      name: { bsonType: "string" },
      category: { bsonType: "string" },
      price: { bsonType: "number" },
      ingredients: {
        bsonType: "array",
        items: { bsonType: "string" }
      },
      is_seasonal: { bsonType: "bool" }
    }
  }},
  comment: "Menu items collection with structured schema validation"
});