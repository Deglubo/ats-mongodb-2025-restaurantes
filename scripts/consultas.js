// JSON Schema creation
// Inspections
{
    $jsonSchema: {
        "type": "object",
        "properties": {
            "_id": {
                "type": "object",
                "properties": {
                    "$oid": {
                        "type": "string"
                    }
                }, 
                "required": [
                    "$oid"
                ]
            },
            "id": {
                "type": "string"
              },
              "certificate_number": {
                "type": "integer"
              },
              "business_name": {
                "type": "string"
              },
              "date": {
                "type": "string"
              },
              "result": {
                "type": "string"
              },
              "sector": {
                "type": "string"
              },
              "address": {
                "type": "object",
                "properties": {
                    "city": {
                        "type": "string"
                    },
                    "zip": {
                        "type": "string"
                    },
                    "street": {
                        "type": "string"
                    }
                    "number": {
                        "type": "string"
                    }
                },
                "required": [
                "city",
                "zip",
                "street",
                "number"
              ]
            },
            "restaurant_id": {
                "type": "string"
            }
        },
        "required": [
            "_id",
            "id",
            "certificate_number",
            "business_name",
            "date",
            "result",
            "sector",
            "address",
            "restaurant_id"
        ],
        "additionalProperties": false
    }
}

// Restaurants
{
    $jsonSchema: {
        "type": "object",
        "properties": {
            "_id": {
                "type": "object",
                "properties": {
                    "$oid": {
                        "type": "string"
                    }
                }, 
                "required": [
                    "$oid"
                ]
            },
            "URL": {
                "type": "string"
              },
            "address": {
                "type": "string"
              },
            "address line 2": {
                "type": "string"
              },
            "name": {
                "type": "string"
              },
            "outcode": {
                "type": "string"
              },
            "postcode": {
                "type": "string"
              },
            "rating": {
                "type": "integer"
              },
            "type_of_food": {
                "type": "string"
              },
        },
        "required": [
            "_id",
            "URL",
            "address",
            "address line 2",
            "name",
            "outcode",
            "postcode",
            "rating",
            "type_of_food"
        ]
    }
}

// CONSULTAS
// Consultas: Todos los restaurantes de tipo de comida “Chinese.”
db.restaurants.find({"type_of_food": "Chinese"})

// Consultas: Listar las inspecciones con violaciones, ordenadas por fecha. 
db.inspections.find({ "result": "Violation Issued" }).sort({"date": -1})

// Consultas: Encontrar restaurantes con una calificación superior a 4.
db.restaurants.find({"rating": {$gt: 4}})

// AGGREGATIONS
// Agrupar restaurantes por tipo de comida y calcular la calificación promedio.
db.restaurants.aggregate([
    {
        $group: {
            _id: "$type_of_food",
            promedio_calificacion: {$avg: "$rating"}
        }
    },
    {
        $project: {
            type_of_food: "$_id",
            promedio_calificacion: {$round: ["$promedio_calificacion", 2]},
            _id: 0
        }
    },
    {
        $sort: {promedio_calificacion: -1}
    }
]);

// Contar el número de inspecciones por resultado y mostrar los porcentajes.
db.inspections.aggregate([
    {
        $group: {
            _id: "$result",
            count: {$sum: 1}
        }
    },
    {
        $group: {
            _id: null,
            total: {$sum: "$count"},
            results: {$addToSet: { result: "$_id", count: "$count" }}
        }
    },
    {
        $unwind: "$results"
    },
    {
        $project: {
            _id: 0,
            result: "$results.result",
            count: "$results.count",
            percentage: {
                $round: [{$multiply: [ {$divide: ["$results.count", "$total"]}, 100 ]}, 2]
            }
        }
    },
    {
        $sort: {count: -1}
    }
]);

// Unir restaurantes con sus inspecciones utilizando $lookup
db.restaurants.aggregate([
    {
        $lookup: {
          from: "inspections",
          let: { restaurant_id_str: { $toString: "$_id" } },
          pipeline: [
            {
              $match: {
                $expr: { $eq: ["$restaurant_id", "$$restaurant_id_str"] } 
              }
            }
          ],
          as: "inspection_history"
        }
      },
    {
        $project: {
            _id: 1,
            name: 1,
            type_of_food: 1,
            rating: 1,
            inspection_history: 1
        }
    }
]);

// Consultas indexación
// Consulta restaurants type_of_food y rating:

db.restaurants.find({
    type_of_food: "Curry",
    rating: { $gt: 4 }
  }).explain("executionStats");

// Consulta inspecciones con result y fechas:

db.inspections.find({
    result: "No Violation Issued",
    date: { $gte: "Jul 01 2023", $lte: "Jul 31 2023" }
  }).explain("executionStats");

// Consulta union restaurants e inspections con lookup:
db.restaurants.aggregate([
    {
      $lookup: {
        from: "inspections",
        localField: "_id",
        foreignField: "restaurant_id",
        as: "inspection_history"
      }
    },
    {
      $match: {
        "inspection_history.result": "Violation Issued"
      }
    }
  ]).explain("executionStats");

// Implementación index:
db.restaurants.createIndex({ type_of_food: 1, rating: 1 });
db.inspections.createIndex({ result: 1, date: 1 });
db.inspections.createIndex({ restaurant_id: 1 });
