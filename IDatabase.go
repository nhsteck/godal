package godal

type IDatabase interface {
	// Connect with database
	Connect()

	// Insert a record to table
	Create(tableName string, mapData map[string]interface{}) (interface{}, error)

	// Insert multi record to table
	CreateBatch(tableName string, listMapData []map[string]interface{}) (interface{}, error)

	// Update data on table
	Update(tableName string, newValue map[string]interface{}, whereCondition map[string]interface{}) (interface{}, error)

	// Delete record on table
	Delete(tableName string, whereCondition map[string]interface{}) (interface{}, error)

	// Get all data from table and map to array of struct.
	GetAllToMap(tableName string, limit int, offset int) ([]map[string]interface{}, error)

	// Get all data from table and map to array of struct.
	GetAllToStruct(tableName string, limit int, offset int, respStruct interface{}) (interface{}, error)

	// Execute query and return the map
	ExecuteSelectToMap(sqlQuery string, params []interface{}) ([]map[string]interface{}, error)

	// Execute query and return the struct
	ExecuteSelectToStruct(sqlQuery string, params []interface{}, respStruct interface{}) (interface{}, error)

	// Execute non query
	Execute(sqlExecute string, params []interface{}) (interface{}, error)
}
