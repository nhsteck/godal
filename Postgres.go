package godal

import (
	"fmt"
	"reflect"

	"database/sql"

	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

var (
	DBConn *sql.DB
)

func (p Postgres) Connect() {
	strConn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s sslmode=disable",
		p.Host, p.Port, p.User, p.Dbname, p.Pass)
	db, err := sql.Open("postgres", strConn)
	db.SetMaxIdleConns(int(p.MaxIdleConn))
	db.SetMaxOpenConns(int(p.MaxOpenConn))
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	log.Printf("Connect to postgres database %s:%s/%s successful", p.Host, p.Port, p.Dbname)
	DBConn = db
}

func (p Postgres) Create(tableName string, mapData map[string]interface{}) (interface{}, error) {
	sqlStatement := `
		INSERT INTO %s(%s) 
		VALUES (%s) 
		RETURNING *
	`
	arrValues, strParams, strValues := convertMapToParams(mapData)
	sqlStatement = fmt.Sprintf(sqlStatement, tableName, strParams, strValues)

	rs, err := DBConn.Exec(sqlStatement, arrValues...)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return rs, nil
}

func (p Postgres) Update(tableName string, newValue map[string]interface{}, whereCondition map[string]interface{}) (interface{}, error) {
	sqlStatement := `
		UPDATE %s 
		SET %s 
		WHERE %s
	`

	arrSet, strSet, loopIndex := buildConditionQuery(newValue, ",", 1)
	arrWhere, strWhere, _ := buildConditionQuery(whereCondition, " AND", loopIndex)
	sqlStatement = fmt.Sprintf(sqlStatement, tableName, strSet, strWhere)
	arrValues := make([]interface{}, 0)
	arrValues = append(arrSet, arrWhere...)

	rs, err := DBConn.Exec(sqlStatement, arrValues...)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return rs, nil
}

func (p Postgres) Delete(tableName string, whereCondition map[string]interface{}) (interface{}, error) {
	sqlStatement := `DELETE FROM %s WHERE %s`
	arrWhere, strWhere, _ := buildConditionQuery(whereCondition, " AND", 1)
	sqlStatement = fmt.Sprintf(sqlStatement, tableName, strWhere)

	rs, err := DBConn.Exec(sqlStatement, arrWhere...)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return rs, nil
}

func (p Postgres) GetAllToMap(tableName string, limit int, offset int) ([]map[string]interface{}, error) {
	sqlStatement := fmt.Sprintf("SELECT * FROM %s", tableName)
	if limit > -1 {
		sqlStatement = sqlStatement + fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)
	}

	rows, err := DBConn.Query(sqlStatement)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	cols := make([]interface{}, len(colNames))
	colPtrs := make([]interface{}, len(colNames))

	for i := 0; i < len(colNames); i++ {
		colPtrs[i] = &cols[i]
	}

	var myMap = make([]map[string]interface{}, 0)
	for rows.Next() {
		err = rows.Scan(colPtrs...)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
		var rowMap = make(map[string]interface{})
		for i, col := range cols {
			rowMap[colNames[i]] = col
		}

		myMap = append(myMap, rowMap)
	}

	return myMap, nil
}

func (p Postgres) GetAllToStruct(tableName string, limit int, offset int, respStruct interface{}) (interface{}, error) {
	sqlStatement := fmt.Sprintf("SELECT * FROM %s", tableName)
	if limit > -1 {
		sqlStatement = sqlStatement + fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)
	}

	rows, err := DBConn.Query(sqlStatement)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	cols := make([]interface{}, len(colNames))
	colPtrs := make([]interface{}, len(colNames))

	for i := 0; i < len(colNames); i++ {
		colPtrs[i] = &cols[i]
	}

	attr := reflect.ValueOf(respStruct)
	attrType := reflect.TypeOf(respStruct)

	mapAttr := make(map[string]string)
	mapType := make(map[string]string)
	for k := 0; k < attr.NumField(); k++ {
		fieldTag := attrType.Field(k).Tag
		fieldType := attrType.Field(k).Type.String()
		dbFieldName, _ := fieldTag.Lookup("db")
		mapAttr[dbFieldName] = attrType.Field(k).Name
		mapType[dbFieldName] = fieldType
	}

	var arrStruct = make([]interface{}, 0)
	for rows.Next() {
		err = rows.Scan(colPtrs...)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		newStruct := reflect.New(attrType).Elem()
		for i, col := range cols {
			fieldName := mapAttr[colNames[i]]
			newStruct.FieldByName(fieldName).Set(reflect.ValueOf(col))
		}

		arrStruct = append(arrStruct, newStruct.Addr().Interface())
	}

	return arrStruct, nil
}

func (p Postgres) ExecuteSelectToMap(sqlQuery string, params []interface{}) ([]map[string]interface{}, error) {
	sqlStatement := sqlQuery

	rows, err := DBConn.Query(sqlStatement, params...)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	cols := make([]interface{}, len(colNames))
	colPtrs := make([]interface{}, len(colNames))

	for i := 0; i < len(colNames); i++ {
		colPtrs[i] = &cols[i]
	}

	var myMap = make([]map[string]interface{}, 0)
	for rows.Next() {
		err = rows.Scan(colPtrs...)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
		var rowMap = make(map[string]interface{})
		for i, col := range cols {
			rowMap[colNames[i]] = col
		}

		myMap = append(myMap, rowMap)
	}

	return myMap, nil
}

func (p Postgres) ExecuteSelectToStruct(sqlQuery string, params []interface{}, respStruct interface{}) (interface{}, error) {
	sqlStatement := sqlQuery

	rows, err := DBConn.Query(sqlStatement, params...)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	cols := make([]interface{}, len(colNames))
	colPtrs := make([]interface{}, len(colNames))

	for i := 0; i < len(colNames); i++ {
		colPtrs[i] = &cols[i]
	}

	attr := reflect.ValueOf(respStruct)
	attrType := reflect.TypeOf(respStruct)

	mapAttr := make(map[string]string)
	mapType := make(map[string]string)
	for k := 0; k < attr.NumField(); k++ {
		fieldTag := attrType.Field(k).Tag
		fieldType := attrType.Field(k).Type.String()
		dbFieldName, _ := fieldTag.Lookup("db")
		mapAttr[dbFieldName] = attrType.Field(k).Name
		mapType[dbFieldName] = fieldType
	}

	var arrStruct = make([]interface{}, 0)
	for rows.Next() {
		err = rows.Scan(colPtrs...)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		newStruct := reflect.New(attrType).Elem()
		for i, col := range cols {
			fieldName := mapAttr[colNames[i]]
			newStruct.FieldByName(fieldName).Set(reflect.ValueOf(col))
		}

		arrStruct = append(arrStruct, newStruct.Addr().Interface())
	}
	return arrStruct, nil
}

func (p Postgres) Execute(sqlExecute string, params []interface{}) (interface{}, error) {
	return nil, nil
}

func convertMapToParams(mapData map[string]interface{}) ([]interface{}, string, string) {
	var mapLen int = len(mapData)
	var arrValues []interface{} = make([]interface{}, 0)
	var strParams string = ""
	var strValues string = ""
	var loopIndex int = 1

	for k, v := range mapData {
		arrValues = append(arrValues, v)
		if loopIndex == mapLen {
			strParams = strParams + k
			strValues = strValues + fmt.Sprintf("$%d", loopIndex)
		} else {
			strParams = strParams + k + ", "
			strValues = strValues + fmt.Sprintf("$%d, ", loopIndex)
		}

		loopIndex++
	}

	return arrValues, strParams, strValues
}

func buildConditionQuery(mapData map[string]interface{}, charSplit string, loopIndex int) ([]interface{}, string, int) {
	var mapLen int = len(mapData) + (loopIndex - 1)
	var result string = ""
	var arrValues []interface{} = make([]interface{}, 0)

	for k, v := range mapData {
		arrValues = append(arrValues, v)
		if loopIndex == mapLen {
			result = result + fmt.Sprintf("%s=$%d", k, loopIndex)
		} else {
			result = result + fmt.Sprintf("%s=$%d%s ", k, loopIndex, charSplit)
		}

		loopIndex++
	}

	return arrValues, result, loopIndex
}
