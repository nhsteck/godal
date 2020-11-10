package godal

import (
	"encoding/json"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
)

var (
	pg IDatabase
)

func TestMain(m *testing.M) {
	log.Println("Do stuff BEFORE the tests!")

	setup()

	exitVal := m.Run()
	log.Println("Do stuff AFTER the tests!")

	os.Exit(exitVal)
}

func setup() {
	pg = Postgres{
		Host:        "127.0.0.1",
		Port:        "5432",
		Dbname:      "db_godal",
		User:        "odoo",
		Pass:        "1234",
		MaxIdleConn: 10,
		MaxOpenConn: 2,
	}
	pg.Connect()
}

func TestCreate(t *testing.T) {
	t.SkipNow()

	tableName := "users"
	mapData := map[string]interface{}{
		"id":    129,
		"name":  "Hung Son",
		"email": "son@gmail.com",
		"phone": "0909123333",
	}
	_, err := pg.Create(tableName, mapData)
	if err != nil {
		log.Errorln("FAIL >> TestCreate")
		t.Skip()
	}
}

func TestCreateBatch(t *testing.T) {
	//t.SkipNow()

	tableName := "users"
	var listMapData = make([]map[string]interface{}, 0)
	firstRow := map[string]interface{}{
		"name":  "test 1",
		"email": "test1@local.com",
		"phone": "0323929323",
	}

	secondRow := map[string]interface{}{
		"name":  "test 2",
		"email": "test2@local.com",
	}

	thirdRow := map[string]interface{}{
		"name":  "test 3",
		"phone": "0323929323",
	}

	listMapData = append(listMapData, firstRow)
	listMapData = append(listMapData, secondRow)
	listMapData = append(listMapData, thirdRow)
	log.Println("Data Input: ")
	log.Println(listMapData)

	_, err := pg.CreateBatch(tableName, listMapData)
	if err != nil {
		log.Errorln("FAIL >> TestCreateBatch")
		log.Errorln("Loi: ")
		log.Errorln(err)
		t.Skip()
	}
}

func TestCreateOrUpdateBatch(t *testing.T) {
	t.SkipNow()

	tableName := "users"
	var listMapData = make([]map[string]interface{}, 0)
	firstRow := map[string]interface{}{
		"id":    1,
		"name":  "test 1.2.2.4",
		"email": "test1@local.com",
		"phone": "0323929323",
	}

	secondRow := map[string]interface{}{
		"id":    2,
		"name":  "test 2.1.4.5",
		"email": "test2@local.com",
	}

	thirdRow := map[string]interface{}{
		"id":    4,
		"name":  "test 3.6",
		"phone": "0323929323",
	}

	listMapData = append(listMapData, firstRow)
	listMapData = append(listMapData, secondRow)
	listMapData = append(listMapData, thirdRow)
	log.Println("Data Input: ")
	log.Println(listMapData)

	_, err := pg.CreateOrUpdateBatch(tableName, listMapData, "id")
	if err != nil {
		log.Errorln("FAIL >> TestCreateBatch")
		log.Errorln("Loi: ")
		log.Errorln(err)
		t.Skip()
	}
}

func TestUpdate(t *testing.T) {
	t.SkipNow()

	tableName := "users"

	newValue := map[string]interface{}{
		"name": "Hung Son 1",
	}

	whereValue := map[string]interface{}{
		"id": "126",
	}

	_, err := pg.Update(tableName, newValue, whereValue)
	if err != nil {
		log.Errorln("FAIL >> TestUpdate")
		t.Skip()
	}
}

func TestDelete(t *testing.T) {
	t.SkipNow()

	tableName := "users"

	whereValue := map[string]interface{}{
		"id": "126",
	}

	_, err := pg.Delete(tableName, whereValue)
	if err != nil {
		log.Errorln("FAIL >> TestDelete")
		t.Skip()
	}
}

func TestExecuteSelectToMap(t *testing.T) {
	t.SkipNow()

	sqlQuery := `SELECT * FROM users WHERE id > $1`
	params := []interface{}{123}
	rs, err := pg.ExecuteSelectToMap(sqlQuery, params)
	if err != nil {
		log.Errorln("FAIL >> TestExecuteSelectToMap")
		t.Skip()
	}
	log.Infoln(rs)
}

func TestExecuteSelectToStruct(t *testing.T) {
	t.SkipNow()
	type Users struct {
		ID    string `db:"id" json:"a_id"`
		Name  string `db:"name" json:"a_name"`
		Email string `db:"email" json:"a_email"`
		Phone string `db:"phone" json:"a_phone"`
	}

	objUsers := Users{}
	sqlQuery := `SELECT * FROM users WHERE id >= $1`
	params := []interface{}{123}
	rs, err := pg.ExecuteSelectToStruct(sqlQuery, params, objUsers)
	if err != nil {
		log.Errorln("FAIL >> TestExecuteSelectToMap ", err)
		t.Skip()
	}
	var arrUsers []Users = make([]Users, 0)
	arrOrg := rs.([]interface{})
	for _, elem := range arrOrg {
		newUser := &Users{}
		byteData, _ := json.Marshal(elem)
		json.Unmarshal(byteData, &newUser)
		arrUsers = append(arrUsers, *newUser)
	}
	log.Infoln(arrUsers)
}

func TestExecute(t *testing.T) {
	t.Skip()
	sqlExecute := "INSERT INTO users VALUES ($1, $2, $3, $4)"
	params := []interface{}{127, "Thanh Tâm", "tampham1190@gmail.com", "0989554552"}
	rs, err := pg.Execute(sqlExecute, params)
	if err != nil {
		log.Errorln("FAIL >> TestExecuteSelectToMap ", err)
		t.Skip()
	}
	log.Infoln(rs)
}
