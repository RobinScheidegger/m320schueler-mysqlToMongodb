package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// connect to MySQL DB
	db, err := sql.Open("mysql", "root:@tcp(localhost:3306)/m320schueler")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer db.Close()

	// Abfrage MySQL DB
	schueler, err := db.Query("SELECT * FROM schueler")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer schueler.Close()

	klasse, err := db.Query("SELECT * FROM klasse")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer klasse.Close()

	klasseSchueler, err := db.Query("SELECT * FROM klasse_schueler")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer klasseSchueler.Close()

	//fill in Array
	var schuelerArray [][]string
	for schueler.Next() {
		var id int
		var benutzername, name, vorname string
		err := schueler.Scan(&id, &benutzername, &name, &vorname)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		singleSchueler := []string{strconv.Itoa(id), benutzername, name, vorname}
		schuelerArray = append(schuelerArray, singleSchueler)
	}

	var klasseArray [][]string
	for klasse.Next() {
		var kid int
		var name string
		err := klasse.Scan(&kid, &name)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		singleKlasse := []string{strconv.Itoa(kid), name}
		klasseArray = append(klasseArray, singleKlasse)
	}

	var klasseSchuelerArray [][]int
	for klasseSchueler.Next() {
		var kid, id int
		err := klasseSchueler.Scan(&kid, &id)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		singleKlasseSchueler := []int{kid, id}
		klasseSchuelerArray = append(klasseSchuelerArray, singleKlasseSchueler)
	}

	// Schüler Objekte erstellen
	var allSchuelers []map[string]interface{} = createSchueler(schuelerArray, klasseArray, klasseSchuelerArray)

	// Konfigurieren Sie den MongoDB-Client
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer client.Disconnect(context.Background())

	// Wählen Sie die Datenbank und die Sammlung aus
	database := client.Database("m165schueler")
	collection := database.Collection("schueler")

	// Fügen Sie Daten in die Sammlung ein
	for _, schueler := range allSchuelers {
		data := bson.M(schueler)
		result, err := collection.InsertOne(context.Background(), data)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println(result.InsertedID)
	}
}

func createSchueler(schuelerArray [][]string, klasseArray [][]string, klasseSchuelerArray [][]int) []map[string]interface{} {
	var allSchuelers []map[string]interface{}
	for _, schueler := range schuelerArray {
		var benutzername string = schueler[1]
		var name string = schueler[2]
		var vorname string = schueler[3]

		id, err := strconv.Atoi(schueler[0])
		if err != nil {
			panic(err)
		}
		var klassen []string = getKlassen(id, klasseSchuelerArray, klasseArray)

		schuelerMap := make(map[string]interface{})
		schuelerMap["benutzername"] = benutzername
		schuelerMap["name"] = name
		schuelerMap["vorname"] = vorname
		schuelerMap["klassen"] = klassen

		allSchuelers = append(allSchuelers, schuelerMap)
	}
	return allSchuelers
}

func getKlassen(id int, klasseSchuelerArray [][]int, klasseArray [][]string) []string {
	var klassenIds []int
	var klassenNames []string
	for _, ids := range klasseSchuelerArray {
		if id == ids[1] {
			klassenIds = append(klassenIds, ids[0])
		}
	}
	for _, id := range klassenIds {
		for _, klassen := range klasseArray {
			klassenId, err := strconv.Atoi(klassen[0])
			if err != nil {
				panic(err)
			}
			if id == klassenId {
				klassenNames = append(klassenNames, klassen[1])
			}
		}
	}
	return klassenNames
}
