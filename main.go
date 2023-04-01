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
	// Daten von MySql DB holen
	schuelerArray, klasseArray, klasseSchuelerArray := dbMysql()

	// Schüler Objekte erstellen
	var allSchuelers []map[string]interface{} = createSchueler(schuelerArray, klasseArray, klasseSchuelerArray)

	// Daten in MongoDb einfügen
	mongoDb(allSchuelers)
}

// Diese Funktion returnt alle gefundenen Daten in drei Doppelarray a
func dbMysql() ([][]string, [][]string, [][]int) {
	// mit DB connecten
	db, err := sql.Open("mysql", "root:@tcp(localhost:3306)/m320schueler")
	if err != nil {
		fmt.Println(err.Error())
		return nil, nil, nil
	}
	defer db.Close()

	// Daten holen
	schueler, err := db.Query("SELECT * FROM schueler")
	if err != nil {
		fmt.Println(err.Error())
		return nil, nil, nil
	}
	defer schueler.Close()

	klasse, err := db.Query("SELECT * FROM klasse")
	if err != nil {
		fmt.Println(err.Error())
		return nil, nil, nil
	}
	defer klasse.Close()

	klasseSchueler, err := db.Query("SELECT * FROM klasse_schueler")
	if err != nil {
		fmt.Println(err.Error())
		return nil, nil, nil
	}
	defer klasseSchueler.Close()

	//Daten in Arrays abfüllen
	var schuelerArray [][]string
	for schueler.Next() {
		var id int
		var benutzername, name, vorname string
		err := schueler.Scan(&id, &benutzername, &name, &vorname)
		if err != nil {
			fmt.Println(err.Error())
			return nil, nil, nil
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
			return nil, nil, nil
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
			return nil, nil, nil
		}
		singleKlasseSchueler := []int{kid, id}
		klasseSchuelerArray = append(klasseSchuelerArray, singleKlasseSchueler)
	}
	return schuelerArray, klasseArray, klasseSchuelerArray
}

// Diese Funktion returnt einen Array mit allen Schülern als Map
func createSchueler(schuelerArray [][]string, klasseArray [][]string, klasseSchuelerArray [][]int) []map[string]interface{} {
	var allSchuelers []map[string]interface{}
	// iteriert durch jeden Schüler & füllt daten in Array
	for _, schueler := range schuelerArray {
		var benutzername string = schueler[1]
		var name string = schueler[2]
		var vorname string = schueler[3]

		id, err := strconv.Atoi(schueler[0])
		if err != nil {
			panic(err)
		}
		// holt alle Klassen des schülers
		var klassen []string = getKlassen(id, klasseSchuelerArray, klasseArray)

		// Map für ein Schüler erstellen
		schuelerMap := make(map[string]interface{})
		schuelerMap["benutzername"] = benutzername
		schuelerMap["name"] = name
		schuelerMap["vorname"] = vorname
		schuelerMap["klassen"] = klassen

		allSchuelers = append(allSchuelers, schuelerMap)
	}
	return allSchuelers
}

// Diese Funktion returnt einen Array mit allen Klassen eines Spezifischen Schülers
func getKlassen(id int, klasseSchuelerArray [][]int, klasseArray [][]string) []string {
	var klassenIds []int
	var klassenNames []string
	// findet alle klassen ids die der Schüler id zugewiesen sind
	for _, ids := range klasseSchuelerArray {
		if id == ids[1] {
			klassenIds = append(klassenIds, ids[0])
		}
	}
	// für jede id in klassenIds wird der klassenname in klassenNames gespeichert
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

// Diese Funktion fügt jeden schüler in die MongoDB ein
func mongoDb(allSchuelers []map[string]interface{}) {
	// mit MongoDB connecten
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer client.Disconnect(context.Background())

	// datenbank & collection wählen
	database := client.Database("m165schueler")
	collection := database.Collection("schueler")

	// für jeden Schüler wird ein BSON datei erstellt & in DB inserted
	for _, schueler := range allSchuelers {
		var data bson.M
		if len(schueler["klassen"].([]string)) > 0 {
			data = bson.M(schueler)
		} else {
			delete(schueler, "klassen")
			data = bson.M(schueler)
			fmt.Println(data)
		}
		result, err := collection.InsertOne(context.Background(), data)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println(result.InsertedID)
	}
}
