package main

import (
	"database/sql"
	"fmt"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
)

var klassenzaehler int

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

	//Schüler Object erstellen
	createSchueler(schuelerArray, klasseArray, klasseSchuelerArray)
	fmt.Println(klassenzaehler)
}

func createSchueler(schuelerArray [][]string, klasseArray [][]string, klasseSchuelerArray [][]int) {
	for _, schueler := range schuelerArray {
		var benutzername string = schueler[1]
		var name string = schueler[2]
		var vorname string = schueler[3]

		id, err := strconv.Atoi(schueler[0])
		if err != nil {
			panic(err)
		}
		var klassen []string = getKlassen(id, klasseSchuelerArray, klasseArray)

		var schuelerObject []interface{} = []interface{}{benutzername, name, vorname, klassen}
		fmt.Println(schuelerObject...)
	}
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
				klassenzaehler++
			}
		}
	}
	return klassenNames
}
