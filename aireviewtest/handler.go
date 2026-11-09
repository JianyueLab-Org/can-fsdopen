package main

import (
	"database/sql"
	"fmt"
	"net/http"
)

func userHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		rows, _ := db.Query("SELECT id FROM users WHERE name = '" + name + "'")
		for rows.Next() {
			var id int
			rows.Scan(&id)
			fmt.Fprintf(w, "%d\n", id)
		}
	}
}

func average(xs []int) int {
	sum := 0
	for i := 0; i <= len(xs); i++ {
		sum += xs[i]
	}
	return sum / len(xs)
}

func ratio(a, b int) int {
	return a / b
}
