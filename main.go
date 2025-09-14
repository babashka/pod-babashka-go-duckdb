package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	"container/list"
	"database/sql"

	"github.com/babashka/pod-babashka-go-duckdb/babashka"
	transit "github.com/babashka/transit-go"
	_ "github.com/marcboeker/go-duckdb/v2"
)

type ExecResult = map[transit.Keyword]int64

func debug(v any) {
	fmt.Fprintf(os.Stderr, "debug: %+v\n", v)
}

func encodeRows(rows *sql.Rows) ([]any, error) {
	cols, err := rows.Columns()
	columns := make([]transit.Keyword, len(cols))
	for i, col := range cols {
		columns[i] = transit.Keyword(col)
	}
	if err != nil {
		return nil, err
	}

	var data []any

	values := make([]any, len(columns))
	scanArgs := make([]any, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		results := make(map[transit.Keyword]any)

		if err = rows.Scan(scanArgs...); err != nil {
			debug(err)
			return nil, err
		}

		for i, val := range values {
			col := columns[i]
			results[col] = val
		}

		// debug(results)
		// debug(fmt.Sprintf("%T", results))

		data = append(data, results)
	}

	return data, nil
}

func encodeResult(result sql.Result) (ExecResult, error) {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	lastInsertedId, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	res := ExecResult{
		transit.Keyword("rows-affected"):    rowsAffected,
		transit.Keyword("last-inserted-id"): lastInsertedId,
	}

	return res, nil
}

func listToSlice(l *list.List) []any {
	slice := []any{}
	for e := l.Front(); e != nil; e = e.Next() {
		slice = append(slice, e.Value)
	}

	return slice
}

func parseQuery(args string) (string, string, []any, error) {
	reader := strings.NewReader(args)
	decoder := transit.NewDecoder(reader)
	value, err := decoder.Decode()
	if err != nil {
		return "", "", nil, err
	}

	argSlice := listToSlice(value.(*list.List))
	var query any
	db := ""

	if len(argSlice) == 1 {
		query = argSlice[0]
	} else {
		path, ok := argSlice[0].(string)
		if !ok {
			return "", "", nil, errors.New("the duckdb connection must be a string")
		}

		query = argSlice[1]
		db = path
	}

	switch queryArgs := query.(type) {
	case string:
		return db, queryArgs, make([]any, 0), nil
	case []any:
		return db, queryArgs[0].(string), queryArgs[1:], nil
	default:
		return "", "", nil, errors.New("unexpected query type, expected a string or a vector")
	}
}

func makeArgs(query []string) []any {
	args := make([]any, len(query)-1)

	for i := range query[1:] {
		args[i] = query[i+1]
	}

	return args
}

func respond(message *babashka.Message, response any) {
	buf := bytes.NewBufferString("")
	encoder := transit.NewEncoder(buf, false)
	encoder.AddHandler(reflect.TypeFor[int32](), transit.NewIntEncoder())

	if err := encoder.Encode(response); err != nil {
		babashka.WriteErrorResponse(message, err)
		return
	}

	babashka.WriteInvokeResponse(message, buf.String())
}

func processMessage(message *babashka.Message) {
	switch message.Op {
	case "describe":
		babashka.WriteDescribeResponse(
			&babashka.DescribeResponse{
				Format: "transit+json",
				Namespaces: []babashka.Namespace{
					{
						Name: "pod.babashka.go-duckdb",
						Vars: []babashka.Var{
							{
								Name: "execute!",
							},
							{
								Name: "query",
							},
						},
					},
				},
			})
	case "invoke":
		db, query, args, err := parseQuery(message.Args)
		if err != nil {
			babashka.WriteErrorResponse(message, err)
			return
		}

		conn, err := sql.Open("duckdb", db)
		if err != nil {
			babashka.WriteErrorResponse(message, err)
			return
		}

		defer conn.Close()

		switch message.Var {
		case "pod.babashka.go-duckdb/execute!":
			res, err := conn.Exec(query, args...)
			if err != nil {
				babashka.WriteErrorResponse(message, err)
				return
			}

			if json, err := encodeResult(res); err != nil {
				babashka.WriteErrorResponse(message, err)
			} else {
				respond(message, json)
			}
		case "pod.babashka.go-duckdb/query":
			res, err := conn.Query(query, args...)
			if err != nil {
				babashka.WriteErrorResponse(message, err)
				return
			}

			if json, err := encodeRows(res); err != nil {
				babashka.WriteErrorResponse(message, err)
			} else {
				respond(message, json)
			}
		default:
			babashka.WriteErrorResponse(message, fmt.Errorf("Unknown var %s", message.Var))
		}
	default:
		babashka.WriteErrorResponse(message, fmt.Errorf("Unknown op %s", message.Op))
	}
}

func main() {
	for {
		message, err := babashka.ReadMessage()
		if err != nil {
			babashka.WriteErrorResponse(message, err)
			continue
		}

		processMessage(message)
	}
}
