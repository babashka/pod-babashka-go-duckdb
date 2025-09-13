package babashka

import (
	"bufio"
	"fmt"
	"github.com/jackpal/bencode-go"
	"os"
)

func debug(v any) {
	fmt.Fprintf(os.Stderr, "debug: %+q\n", v)
}

type Message struct {
	Op   string
	Id   string
	Args string
	Var  string
}

type Namespace struct {
	Name string `bencode:"name"`
	Vars []Var  `bencode:"vars"`
}

type Var struct {
	Name string `bencode:"name"`
	Code string `bencode:"code,omitempty"`
}

type DescribeResponse struct {
	Format     string      `bencode:"format"`
	Namespaces []Namespace `bencode:"namespaces"`
}

type InvokeResponse struct {
	Id     string   `bencode:"id"`
	Value  string   `bencode:"value"` // stringified json response
	Status []string `bencode:"status"`
}

type ErrorResponse struct {
	Id        string   `bencode:"id"`
	Status    []string `bencode:"status"`
	ExMessage string   `bencode:"ex-message"`
	ExData    string   `bencode:"ex-data,omitempty"`
}

func ReadMessage() (*Message, error) {
	reader := bufio.NewReader(os.Stdin)
	message := &Message{}
	if err := bencode.Unmarshal(reader, &message); err != nil {
		return nil, err
	}

	return message, nil
}

func WriteDescribeResponse(describeResponse *DescribeResponse) {
	writeResponse(*describeResponse)
}

func WriteInvokeResponse(inputMessage *Message, value string) error {
	response := InvokeResponse{Id: inputMessage.Id, Status: []string{"done"}, Value: value}

	return writeResponse(response)
}

func WriteErrorResponse(inputMessage *Message, err error) {
	errorMessage := string(err.Error())
	errorResponse := ErrorResponse{
		Id:        inputMessage.Id,
		Status:    []string{"done", "error"},
		ExMessage: errorMessage,
	}
	writeResponse(errorResponse)
}

func writeResponse(response any) error {
	writer := bufio.NewWriter(os.Stdout)
	if err := bencode.Marshal(writer, response); err != nil {
		return err
	}

	writer.Flush()

	return nil
}
