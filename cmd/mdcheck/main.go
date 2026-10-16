package main

import (
	"fmt"

	"telesrv/internal/botapi"
)

func main() {
	for _, in := range []string{
		"*жирный* текст",
		"**двойной жирный** текст",
		"Канал *xirograms* отправлен",
		"`код` и _курсив_",
	} {
		text, ents, err := botapi.ParseOperatorMarkdown(in)
		fmt.Printf("in=%-34q -> text=%-30q entities=%d err=%v\n", in, text, len(ents), err)
		for _, e := range ents {
			fmt.Printf("      %+v\n", e)
		}
	}
}
