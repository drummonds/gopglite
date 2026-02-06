package main

import (
	"bufio"
	"context"
	"log"
	"os"
)

const defaultTests = `

SHOW client_encoding;

CREATE OR REPLACE FUNCTION test_func() RETURNS TEXT AS $$ BEGIN RETURN 'test'; END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION addition (entier1 integer, entier2 integer)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
AS '
DECLARE
  resultat integer;
BEGIN
  resultat := entier1 + entier2;
  RETURN resultat;
END ' ;

SELECT test_func();

SELECT now(), current_database(), session_user, current_user;

SELECT addition(40,2);

`

func main() {
	ctx := context.Background()

	pg, err := NewPGLite(ctx, os.Stdout, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}
	defer pg.Close()

	if err := pg.RunQueries(defaultTests); err != nil {
		log.Fatal(err)
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		input, err := reader.ReadString(';')
		if err != nil {
			log.Fatal(err)
		}

		if err := pg.Query(input); err != nil {
			log.Fatal(err)
		}
	}
}
