# athena-sample

## How to use

1. Export aws credentials.

```
  export AWS_ACCESS_KEY_ID=xxxxx
	export AWS_SECRET_ACCESS_KEY=yyyyy
```

2. Input OUTPUT_LOCATION of `main.go` at line 17.

3. Input queryString(=SQL for Athena) of `main.go` at line 20-.

4. run

```
go run main.go
```
