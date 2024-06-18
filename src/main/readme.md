
```shell
go run mrsequential.go wc.so pg*.txt
```

```shell
go build -buildmode=plugin ../mrapps/wc.go
```

```shell
go run mrcoordinator.go pg*.txt
```

```shell
go run mrworker.go wc.so 
```


```shell
bash test-mr.sh
```