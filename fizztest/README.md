
### Instructions to execute fizzbee tests
(Only single thread test is supported for now, so concurrency issues are not detected)

1. This repo has the test runner.  
```docker pull jayaprabhakar/fizzmo:slatedb-darwin-arm64-v1```  
<br/>
2. Run this from the slatedb-go directory which creates a file plugin.so  
```docker run --rm -v "$PWD":/usr/src/myapp -w /usr/src/myapp -e GOOS=linux  -e CGO_ENABLED=1 golang:1.23.1 go build  -trimpath -buildmode=plugin -gcflags="all=-N -l" -o plugin.so fizztest/fizztest_adapter.go```    
<br/>
3. Run FizzBee model checker which generates some state files in the out/run_* directory.  
```fizz fizztest/KeyValueStore.fizz```    
<br/>
4. Execute the tests  
```docker run --rm  -v ./plugin.so:/app/plugin.so -v ./fizztest/out/{RUN_DIR_NAME}:/data jayaprabhakar/fizzmo:slatedb-darwin-arm64-v1```  
