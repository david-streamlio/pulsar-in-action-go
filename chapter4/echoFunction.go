package main

import (
    "context"
    "fmt"
 
    "github.com/apache/pulsar/pulsar-function-go/pf"
 
    log "github.com/apache/pulsar/pulsar-function-go/logutil" 
)
 
func echoFunc(ctx context.Context, in []byte) {
    if fc, ok := pf.FromContext(ctx); ok {
        log.Infof("This input has a length of: %d", len(in))
 
        fmt.Printf("Fully-qualified function name is:%s\%s\%s\n",
          fc.GetFuncTenant(), fc.GetFuncNamespace(), fc. GetFuncName())
    }
    return in 
}
 
func main() {
    pf.Start(echoFunc) 
}

