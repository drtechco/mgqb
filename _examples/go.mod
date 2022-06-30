module examples

go 1.16

require (
	github.com/drtechco/mgqb v0.0.0 
	go.mongodb.org/mongo-driver v1.9.1
)

replace (
 github.com/drtechco/mgqb v0.0.0  =>  ../
)