# folder_analyser
The program only works with a single filedir into the project called "FileToTest", the program analize and show in the navegator "Id of the files represented like MD5", "Name of the file" and "Size of the file"

### Installing

To run the program, please install previous requirement:

* Install Go compiler for official web site: https://golang.org/

### Install GoLand(Optional - Recomendation)
* Install GoLand: https://www.jetbrains.com/go/

### If you have installed GoLand, Do not follow these steps:
* After clone repository, inside the project folder, open a command prompt and type:
```
go get -u github.com/golang/dep/cmd/dep
```
```
dep init -v
```
```
dep ensure -v 
```
### Required
* Execute the http server:
```
go run main.go
```

* Open an Internet Browser and go to [Localhost port 9000, route documents](http://localhost:9000/documents)

### Modifications:
* If you have change the folder to analyze:
```
fileDir := "./FileToTest/"
```
* And relaunch project.
