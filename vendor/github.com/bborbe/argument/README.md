# Argument

This library helps to fill command line args and enviroment variable to struct.

```
var data struct {
	Username string `arg:"username" env:"USERNAME" default:"ben"`
	Password string `arg:"password" env:"PASSWORD"`
}
if err := argument.Parse(&data); err != nil {
	log.Fatalf("parse args failed: %v", err)
}
fmt.Printf("username %s, password %s\n", data.Username, data.Password)
}
