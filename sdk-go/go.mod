module github.com/charlessonamericantrading/flux/sdk-go

go 1.22

require (
	github.com/google/uuid v1.6.0
	github.com/nats-io/nats.go v1.37.0
	// Validación L3 — 00-protocol.md §5. Soporta draft 2020-12, que es el que declaran
	// los esquemas de flux.
	//
	// En Go no existe el "extra opcional" de pip ni el `optionalDependencies` de npm, así
	// que esto lo pagan también los servicios en L2: +~1 MB de binario y una dependencia
	// más que auditar. A cambio no arrastra nada nuevo — solo golang.org/x/text, que ya
	// entraba por nats.go. Ver README §"El coste de la dependencia".
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2
	golang.org/x/text v0.14.0 // indirect
)

require (
	github.com/klauspost/compress v1.17.2 // indirect
	github.com/nats-io/nkeys v0.4.7 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.18.0 // indirect
	golang.org/x/sys v0.16.0 // indirect
)
