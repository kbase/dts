# The `services` Package

This package implements the server portion of the DTS that exposes its API, dispatching requests via
Goroutines. The server uses [Huma](https://huma.rocks/) to service requests. Documentation for each
API endpoint is automatically generated from annotated input and output fields in the implementing
function.

