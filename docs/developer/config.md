# The `config` Package

This package defines data structures that store configuration information for the various components
of the DTS. Each of these data structures can be unmarshalled from YAML data, and can also be
transmitted as a `map[string]any` using the [mapstructure](https://github.com/mitchellh/mapstructure)
package.
