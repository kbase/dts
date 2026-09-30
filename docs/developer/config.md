# The `config` Package

This package defines data structures that store configuration information for the various components
of the DTS. Each of these data structures can be unmarshalled from YAML data, and can also be
transmitted as a `map[string]any` using the [mapstructure](https://github.com/mitchellh/mapstructure)
package.

## Selected Topics

### `mapstructure`

The `mapstructure` package was introduced to make it easier for other packages to accept independent
configuration objects to aid in separate testing. Unfortunately,

* how `mapstructure` works in practice is not necessarily how one expects, so the abstraction
  is little more [leaky](https://en.wikipedia.org/wiki/Leaky_abstraction) than one might expect
* the [repository](https://github.com/mitchellh/mapstructure) has been archived, which is not
  something one likes to see in a component of "sustainable" software
