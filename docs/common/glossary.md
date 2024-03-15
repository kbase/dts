# DTS Glossary

* **DTS**: The [BER data transfer system](https://kbase.github.io/dts/)
* **Frictionless DataPackage**: a [representation](https://specs.frictionlessdata.io/data-package/)
  for a collection of **Frictionless DataResources**. The DTS generates **transfer
  manifests** in this format.
* **Frictionless DataResource**: a [representation](https://specs.frictionlessdata.io/data-resource/)
  of metadata for an individual resource (file).
* **Resource**: a file, with a unique identifier, that can be transferred from
* **Transfer Manifest**: a file (usually named `manifest.json`) containing a
  machine-readable **Frictionless DataPackage** containing metadata for a set of
  files transferred by the DTS. The DTS deposits a transfer manifest at the top
  level of the directory structure transferred to the destination database.
  a source database to a destination database by the DTS
* **UUID**: A [universally unique identifier](https://en.wikipedia.org/wiki/Universally_unique_identifier)
  used by the DTS to represent transfer operations and other transient tasks
