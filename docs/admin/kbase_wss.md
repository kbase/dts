# The KBase Workspace Service (WSS) Environment

The Data Transfer Service (DTS) can transfer files to a user's staging folder within the 
legacy KBase Workspace Service environment using the `kbase` database as a destination.
In this environment, an authorized DTS user can request a transfer on behalf of another KBase
user.

The way this works behind the scenes is that the files are not transferred directly to the
authorized user or to the user receiving the files. Instead, the files are transferred to
a "holding pen". There, a file watcher process sits, observing the folders created and
populated by the DTS. When a `manifest.json` file is completely written to a DTS transfer
folder (name: `dts-<transfer-id>`), the watcher moves the files to their final destination.

If this watcher process is not running, the files remain in this holding pen. This can be
confusing to users performing transfers. If a user doesn't see the files they requested but
sees that the transfer haѕ been completed (via a status check, for example), checking the
status of the watcher process is a good first step.
