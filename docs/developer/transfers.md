# The `transfers` Package

This package contains data structures and functions that represent the lifecycle of every transfer
in the DTS. This lifecycle is implemented by several concurrent goroutines in the idiom of 
[communicating sequential processes](https://en.wikipedia.org/wiki/Communicating_sequential_processes).
An interaction diagram of the lifecycle is shown below.

![DTS transfer orchestration](https://github.com/kbase/dts/blob/main/transfers/orchestration.png)

The important processes in this lifecycle are

* the **dispatcher**, which accepts user requests forwarded from the REST API and orchestrates a
  response involving one or more of the other processes
* the **store**, which keeps records of transfers and their current status, allowing other
  processes to create, update, and fetch these records
* the **stager**, which handles the process of "staging" files for transfer (e. g. unarchiving them
  from tape so they sit at the right location on disk)
* the **mover**, which initiates transfers for sets of staged files, monitoring their status and
  updating it within the store
* the **manifestor**, which generates and transfers a JSON manifest file after each successful
  file transfer

## A Few Remarks

In order to transmit a user's S3 credentials to Globus to transfer files into the KBase Data
Lakehouse, we inserted a hack into the store process. In the hack, we check to see whether the
destination database is the KBase Data Lakehouse, and if so, we read the Globus ID from the
KBase user federation system and insert it into the user's `ConnectionCredentials` field where it
can be read by the Globus endpoint downstream in order to authorize the transfer via the Globus
S3 connector.
