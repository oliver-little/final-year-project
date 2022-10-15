##File Storage:
- Custom solution seems difficult
- Spark approach is to not bother knowing where the data is stored, and just pull it in at execution 
	- Don't like this idea because for this application, there is no reason we wouldn't have control of where the data is stored
	- Then can leverage indexing/partitioning
- Therefore, if not custom solution, but want control over how the data is stored, some kind of database (probably NoSQL - Mongo or Cassandra)?
- System can pull in the data at upload and store it permanently in the database
	- This will also allow for "updates" when new data is uploaded
	- Some kind of date system, maybe using checksums to check whether a file is new or not?
- Sending results back - **use this database again? or something different**
	
##On-disk database for results:
	- Can probably use the same database as the storage DB, but need to decide on this?
	- Might be worth spinning up something local to the orchestrator as the workers don't need to care.
	
##Message Passing/RPC:
	- Possibly having each worker running a web server to take REST/RPC calls from the orchestrator.
	- Alternative is using a message passing service like Redis or RabbitMQ, but not sure if this is necessary.
	- Main benefit of message passing is persistence - if a node fails a worker can restart to pick it up.
	- However, in this scenario the orchestrator has control of the cluster, and will track failed nodes.
	- Probably better for it to detect failed nodes and handle them itself by spinning up a new worker.

# 15/10/2022

## RPC
	- Airframe RPC
		- Direct scala implementation, very easy to use
		- Might not work too well if need to move out of Scala
	- Finagle
		- Also implemented in scala, very high level
		- Might be better as better developed and will allow me to implement more
