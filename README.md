Abasyn is being developed to synchronise Firebird-powered databases used by in-house accounting application called Abacus. It listens to the configured database event which is sent from the server when some data is ready for replication. Upon receiving the event, Abasyn pulls the data from its database and sends those to other databases. Once completed it cleans up the temporary tables in the database and waits until next event. Also it provides some simple REST API.

Once everything above is properly implemented this file will be updated with all the relevant details and exaplanations.