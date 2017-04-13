# ver0.9.2 - 2017-04-14

Adding attribute indexes dynamically at runtime. Refer to *context* namespace

# ver0.9 - 2017-02-10

_EvictorChannel protocol changed_

*ATTENTION* Evictor Channels are not backward compatible to ver < 0.9
The respective *delete* function must incooperate a additional parameter that denotes the user value of the association that was just deleted, rather than only the the collection key and the keyval key

_CouchDB EvictorChannel does not stop default on failed retries anymore. Set atom *lambdaroyal.memory.eviction.couchdb/stop-on-fatal* to true iff you desire the old behaviour_
