# ver 0.9.15 - 2017-10-04

Auto-compaction for CouchDB evictor. Just call _lambdaroyal.memory.eviction.couchdb/schedule-compaction_ with parameters _eviction-channel and _context_, this starts the eviction right away and recurs this everyday midnight

# ver0.9.2 - 2017-04-14

Adding attribute indexes dynamically at runtime. Refer to *context* namespace

# ver0.9 - 2017-02-10

_EvictorChannel protocol changed_

*ATTENTION* Evictor Channels are not backward compatible to ver < 0.9
The respective *delete* function must incooperate a additional parameter that denotes the user value of the association that was just deleted, rather than only the the collection key and the keyval key

_CouchDB EvictorChannel does not stop default on failed retries anymore. Set atom *lambdaroyal.memory.eviction.couchdb/stop-on-fatal* to true iff you desire the old behaviour_
