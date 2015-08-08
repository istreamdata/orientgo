# Updates in OrientDB (independent of txs)

A field update can change type ?  => test in Java client
doc.save() goes through ODatabaseDocumentTx even if no explicit tx set up (DocDB being followed here->)

Upon doc.save(),

* calls ODocument#validate only if NOT in a tx.  Validate validates the record following the declared constraints defined in schema such as mandatory, notNull, min, max, regexp, etc.
* ODocument#convertAllMultiValuesToTrackedVersions

* currentTx.saveRecord
* for cases where there is no tx, it uses OTransactionNoTx
* returns if record is not marked as dirty
* sets the record to status of MARSHALLING (is this a volatile field?)
* record#toStream
* gets the cluster name use the rid.clusterId
* storage.updateRecord  (OStorage class)
* does REQUEST_RECORD_UPDATE (32)
* after interacts with server -> localCache#updateRecord
* sets the record status to LOADED

# Transactions in OrientDB

Will need a Tx object that is thread local.
Proposal - hook it onto the `dbClient` or `dbClient.currDB` -> it is only present while the tx is in play

Only optimistic tx supported, NOT pessismistic tx based on locking.

## Nested transactions

Unclear from docs whether/how supported
If supported, need to have a chain (stack) of Tx objects off `dbClient` or `dbClient.currDB`

## Java client behavior

For GraphDB, txs are implicit and always used
For DocDB, you have to use them explicitly:

    db.open("remote:localhost:7777/petshop");
    
    try{
      db.begin(TXTYPE.OPTIMISTIC);
      ...
      // WRITE HERE YOUR TRANSACTION LOGIC
      ...
      db.commit();
    }catch( Exception e ){
      db.rollback();
    } finally{
      db.close();
    }
