## Queries

#### Query of fields (properties) only

When you query fields only, you still get back a Document, but the ClusterId portion of that RID of that Document is -2.

```go

	sql := "select title from Posts"
	docs, err := obinary.SQLQuery(dbc, sql, "")
	Ok(err)
    ----
    [ODocument<Classname: ; RID: #-2:0; Version: 0; fields: 
      OField<id: -1; name: title; datatype: 7; value: My very first post>>
     ODocument<Classname: ; RID: #-2:1; Version: 0; fields: 
      OField<id: -1; name: title; datatype: 7; value: Number>>
     ODocument<Classname: ; RID: #-2:2; Version: 0; fields: 
      OField<id: -1; name: title; datatype: 7; value: Number>>
    ]

	sql = "select @rid as ReadRid, title from Posts"
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
    ---- 
    [ODocument<Classname: ; RID: #-2:0; Version: 0; fields: 
      OField<id: -1; name: ReadRid; datatype: 13; value: <OLink RID: #10:0, Record: <nil>>>
      OField<id: -1; name: title; datatype: 7; value: My very first post>>
     ODocument<Classname: ; RID: #-2:1; Version: 0; fields: 
      OField<id: -1; name: ReadRid; datatype: 13; value: <OLink RID: #10:1, Record: <nil>>>
      OField<id: -1; name: title; datatype: 7; value: Number>>
     ODocument<Classname: ; RID: #-2:2; Version: 0; fields: 
      OField<id: -1; name: ReadRid; datatype: 13; value: <OLink RID: #10:2, Record: <nil>>>
      OField<id: -1; name: title; datatype: 7; value: Number>>
    ]
```
