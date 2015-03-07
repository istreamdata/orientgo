## Network Binary Protocol Commands

This is the guide to the commands you can send through the binary protocol.

## See also
- [List of SQL Commands](Commands.md)
- [Network Binary Protocol Specification](Network Binary Protocol.md#request_command)

the commands are divided in three main groups:

* SQL (select) Query
* SQL Commands 
* Script commands

### SQL (Select) Query 

```
(text:string)(non-text-limit:int)[(fetch-plan:string)](serialized-params:bytes[])
```
**text** text of the select query  
**non-text-limit**  Limit can be set in query's text, or here. This field had priority. Send -1 to use limit from query's text  
**fetch-plan** used only for select queries, otherwise empty  
**serialized-params** the byte[] result of the serialization of a [ODocument](#serialized-parameters-odocument-content).


##### Serialized Parameters ODocument content
The ODocument have to contain a field called "params" of type Map.  
the Map should have as key, in case of positional perameters the numeric position of the parameter, in case of named parameters the name of the parameter and as value the value of the parameter.

### SQL Commands
```
(text:string)(has-simple-parameters:boolean)(simple-paremeters:bytes[])(has-complex-parameters:boolean)(complex-parameters:bytes[])
```
**text** text of the sql command  
**has-simple-parameters** boolean flag for determine if the **simple-parameters** byte array is present or not  
**simple-parameters** the byte[] result of the serialization of a [ODocument](#serialized-simple-parameters-odocument-content).  
**has-complex-parameters** boolean flag for determine if the **complex-parameters** byte array is present or not  
**complex-parameters** the byte[] result of the serialization of a [ODocument](#serialized-complex-parameters-odocument-content).  

##### Serialized Simple Parameters ODocument content
The ODocument have to contain a field called "parameters" of type Map.  
the Map should have as key, in case of positional perameters the numeric position of the parameter, in case of named parameters the name of the parameter and as value the value of the parameter.

##### Serialized Complex Parameters ODocument content
The ODocument have to contain a field called "compositeKeyParams" of type Map.  
the Map should have as key, in case of positional perameters the numeric position of the parameter, in case of named parameters the name of the parameter and as value a List that is the list of composite parameters.

### Script 

```
(language:string)(text:string)(has-simple-parameters:boolean)(simple-paremeters:bytes[])(has-complex-parameters:boolean)(complex-parameters:bytes[])
```
**language** the language of the script present in the text field.
All the others paramenters are serialized as the [SQL Commands](#SQL_Commands)
