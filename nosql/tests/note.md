# Note
## Storage System
- Save database in several files as chunks
- File name should be {database_name}_{collection_name}_number.json
  - if one file exceeds a certain size the data will be saved in a new file
    - {database_name}_{collection_name}_1.json
    - {database_name}_{collection_name}_2.json

## Query Language
### Insert
#### Insert a single item  
``insertOne '{collection}' '{data}'``  
Example: ``insert 'students' '{"id": "s100", "name": "foo"}'``

#### Insert multiple items
``insertMany '{collection}' '[list of data]'`` 

### Read
``find '{query}'``

### Update
#### Update a single item
``updateOne '{collection}' '{condition}' set '{new data}'``

#### Update multiple items
``updateMany '{collection}' '{condition}' set '{new data}'``

### Delete
#### Delete a single item
``deleteOne '{collection}' '{condition}' ``

#### Delete multiple items
``deleteMany '{collection}' '{condition}'``



