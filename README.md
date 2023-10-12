# go-db
Database ORMish Package for Go

Anotation based ORM

Annotations supported:
 *column
 *the name of the column of the struct member.
 *usage: column:"id"
 
 *datatype
 *the sql datatype or named datatype of the struct member
 *usage: datatype:"uuid.UUID" or datatype:"string"
 
 *primaryKey
 *whether the struct member is a primary key
 *usage: primaryKey:"true"
 
 *foreignKey
 *the name of the foreign key the struct member represents
 *usage: foreignKey:"true"
 
 *references
 *the table the foreign key references
 *usage: refereces:"some_table"
 
 *null
 *whether the column can be null
 *usage: null:"false"
 
 *default
 *what the default for the column is
 *usage: default:"NOW()"
 
 *join
 *representation of a join table between 2 tables
 *usage: join:"table_1:id,table_2:id"
 
 *tableName
 *the table name for the join table
 *usage: tableName:"join_table_name"
