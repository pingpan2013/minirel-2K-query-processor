## Minirel 2K Query Processor 
This is the README file for the final project of EECS484 Database Management Systems in University of Michigan Ann Arbor(the 2014 winter term).

Implement a query processor and some basic utilities for the Minirel2K system, which is a simple single-user DBMS that accepts a (small)
subset of SQL. The implemented sql commands include:

* Insert
* Select, including ScanSelect (scanselect.cpp) and IndexSelect (indexselect.cpp)
* Join, including Simple Nested-loops Join (SNL.cpp), Sort-Merge Join (SMJ.cpp), and Indexed Nested-loops Join (INL.cpp)

Refer the spec in the docs folder for details.


### Installation
Note: g++ 4.7 or above is needed to compile the program.

In linux terminal, first compile the program with the provided makefile:
```
$make
```

### Usage
After compiling, the DBMS system will include three excutable programs for running from the command line.

The following excutable creates the database dbname:
```
$dbcreate <dbname>
```

The following excutable allows you to interact with the database named dbname by writing(simple) SQL commands. Use [SQL-file] to specify the sql file name. Note that concurrency is not supported in this version
```
$minirel <dbname> [SQL-file]
```

Finally, you need to use the following excutable to destory the database:
```
$dbdestroy <dbname>
```

Note that we offered some simple scripts to test if the system works correctly. Simply replace the [myDB] with your database name <dbname> in the scripts. The folder sql offers some testing sql files.

### Group Members
* Pingpan Cheng
* Bowei Xu  

### License
Published under GNU license.

