#!/usr/bin/env python
# coding: utf-8

# In[121]:


import sqlparse as sp
import re
import csv
import copy as cp


# In[122]:


#Reading metadata 
fp = open("metadata.txt", "r")
tables = [] #List to store tables in database
meta = {} #Dictionary to store table( index ), with there all columns with ordering( value ) as meta[ table ] = [ columns ]

while fp.readline()[ : -1 ] == "<begin_table>":

    table = fp.readline()#reading name of table
    table = table[ : -1 ]
    
    tables.append( table )#Inserting name in list of tables
    x = fp.readline()
    x = x[ : -1 ]
    
    meta[ table ] = []
    while x :#Reading for columns of table
        if x == "<end_table>" or x == "<end_table" :
            break ;
            
        meta[ table ].append( x )
        x = fp.readline()
        x = x[ : -1 ]
        
i = 0
#Printing table with their column names
while i < len( tables ) :
    print(tables[ i ])
    print( meta[ tables[ i ] ] )
    i = i + 1
        


# In[143]:


def check_aggr( cols ) :
    if re.match("^(count|max|sum|avg|min)\((\w|.)+\)$", cols.lower() ) :# For aggregate functions 
        i = cols.find("(")
        cols = cols[ i + 1 : -1 ]
        cols = rem_space( cols )
        return cols
    return cols

def table_included( cols ) :
    if "." in cols :
        if cols.count(".") > 1 :
            raise SystemExit("Invalid column name ")
        else :
            j = cols.index(".")
            table = cols[  : j  ]
            column = cols[ j + 1 : ]
            
#             table = table.replace(" ", "" )
            if table not in req_tables :
                raise SystemExit("Table not found")
            
#             column = column.replace(" ", "" )
            if column not in meta[ table ] :
                raise SystemExit("Column not found")
                
            return column    
            
    
    return cols 

def validate_where( where_part ) :
    col_1 = ""
    col_2 = ""
#     print("In validate_where ")
    if "<=" in where_part :
        i = where_part.index("<")
         
        first = where_part[ : i ]       
        second = where_part[ i + 2 : ]
        
        first = rem_space( first )
        second = rem_space( second )
         
        col_1 = check_aggr( first )
        col_2 = check_aggr( second )
            
#         print("First :", col_1)
#         print("Column :", col_2)
        col_1 = table_included( col_1 )
        col_2 = table_included( col_2 )
            
    elif ">=" in where_part :
        i = where_part.index(">")
          
        first = where_part[ : i ]            
        second = where_part[ i + 2 : ]

        first = rem_space( first )
        second = rem_space( second )
        
        col_1 = check_aggr( first )
        col_2 = check_aggr( second )
            
#         print("First :", col_1)
#         print("Column :", col_2)
        col_1 = table_included( col_1 )
        col_2 = table_included( col_2 )
            
    elif "=" in where_part :
        i = where_part.index("=")
            
        first = where_part[ : i ]
        second = where_part[ i + 1 : ]
         
        first = rem_space( first )
        second = rem_space( second )    
            
        col_1 = check_aggr( first )
        col_2 = check_aggr( second )
            
#         print("First :", col_1)
#         print("Column :", col_2)
        col_1 = table_included( col_1 )
        col_2 = table_included( col_2 )
            
    elif "<" in where_part :
        i = where_part.index("<")
            
        first = where_part[ : i ]
        second = where_part[ i + 1 : ]
         
        first = rem_space( first )
        second = rem_space( second )    
            
        col_1 = check_aggr( first )
        col_2 = check_aggr( second )
            
#         print("First :", col_1)
#         print("Column :", col_2)
        col_1 = table_included( col_1 )
        col_2 = table_included( col_2 )
            
    elif ">" in where_part :
        i = where_part.index(">")
            
        first = where_part[ : i ]
        second = where_part[ i + 1 : ]
        
        first = rem_space( first )
        second = rem_space( second )
        
        col_1 = check_aggr( first )
        col_2 = check_aggr( second )
            
#         print("First :", col_1)
#         print("Column :", col_2)
        col_1 = table_included( col_1 )
        col_2 = table_included( col_2 )
    else :
        raise SystemExit("Unsported operator found")
            
    match_1, match_2 = 0, 0   
    for table in req_tables :

        if match_1 == 0 and col_1 in meta[ table ] :
            match_1 = 1
        elif match_1 == 1 and col_1 in meta[ table ] and "." not in first :
            raise SystemExit("Ambigous column name in where ")
            
        if match_2 == 0 and col_2 in meta[ table ] :
            match_2 = 1
        elif match_2 == 1 and col_2 in meta[ table ] and "." not in second :
            raise SystemExit("Ambigous column name in where ")     
            
    if col_2.isdigit() and col_1.isdigit() :
        raise SystemExit("Error in where, both operands are numbers")
            
    if ( not col_1.isdigit() and match_1 == 0 ) or ( not col_2.isdigit() and match_2 == 0 ) :    
        if match_1 == 0 :
            print(col_1)
        else :
            print(col_2)  
        raise SystemExit("Columns not found ")

def rem_space( tab ) :
        while tab[ 0 ] == " " :
            tab = tab[ 1 : ]
        while tab[ -1 ] == " " :
            tab = tab[ : -1 ]
        return tab
     
query = "select distinct * from table1, table2 where  D >=  C or A < D;"

query = sp.format( query, keyword_case = "upper" )

distinct = 0 #to be used in query execution 
aggr = 0 #To be used in query execution 

#Testing for semicolon
if query[ len( query ) - 1 ] == ";" :
    query = query[ : -1 ]
else :
    raise SystemExit("Query terminated incorrectly " )
#Parsing sql query    
parsed = sp.parse( query )[ 0 ] 
tokens = parsed.tokens
ll = sp.sql.IdentifierList( tokens ).get_identifiers()
query_tokenized = []
for i in ll :
    query_tokenized.append( str(i) )
#Parsing is done

#Checking the syntax of query
#Parser itself checks for select from and where and splits the query accordingly and we had stored the splits in 
#query_tokenized list, hence we can dircly check for slect/from/where by just testing list's length, as it will 
#break accordingly
print(query_tokenized)
if query_tokenized[ 0 ].lower() == "select" : 
    if len( query_tokenized ) < 4 :
        raise SystemExit("Syntax error( short in length )")

    if query_tokenized[ 1 ].lower() == "distinct" :
        if( query_tokenized[ 3 ].lower() == "from" ) :
            if len( query_tokenized ) == 6 :
                if query_tokenized[ 5 ][ : 5 ].lower() == "where" :
                    pass
                else :
                    raise SystemExit("Syntax error( Error in where part )")
        else :
            raise SystemExit("Syntax error( Error in from part )")  
    else :
        if( query_tokenized[ 2 ].lower() == "from") :
            if len( query_tokenized ) == 5 :
                if query_tokenized[ 4 ][ : 5 ].lower() == "where"  :
                    pass
                else :
                    raise SystemExit("Syntax error")
        else :
            raise SystemExit("Syntax error")
else :
    raise SystemExit("Syntax error")

        
#Fetching out required tables in query, after eliminating space
req_tables = []
output= []
if query_tokenized[ 1 ].lower() == "distinct" :
    
    temp = query_tokenized[ 2 ].split(",")
    for tok in temp :
        rem = rem_space( tok )
        output.append( rem )
        
    temp = query_tokenized[ 4 ].split(",")
    for tok in temp :
        rem = rem_space( tok )
        if rem in req_tables :
            raise SystemExit("Same table entered multiple time ")
        req_tables.append( rem )
else :    
    temp = query_tokenized[ 1 ].split(",")
    for tok in temp :
        rem = rem_space( tok )
        output.append( rem )
        
    temp = query_tokenized[ 3 ].split(",")
    for tok in temp :
        rem = rem_space( tok )
        if rem in req_tables :
            raise SystemExit("Same table entered multiple time ")
        req_tables.append( rem )

# req_tables = []

# for table in raw_tables :
#     table = table.replace(" ", "" )
#     req_tables.append( table )

#Checking if the required tables exist in database
for table in req_tables :
    if table not in tables :
        raise SystemExit( " Table not found " )

#Checking if columns exist or not from select part
for cols in output :
#     print("For ", cols)
#     cols = cols.replace(" ", "" )
    match = 0
    
    if cols.lower() == "distinct" :#we are not storing  first distinct keyword in 'output' list
        raise SystemExit("More than one distinct")
        
    elif cols == "*" :
        continue
        
    elif re.match("^(count|max|sum|avg|min)\((\w|.)+\)$", cols.lower() ) :# For aggregate functions
        aggr = 1 
#         match = 0
        i = cols.find("(")
        cols = cols[ i + 1 : -1 ]
        cols = rem_space( cols )
#         print("Column")
#         print(cols)
        if "." in cols :
            table_included( cols )
        else :
#             print("Req-tables\n",req_tables)
            for table in req_tables :
                for column in meta[ table ] :
                    if cols == column :
                        if match == 1 :
#                             print("Amb table = ", table )
#                             print("With col = ", column)
                            raise SystemExit( "Ambiguous column name " )
                        else: 
#                             print("Table : ")
#                             print(table)
#                             print("With col = ")
#                             print(column)
                            match = 1 

            if match == 0 :
                raise SystemExit("Column not found")
            
    elif "." in cols : #For columns with table_name.column_name style
        table_included( cols )
    
    else :
#         match = 0
        for table in req_tables :
            for column in meta[ table ] :
                if cols == column :
                    if match == 1 :
                        raise SystemExit( "Ambiguous column name " )
                    match = 1 

        if match == 0 :
            raise SystemExit("Column not found")

#Validation of 'where' part
# print("Check\n",query_tokenized[ 4 ][ : 4 ])
if( ( len( query_tokenized ) == 5 and query_tokenized[ 4 ][ : 5 ].lower() == "where" ) or ( len( query_tokenized ) == 6 and query_tokenized[ 5 ][ : 5 ].lower() == "where" ) ) :
    if len( query_tokenized ) == 5 :
        where_part = query_tokenized[ 4 ] 
    elif len( query_tokenized ) == 6 :
        where_part = query_tokenized[ 5 ]

    where_part = where_part[ 6 : ]
#     print("Where part :")
#     print(where_part)
    if " and " in where_part.lower() :
        ll = where_part.split( " AND " )
        validate_where( ll[ 0 ] )
        validate_where( ll[ 1 ] )

    elif " or " in where_part.lower() :
        ll = where_part.split( " OR " )
        validate_where( ll[ 0 ] )
        validate_where( ll[ 1 ] )

    else :
        validate_where( where_part )
    
            
print(query_tokenized)  
print("Output list : ", output )
print("Required tables : ", req_tables )


# In[144]:


#GENERELAISING column names

#output has all columns which should be printed in order
#req_tables contains all the tables which are required in query
#meta is a dictionary which has table-name as index and a list of its columns as value
#query_tokenized has different elements of query

#modifying column names to be selected to table_name.column_name form and storing it in out_col list
out_col = []
for col in output :
    tab = []
    if "." not in col : 
        if re.match("^(count|max|sum|avg|min)\((\w|.)+\)$", col.lower() ) :# For aggregate functions
            name = check_aggr( col )
            i = col.index("(")
            col_temp = col[ i + 1 : ]
            col_temp = col_temp[ : -1 ]
            #Removing space from ends
            col_temp = rem_space( col_temp )
            
            for table in req_tables :
                for column in meta[ table ] :
                    if col_temp == column :
                        col_temp = table + "." + col_temp 
                        break
            col = col[  : i + 1 ] + col_temp +")"
            
            out_col.append( col )#storing in out_col list
            
        elif "*" in col :
            col = rem_space( col )
            out_col.append( col )
        else :
            #Removing space from ends
            col_temp = col
            for table in req_tables :
                for column in meta[ table ] :
                    if col_temp == column :
                        col_temp = table + "." + col_temp
                        break
            col = col_temp
            
            out_col.append( col )#storing in out_col list
            
            
print("Columns for output :",out_col )

#Modifying columns in 'where' clause to table_name.column_name form,
conditions = []
# if( query_tokenized[ 1 ].lower() == "distinct" ):
#     pass
if ( ( not( query_tokenized[ 1 ].lower() == "distinct") and len( query_tokenized ) == 5 ) ) or ( query_tokenized[ 1 ].lower() == "distinct" and len( query_tokenized ) == 6 )  :
    where_part = query_tokenized[ -1 ] #taking where part out of query_tokenized list and eliminating 'where' from it
    where_part = where_part[ 5 : ]
    where_part = rem_space( where_part )
    conds = []
    #breaking conditions and storing them seperatly in a list, to make modification easy
    if "and" in where_part.lower() :
        conds = where_part.split(" AND ")
        conds.append("AND")
        
    elif "or" in where_part.lower() :
        conds =  where_part.split(" OR ")
        conds.append("OR")
        
    else:
        conds.append( where_part )
        
    it = 0
#     print("Cond list ", conds )
    while( it < len( conds ) and it < 2 ) :#Breaking each condition on the basis of there operation
        op = ""
        operands = []
        cnd = conds[ it ]
#         print("Iteration number ", it )
        
        if "<=" in cnd :
            operands = cnd.split("<=")
            op = "<="
        elif ">=" in cnd :
            operands = cnd.split(">=")
            op = ">="
        elif "<" in cnd :
            operands = cnd.split("<")
            op = "<"
        elif ">" in cnd :
            operands = cnd.split(">")
            op = ">"
        elif "=" in cnd :
            operands = cnd.split("=")
            op = "="
            
        col_op = []#For storing [ operand_1, operand_2, operation ] for each condition, and then it would be appended in conditions list
#         print("\n\nFor operands :", operands )
        for col in operands :
            col = rem_space( col )
#             print( "In for ", col)
            if "." not in col : 
#                 print("1")
                if re.match("^(count|max|sum|avg|min)\((\w|.)+\)$", col.lower() ) :# For aggregate functions
                    raise SystemExit("Inappropriate use of aggregation")
#                     print("2")
#                     name = check_aggr( col )
#                     i = col.index("(")
#                     col_temp = col[ i + 1 : ]
#                     col_temp = col_temp[ : -1 ]
                    #Removing space from ends
#                     col_temp = rem_space( col_temp )
    
#                     for table in req_tables :
#                         for column in meta[ table ] :
#                             if col_temp == column :
#                                 col_temp = table + "." + col_temp 
#                                 break
#                     i = col.index("(")            
#                     col = col[  : i + 1 ] + col_temp +")"
#                     print("Appending column :", col)
#                     col_op.append( col )#appending operands in col_op list
                else :
#                     print("2e")
                    #Removing space from ends
                    col_temp = col
                    for table in req_tables :
#                         print("Serachin in ", table)
                        for column in meta[ table ] :
                            if col_temp == column :
                                col_temp = table + "." + col_temp
                                print(col_temp)
                                break
                    col = col_temp
                    col_op.append( col )#appending operands in col_op list
            else :
                col_temp = table_included( col )
                i = col.index(".")
                if col_temp == col[ i + 1 : ] :
                    col_op.append( col )
                
        col_op.append( op )            
        conditions.append( col_op )#Storing each condition tuple in main 'conditions' list which will be used further
        
        it = it + 1 
        
    if len( conds ) == 3 :
        conditions.append( conds[ 2 ] ) 
        
print("Where conditions : ", conditions)
    


# In[153]:


def cond_eval( var_1, var_2, cond ) :
    if cond == "<=":
        if x <= y :
            return 1 
        else:
            return 0
                            
    if cond == ">=":
        if x >= y :
            return 1 
        else:
            return 0
                            
    if cond == "=":
        if x == y :
            return 1 
        else:
            return 0
                            
    if cond == "<":
        if x < y :
            return 1 
        else:
            return 0
                            
    if cond == ">":
        if x > y :
            return 1 
        else:
            return 0





csv_reads = {}#creating dictionary so that we can directly read the data of table by just providing table name
for table in tables :
    file = table + ".csv"
    csv_reads[ table ] = []
    with open( file, "r" ) as ff :
        temp = csv.reader( ff, delimiter=',' )
        for row in temp :
            csv_reads[ table ].append( row )

rem_data = []
header = []
#Initializing 'rem_data' list of list, which will be used for printing the results
for head in meta[ req_tables[ 0 ] ] :
    name = req_tables[ 0 ] + "." + head
    header.append( name )
rem_data.append( header )

for row in csv_reads[ req_tables[ 0 ] ] :
    rem_data.append( row ) #Initialized with first table entries
    
    
#Taking cartesian product if 'req_tables' has more than one table entery
temp_rem = []
temp = []
if len( req_tables ) > 1 :
    i = 1 
    j = 1
    while( i < len( req_tables ) ) :
        header = []
        #Including header of different tables 
        for head in meta[ req_tables[ i ] ] :
            name = req_tables[ i ] + "." + head
            header.append( name )
        
        temp = cp.deepcopy( rem_data[ 0 ] )
        temp.extend( header )
        temp_rem.append( temp )
        
        while j < len( rem_data ) :
            
            for row in csv_reads[ req_tables[ i ] ] :
                temp = []
                temp = cp.deepcopy( rem_data[ j ] )
                temp.extend( row )
                temp_rem.append( temp )
            
            j = j + 1
        
        i = i + 1 
        rem_data = temp_rem
        
if len( conditions ) > 0 :
    temp_c1 = []
    temp_c2 = []
    
    if len( conditions ) == 1 :
        operand_1 = conditions[ 0 ][ 0 ]
        operand_2 = conditions[ 0 ][ 1 ]
        print(operand_1)
        print(operand_2)
        
        if operand_1.isdigit() or operand_2.isdigit() :
            #One of the operand is digit
            temp_c1 = []
            temp_c1.append( rem_data[ 0 ] )
                
            if operand_1.isdigit() :
                x = int( operand_1 )
                i_2 = rem_data[ 0 ].index( operand_2 )
                i_1 = -1
            else :
                y = int( operand_2 )
                i_1 = rem_data[ 0 ].index( operand_1 )
                i_2 = -1
                
            j = 1 
            while j < len( rem_data ) :
                t = []
                t = rem_data[ j ]
                if i_1 == -1 :
                    y = t[ i_2 ]
                else :
                    x = t[ i_1 ]
                    
                if cond_eval( x, y, conditions[ 0 ][ 2 ] ) :
                    temp_c1.append( t )
                    
                j = j + 1
                
            del rem_data[:]
            rem_data = cp.deepcopy( temp_c1 )
            
            
            
        else : #Both are Column name( Possibility of join operation )
            i_1 = operand_1.index(".")
            t_1 = operand_1[ : i_1 ]
            i_2 = operand_2.index(".")
            t_2 = operand_2[ : i_2 ]
#             print("Tables")
#             print(t_1)
#             print(t_2)
            
            if not( t_1 == t_2 ) and conditions[ 0 ][ 2 ] == "=" :
                i_1 = rem_data[ 0 ].index( operand_1 )
                i_2 = rem_data[ 0 ].index( operand_2 )
                
                temp_c1.append( [ operand_1 ] )
                ll = []
                ll = cp.deepcopy( rem_data[ 0 ] )
                ll.pop( i_1 )
                ll.pop( i_2 )
                temp_c1[ 0 ].extend( ll )
#                 print("temp_c1", temp_c1[0] )
                j = 1 #For indexing the rem_data for joining
                while j < len( rem_data ) :
                    t = []
                    del ll[:]
                    
                    t = cp.deepcopy( rem_data[ j ] )
#                     print(t)
                    if t[ i_1 ] == t[ i_2 ] :
                        ll.append( t[ i_1 ] ) 
                        t.pop( i_1 )
                        t.pop( i_2 )
                        ll.extend( t )
#                         print("Appending ", ll)
                        temp_c1.append( ll )
                    j = j + 1    
                
                del rem_data[:]
#                 print("temp_c1 during deepcopy ", temp_c1 )
                rem_data = cp.deepcopy( temp_c1 )
                
            else :
                temp_c1 = []
                temp_c1.append( rem_data[ 0 ] )
                i_1 = rem_data[ 0 ].index( operand_1 )
                i_2 = rem_data[ 0 ].index( operand_2 )
                
                j = 1 
                while j < len( rem_data ) :
                    t = []
                    t = rem_data[ j ]
                    x = t[ i_1 ]
                    y = t[ i_2 ]
                    
                    if cond_eval( x, y, conditions[ 0 ][ 2 ] ) :
                        temp_c1.append( t )
                    
                    j = j + 1
                
                del rem_data[:]
                rem_data = cp.deepcopy( temp_c1 )
    
    elif len( conditions ) == 3 :
        first = conditions[ 0 ]
        second = conditions[ 1 ]
        temp_c1 = [] #for result of first condition
        temp_c2 = [] #for result of second condition
        
        #For first condition
        
        operand_1 = first[ 0 ]
        operand_2 = first[ 1 ]
        print(operand_1)
        print(operand_2)
        
        if operand_1.isdigit() or operand_2.isdigit() :
            #One of the operand is digit
                temp_c1.append( rem_data[ 0 ] )
                
                if operand_1.isdigit() :
                    x = int( operand_1 )
                    i_2 = rem_data[ 0 ].index( operand_2 )
                    i_1 = -1
                else :
                    y = int( operand_2 )
                    i_1 = rem_data[ 0 ].index( operand_1 )
                    i_2 = -1
                
                j = 1 
                while j < len( rem_data ) :
                    t = []
                    t = rem_data[ j ]
                    if i_1 == -1 :
                        y = t[ i_2 ]
                    else :
                        x = t[ i_1 ]
                    
                    if cond_eval( x, y, first[ 2 ] ) :
                        temp_c1.append( t )
                    
                    j = j + 1
                
#                 del rem_data[:]
#                 rem_data = cp.deepcopy( temp_c1 )
            
            
            
        else : #Both are Column name( Possibility of join operation )
            i_1 = operand_1.index(".")
            t_1 = operand_1[ : i_1 ]
            i_2 = operand_2.index(".")
            t_2 = operand_2[ : i_2 ]
#             print("Tables")
#             print(t_1)
#             print(t_2)
            
            if not( t_1 == t_2 ) and first[ 2 ] == "=" : #t_1 and t_2 are different tables and operation is '=' hence JOIN
                
                raise SystemExit("Inappropriate use of join condition")
                
                i_1 = rem_data[ 0 ].index( operand_1 )
                i_2 = rem_data[ 0 ].index( operand_2 )
                
                temp_c1.append( [ operand_1 ] )
                ll = []
                ll = cp.deepcopy( rem_data[ 0 ] )
                ll.pop( i_1 )
                ll.pop( i_2 )
                temp_c1[ 0 ].extend( ll )
#                 print("temp_c1", temp_c1[0] )
                j = 1 #For indexing the rem_data for joining
                while j < len( rem_data ) :
                    t = []
                    del ll[:]
                    
                    t = cp.deepcopy( rem_data[ j ] )
#                     print(t)
                    if t[ i_1 ] == t[ i_2 ] :
                        ll.append( t[ i_1 ] ) 
                        t.pop( i_1 )
                        t.pop( i_2 )
                        ll.extend( t )
#                         print("Appending ", ll)
                        temp_c1.append( ll )
                    j = j + 1
                
            else :
                temp_c1 = []
                temp_c1.append( rem_data[ 0 ] )
                i_1 = rem_data[ 0 ].index( operand_1 )
                i_2 = rem_data[ 0 ].index( operand_2 )
                
                j = 1 
                while j < len( rem_data ) :
                    t = []
                    t = rem_data[ j ]
                    x = t[ i_1 ]
                    y = t[ i_2 ]
                    
                    if cond_eval( x, y, first[ 2 ] ) :
                        temp_c1.append( t )
                    
                    j = j + 1
                    
        #For second condition
        operand_1 = second[ 0 ]
        operand_2 = second[ 1 ]
        print(operand_1)
        print(operand_2)
        
        if operand_1.isdigit() or operand_2.isdigit() :
            #One of the operand is digit
                temp_c2.append( rem_data[ 0 ] )
                
                if operand_1.isdigit() :
                    x = int( operand_1 )
                    i_2 = rem_data[ 0 ].index( operand_2 )
                    i_1 = -1
                else :
                    y = int( operand_2 )
                    i_1 = rem_data[ 0 ].index( operand_1 )
                    i_2 = -1
                
                j = 1 
                while j < len( rem_data ) :
                    t = []
                    t = rem_data[ j ]
                    if i_1 == -1 :
                        y = t[ i_2 ]
                    else :
                        x = t[ i_1 ]
                    
                    if cond_eval( x, y, second[ 2 ] ) :
                        temp_c1.append( t )
                    
                    j = j + 1
                
#                 del rem_data[:]
#                 rem_data = cp.deepcopy( temp_c1 )
            
            
            
        else : #Both are Column name( Possibility of join operation )
            i_1 = operand_1.index(".")
            t_1 = operand_1[ : i_1 ]
            i_2 = operand_2.index(".")
            t_2 = operand_2[ : i_2 ]
#             print("Tables")
#             print(t_1)
#             print(t_2)
            
            if not( t_1 == t_2 ) and second[ 2 ] == "=" : #t_1 and t_2 are different tables and operation is '=' hence JOIN
                
                raise SystemExit("Inappropriate use of join condition")
                
                i_1 = rem_data[ 0 ].index( operand_1 )
                i_2 = rem_data[ 0 ].index( operand_2 )
                
                temp_c2.append( [ operand_1 ] )
                ll = []
                ll = cp.deepcopy( rem_data[ 0 ] )
                ll.pop( i_1 )
                ll.pop( i_2 )
                temp_c2[ 0 ].extend( ll )
#                 print("temp_c1", temp_c1[0] )
                j = 1 #For indexing the rem_data for joining
                while j < len( rem_data ) :
                    t = []
                    del ll[:]
                    
                    t = cp.deepcopy( rem_data[ j ] )
#                     print(t)
                    if t[ i_1 ] == t[ i_2 ] :
                        ll.append( t[ i_1 ] ) 
                        t.pop( i_1 )
                        t.pop( i_2 )
                        ll.extend( t )
#                         print("Appending ", ll)
                        temp_c2.append( ll )
                    j = j + 1    
                
#                 del rem_data[:]
#                 print("temp_c1 during deepcopy ", temp_c1 )
#                 rem_data = cp.deepcopy( temp_c1 )
                
            else :
                temp_c2 = []
                temp_c2.append( rem_data[ 0 ] )
                i_1 = rem_data[ 0 ].index( operand_1 )
                i_2 = rem_data[ 0 ].index( operand_2 )
                
                j = 1 
                while j < len( rem_data ) :
                    t = []
                    t = rem_data[ j ]
                    x = t[ i_1 ]
                    y = t[ i_2 ]
                    
                    if cond_eval( x, y, first[ 2 ] ) :
                        temp_c2.append( t )
                    
                    j = j + 1
        
        del rem_data[:]
        rem_data.append( temp_c1[ 0 ] )
        
        if conditions[ 2 ].lower() == "and" :
            i = 0
            for row in temp_c1 :
                
                if i == 0 : #To avoid first row i.e., header
                    i = 1
                    continue
                    
                if row in temp_c2 :
                    rem_data.append( row )
                    
        if conditions[ 2 ].lower() == "or" :
            i = 0
            for row in temp_c1 :
                
                if i == 0 :
                    i = 1
                    continue
                    
                rem_data.append( row )
                
            for row in temp_c2 :
                if row not in rem_data :
                    rem_data.append( row )
                    
    print("Conditions are there")

agg_data = cp.deepcopy( rem_data ) #Data remaining aftyer 'where' clause is copied to be used by aggregation function
#After executing all the conditions in 'where'
temp_1 = []
agg = 0 
if len( rem_data ) == 1 : #If after running a condition no data rows were left, then this condition will run
    ll = []
    hh = []
    for head in out_col :
        if head == "*" :
            hh.extend( rem_data[ 0 ] )
            for x in rem_data[ 0 ] :
                ll.append("null")
        else :
            hh.append(head)
            ll.append("null")
    
    temp_1.append( hh )
    temp_1.append( ll )
else :    #If after executing 'where' clause data is left in rem_data, then this condition will be executed 
    for col in out_col :
        temp_2 = []
        if col == "*" :
    #         print("In for ", col, " with agg = ", agg)
            if agg == 1 :
                temp_1[ 0 ].extend( rem_data[ 0 ] )
                temp_1[ 1 ].extend( rem_data[ 1 ] )


        elif re.match("^(count|max|sum|avg|min)\((\w|.)+\)$", col.lower() ) :# For aggregate function
            agg = 1
            name = check_aggr( col )
            agg_op = col[ : 3 ]
            agg_col_data = []
            i = agg_data[ 0 ].index( name )
            ind = 1
            while ind < len( agg_data ) :
                agg_col_data.append( agg_data[ ind ][ i ] )
                ind = ind + 1 

            if agg_op.lower() == "min" :
                agg_val = min( agg_col_data )

            if agg_op.lower() == "max" :
                agg_val = max( agg_col_data )

            if agg_op.lower() == "avg" :
                tt = 0
                for elem in agg_col_data :
                    tt = tt + int( elem )

                cc = len( agg_col_data )
                agg_val = tt / cc

            if agg_op.lower() == "sum" :
                tt = 0
                for elem in agg_col_data :
                    tt = tt + int( elem )

                agg_val = tt    

            del rem_data[:]
            rem_data.append( agg_data[ 0 ] )    
            ind = 1
            while ind < len( agg_data ) :

                if agg_data[ ind ][ i ] == agg_val :
                    rem_data.append( agg_data[ ind ] ) 
    #                 print("Found and inserted")
                    ind = len( agg_data )

                ind = ind + 1

            if ind == len( agg_data ) : #Comparing it because if value is found then ind will be one more than len( agg_data ), as we are storing len( agg_data ) in it, and after this, it will be incremented once 
    #             print("Previously not found ")
                rem_data.append( agg_data[ -1 ] ) #If no value is matched then last row is used in SQL

            if len( temp_1 ) > 0 :
                del temp_2[:]
                temp_2.append( temp_1[ 0 ] )
                ll = []

                for head in temp_2[ 0 ] :
                    if re.match("^(count|max|sum|avg|min)\((\w|.)+\)$", head.lower() ) : #in case one aggregation is already happend
                        i = temp_1[ 0 ].index( head )
                        val = temp_1[ 1 ][ i ] #If one aggregation is already happend then, temp_1 will have only two lists, one for header one for value 
                        ll.append( val )
                    else :    
                        i = rem_data[ 0 ].index( head ) 
                        ll.append( rem_data[ 1 ][ i ] )

                temp_2.append( ll )        

                del temp_1[:]
                temp_1 = cp.deepcopy( temp_2 )
                del temp_2[:]

                temp_1[ 0 ].extend( [ col ] )
                temp_1[ 1 ].extend( [ agg_val ] )

            else :
                temp_1.append( [ col ] )
                temp_1.append( [ agg_val ] )

        else :
            i = rem_data[ 0 ].index( col )
            ind = 0 
            while ind < len( rem_data ) :
                l = []
                l.append( rem_data[ ind ][ i ] )

                if len( temp_1 ) < len( rem_data ) : #Inserting first time
                    temp_1.append( l )

                elif len( temp_1 ) == len( rem_data ) : #temp_1 has values now we have to extend new values in it
                    li = cp.deepcopy( temp_1[ ind ] )
                    li.extend( l )
                    temp_2.append( li )

                ind = ind + 1

            if len( temp_2 ) == len( rem_data ) :
                del temp_1[:]
                temp_1 = cp.deepcopy( temp_2 )
                del temp_2[:]

            
if len( temp_1 ) > 0 :
    del rem_data[:]
    rem_data = cp.deepcopy( temp_1 )
    
if query_tokenized[ 1 ].lower() == "distinct" :
    dist = set()
    i = 0
    for row in rem_data :
        if i == 0 :
            i = 1
            print( row, "\n" )
            continue
            
        if row not in dist :
            dist.add( row )
            print(row,"\n")
        
else :
    for row in rem_data :
        print(row,"\n")
    


# In[ ]:




