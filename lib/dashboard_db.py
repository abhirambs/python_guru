#!/usr/bin/python
#
# Generic MySQL Database Interaction module
#
# Author: Abhiram Shivaswamy, Jan 2017
#
#

import os
import logging
import sqlalchemy
from sqlalchemy.pool import NullPool
import sqlsoup
import datetime
import sys
import config

import changeset

# These values are taken from
# http://dev.mysql.com/doc/internals/en/myisam-record-structure.html#myisam-column-attributes
class mySqlDataTypes:
    INT_8BIT = 1
    INT_16BIT = 2
    INT_32BIT = 3
    REAL = 5
    TIMESTAMP = 7
    INT_64BIT = 8
    DATETIME = 12
    BIT = 16
    VARCHAR = 253
    CHAR = 254

# It is possible to query SQLAlchemy/SQLSoup for properties on the columns of a
# recordset.  It returns an array of values for each column.  These are the
# meanings of each index in the array.  For COLUMN_DATATYPE, a number is returned.
# Refer to "mySqlDataTypes" above for the meanings of various numbers.
# This list is derived from here:
# http://www.python.org/dev/peps/pep-0249/#cursor-attributes
class mySQLColumnPropertyIndexes:
    COLUMN_NAME = 0
    COLUMN_DATATYPE = 1
    COLUMN_DISPLAYED_SIZE = 2
    COLUMN_INTERNAL_SIZE = 3
    COLUMN_PRECISION = 4
    COLUMN_SCALE = 5
    COLUMN_NULL_ALLOWED = 6

class mySQLResultSetIndexes:
    COLUMN_NAME = 0
    COLUMN_VALUE = 1


dashboard_db_connection = None

def get_dashboard_db_connection() :
    global dashboard_db_connection

    if dashboard_db_connection == None :
        dashboard_db_connection = dashboardDB()
        logging.debug("Got database connection.")
    return dashboard_db_connection

dashboard_engine_connection = None

def get_dashboard_engine_connection(db_type, db_user, db_password, db_host, db_port, db_name, db_socket_file=None) :
    global dashboard_engine_connection

    # Build the connection string according to the connection parameters:

    # Start with the database type, username, and password.
    # They will always be present:
    connectString = "%s://%s:%s@" % (db_type, db_user, db_password)

    # If there was no host specified, use "localhost":
    if db_host == None:
        connectString += "localhost"
    else:
        connectString += db_host

    # If there was a port specified, use it:
    if db_port != None:
        connectString += ":" + str(db_port)

    # Then add the database name.  This will always be present:
    connectString += "/" + db_name

    # If there was a customized socket file specified, add it:
    if db_socket_file != None:
        connectString += '?unix_socket=%s' % (db_socket_file, )

    logging.debug("connect String->%s< "% connectString)

    try:
        logging.debug("Connecting to the database.")

        # Create an SQLAlchemy "Engine" object (representing a database
        # connection).  Disable connection pooling by selecting a "null pool".
        # We need to disable connection pooling to ensure that when we
        # disconnect from the database with the "dashboardDB.close()" method, our
        # connection to the database is completely severed.  With connection
        # pooling enabled, when we attempt to close the connection, it will
        # actually be held open in anticipation of it being used by another
        # query.
        databaseEngine = sqlalchemy.create_engine(connectString,
            poolclass=NullPool)

        # Instantiate an "SQLSoup" object, which gives us a database session to
        # use:
        dashboard_engine_connection = sqlsoup.SQLSoup(databaseEngine)
        logging.debug("obtained engine connection")
    except:
        logging.error("can't connect to database server Exception type:%s Exception value:%s" %( sys.exc_info()[0] , sys.exc_info()[1] ))


    return dashboard_engine_connection


class dbException(Exception):

    def DatabaseError():
        pass

class dashboardDB:

    def __init__(self, db_host=config.db.DB_HOST, db_name=config.db.DB_NAME, db_user=config.db.DB_USER, db_password=config.db.DB_PASSWD, db_port=config.db.DB_PORT, db_socket_file=None):

        self.dbHost = db_host
        self.dbName = db_name
        self.dbUser = db_user
        self.dbPassword = db_password
        self.dbPort = db_port
        self.dbSocketFile = db_socket_file
        self.dbType = "mysql"
        self.engine = None


# ------------------------------------------------------------------------------

    # Insert a new record in the database in a specified table, optionally retrieving the
    # auto-generated ID field value inserted by the database system.
    #
    # Parameters:
    #     tableName               The name of the table into which to insert the new
    #                             record (string)
    #     fieldList               A Python dictionary of the fields and the values to
    #                             insert into them.  (To insert NULL, use the Python
    #                             constant "None".)
    #     getAutoInsertedValue    True if the function should attempt to retrieve and
    #                             return the value inserted by the database system for
    #                             the auto-generated field.  If the table does not have
    #                             an auto-generated field, set this to "False" or omit
    #                             it.  (Optional; defaults to False)
    #
    # Return value:
    #     The value returned for the auto-generated ID field value (if requested) or None.
    #
    #                                       failed.
    #
    def insert_new_record(self, tableName, fieldList, getAutoInsertedValue=False):
        

        # Assemble the list of field names and values:
        result = None
        fieldNameList = None
        fieldValuePlaceHolderList = None
        firstField = True

        # Convert and truncate fields as needed:
        convertedFields = self._preprocess_field_data(tableName, fieldList, {})[0]

        # The list of fields and their values that will be passed to the database:
        sqlParameters = {}

        # For each field, add its name to two lists.
        # One list ("fieldNameList") will be the list of fields before the keyword "VALUES" in the
        # SQL "INSERT" statement.  The other list will be the list of placeholder variables after
        # "VALUES" where the field values will be substituted:
        #
        # INSERT INTO MY_TABLE
        # (MY_ID_NUMBER, MY_STRING_FIELD, FIELD_3)        <-- fieldNameList
        # VALUES
        # (:my_id_number, :my_string_field, :field_3)     <-- fieldValuePlaceHolderList
        # ;

        # Iterate over every field:
        for singleFieldName in convertedFields:
            if firstField == True:
                # Before adding the first field, add the opening parenthesis:
                fieldNameList = "("
                fieldValuePlaceHolderList = "("
                firstField = False
            else:
                # Before every subsequent field, add the delimiting comma and a space:
                fieldNameList += ", "
                fieldValuePlaceHolderList += ", "

            # Add the field name to the first list:
            fieldNameList = fieldNameList + singleFieldName

            # For the second list, add the field name, but in lowercase and with a colon
            # preceding it:
            fieldValuePlaceHolderList = fieldValuePlaceHolderList \
                + ":" + singleFieldName.lower()

            # Add the field (in lowercase, like the in the second list) and its
            # corresponding value:
            sqlParameters[singleFieldName.lower()] = convertedFields[
                singleFieldName]


        # End the field lists with a closing parenthesis:
        fieldNameList += ")"
        fieldValuePlaceHolderList += ")"

        # Use the lists to build the entire SQL statement:
        sqlStatement = "INSERT INTO " + tableName + " " + fieldNameList + " " \
            + "VALUES " + fieldValuePlaceHolderList

        try:
            # Execute the INSERT statment:
            self.check_db_connect()
            self.engine.execute(sqlStatement, params=sqlParameters)

            if getAutoInsertedValue == True:
                # Retrieve the inserted value if it was requested:
                result = self._get_single_result("SELECT last_insert_id()", {})

            self.engine.commit()

        except Exception as ex:
            logging.error("Error: inserting new record in " "database:\n%s" % str(ex))

        # Return the inserted value (or None) if it was not requested:
        return result


# ------------------------------------------------------------------------------

    # Update an existing record in the database in a specified table.
    #
    # Parameters:
    #     tableName               The name of the table in which to update an
    #                             existing record (string)
    #
    #     whereFields             A Python dictionary of the fields and their
    #                             values with which to limit the update
    #                             operation (the WHERE clause).
    #
    #     setFields               A Python dictionary of the fields to update
    #                             and the new values to set them to.  (The "SET"
    #                             clause; to insert NULL, use the Python constant
    #                             "None".)
    #
    # Return value:
    #     (None)
    #
    # Raises:
    #     dbException.DatabaseError    If an error occurs during the update.
    #
    def update_record(self, tableName, whereFields, setFields):

        logging.debug("dashboardDB.update_record()")
        self.check_db_connect()

        # Convert the various field values from to appropriate types
        (processedSetFields, processedWhereFields) = self._preprocess_field_data(
            tableName, setFields, whereFields)

        # Assemble the "SET field = value, field = value, etc." clause of the
        # UPDATE statement.  Insert the field names directly into the SQL
        # statement, but for the values, insert a placeholder to be used by the
        # prepared-SQL system.  Put the values into a Python dictionary.
        parameters = {}
        (setClause, parameters) = self._assemble_sql_field_list_clause(
            processedSetFields, "", ", ", parameters)

        # Were there any fields to update?
        if setClause == None:
            # No.  Then there's nothing to update.  Just exit:
            return

        # Assemble the key fields:
        (whereClause, parameters) = self._assemble_sql_field_list_clause(
            processedWhereFields, "update_key_", " AND ", parameters)

        # Assemble the UPDATE statement:
        sqlQuery = "UPDATE " + tableName + " SET " + setClause + " " \
            + "WHERE " + whereClause + "; " \
            + "COMMIT;"

        logging.debug('dashboardDB.update_record: "%s"'
            % (self._interpolateSqlVariablesForLogging(sqlQuery, parameters), ))

        # Execute the UPDATE statement:
        try:
            self.engine.execute(sqlQuery, params=parameters)

        except Exception as ex:
            # Re-raise the exception using a Dashboard error (so the caller will not
            # need to know about SQLAlchemy exceptions).
            # Try to extract the original database error and insert it into
            # our error message:
            try:
                raise dbException.DatabaseError(
                    "Database error occurred: %s" % str(ex.orig))
            except AttributeError:
                # But if we couldn't extract the original database error
                # (because we didn't catch an SQLAlchemy exception), just raise
                # a Dashboard exception with the entire text of the original
                # exception:
                raise dbException.DatabaseError(
                    "Unable to perform update: %s" % str(ex))

# ------------------------------------------------------------------------------

    # Given a table name, determine the table's datatypes and field lengths.
    #
    # Parameters:
    #     tableName       The name of the table.
    #
    # Return Value:
    #     A dictionary with each column name as the key and its properties in an
    #     array as the values.  The meaning of each entry in the array is
    #     defined in the class mySQLColumnPropertyIndexes (defined at the top of
    #     this file.
    #
    def _get_table_column_properties(self, tableName):

        # Ensure this a connection to the database:
        self.check_db_connect()

        # To determine the datatypes and field lengths, we need to have a
        # cursor in the table.  The only way I could find to get one is to
        # execute a query against the table:
        resultSet = self.engine.execute("SELECT * FROM %s" % tableName)

        # Take the cursor and pass it to this function to get the datatypes and
        # field lengths:
        return self._get_query_column_properties(resultSet)

# ------------------------------------------------------------------------------

    # Update an existing record in the database in a specified table.
    #
    # Parameters:
    #     tableName               The name of the table in which to update an
    #                             existing record (string)
    #
    #     whereFields             A Python dictionary of the fields and their
    #                             values with which to limit the update
    #                             operation (the WHERE clause).
    #
    #     setFields               A Python dictionary of the fields to update
    #                             and the new values to set them to.  (The "SET"
    #                             clause; to insert NULL, use the Python constant
    #                             "None".)
    #
    # Return value:
    #     (None)
    #
    # Raises:
    #     dashbaordExceptions.DatabaseError    If an error occurs during the update.
    #
    def update_record(self, tableName, whereFields, setFields):

        logging.debug("dashboardDB.update_record()")
        self.check_db_connect()

        # Convert the various field values from to appropriate types
        (processedSetFields, processedWhereFields) = self._preprocess_field_data(
            tableName, setFields, whereFields)

        # Assemble the "SET field = value, field = value, etc." clause of the
        # UPDATE statement.  Insert the field names directly into the SQL
        # statement, but for the values, insert a placeholder to be used by the
        # prepared-SQL system.  Put the values into a Python dictionary.
        parameters = {}
        (setClause, parameters) = self._assemble_sql_field_list_clause(
            processedSetFields, "", ", ", parameters)

        # Were there any fields to update?
        if setClause == None:
            # No.  Then there's nothing to update.  Just exit:
            return

        # Assemble the key fields:
        (whereClause, parameters) = self._assemble_sql_field_list_clause(
            processedWhereFields, "update_key_", " AND ", parameters)

        # Assemble the UPDATE statement:
        sqlQuery = "UPDATE " + tableName + " SET " + setClause + " " \
            + "WHERE " + whereClause + "; " \
            + "COMMIT;"


        # Execute the UPDATE statement:
        try:
            self.engine.execute(sqlQuery, params=parameters)

        except Exception as ex:
            # Re-raise the exception using an  error (so the caller will not
            # need to know about SQLAlchemy exceptions).
            # Try to extract the original database error and insert it into
            # our error message:
            try:
                raise dashboardExceptions.DatabaseError(
                    "Database error occurred: %s" % str(ex.orig))
            except AttributeError:
                # But if we couldn't extract the original database error
                # (because we didn't catch an SQLAlchemy exception), just raise
                # an exception with the entire text of the original
                # exception:
                raise dashboardExceptions.DatabaseError(
                    "Unable to perform update: %s" % str(ex))



    def _python_to_mySQL_date(self, dateObject):
        result = None

        # If the value is not null:
        if dateObject != None:
            # Extract the various parts and insert them into a string in a format expected
            # by MySQL:
            result = "%04d-%02d-%02d %02d:%02d:%02d.%06d" % (dateObject.year,
                dateObject.month, dateObject.day, dateObject.hour,
                dateObject.minute, dateObject.second, dateObject.microsecond)

        # Return the string:
        return result

# ------------------------------------------------------------------------------

    # When inserting data into a BIT (boolean) field in MySQL and most DBMSes,
    # the value specified should be the number 0 or 1 for True and False.
    # Convert the Python boolean value to 0 or 1 as appropriate so this can be
    # insert into an INSERT or UPDATE SQL statement.
    #
    # Parameters:
    #     bitData       The boolean value, as a Python boolean type.
    #
    # Return Value:
    #     The MySQL literal representation of the value: 0 for False and 1 for
    #     True.
    #
    def _python_mysql_bit(self, bitData):
        result = '0'
        if bitData == True:
            result = '1'

        return result

# ------------------------------------------------------------------------------

    # MySQL returns the contents of fields with the datatype BIT as either
    # "\x00" or "\x01" a single byte with the value 0 or 1.  Convert this to a
    # Python boolean type.
    #
    # Parameters:
    #     bitData       The MySQL representation of the BIT data.
    #
    # Return Value:
    #     A Python boolean representation of the data.
    #
    def _mysql_python_bit(self, bitData):

        return bitData != "\x00"

    def get_query_results(self, sqlQuery, parameters):

        # Ensure there is a connection to the database:
        self.check_db_connect()

        # Perform the query and get the results from the database:
        resultSet = self.engine.execute(sqlQuery, params = parameters)

        # Collect the results and convert them to an array of dictionaries.  In
        # the process, this converts DATETIME and BIT fields to appropriate
        # representations for MySQL:
        return self._postprocess_query_data(resultSet)

    def _get_single_column_results(self, sqlQuery, parameters):

        self.check_db_connect()

        # Start with an empty array:
        result = []

        recordSet = self.engine.execute(sqlQuery, params=parameters)
        for singleRecord in recordSet:
            result.append(singleRecord[0])

        return result

    # For a database query that returns a recordset (result set) that contains
    # only one record with one column -- a single value -- execute the query and
    # return that single item.
    #
    # Parameters:
    #     sqlQuery      The SQL query to execute.
    #
    #     parameters    The variables to substitute into the query.  The SQL
    #                   query is prepared by the database system to help prevent
    #                   SQL-injection attacks.  Any words in the query preceded
    #                   by a colon (':') have their value substituted with the
    #                   specified value in the "parameters" dictionary.
    #
    # Return value:
    #     The result, as a string.  If the query did not return any records,
    #     returns None.
    #
    def _get_single_result(self, sqlQuery, parameters):
        result = None
        queryResults = self._get_single_column_results(sqlQuery, parameters)
        if len(queryResults) > 0:
            result = queryResults[0]

        return result

    # Given the result set from a query, determine the datatypes and field
    # lengths of its columns.
    #
    # Parameters:
    #     resultSet     The result object from a query that has just been run.
    #
    # Return Value:
    #     A dictionary with each column name as the key and its properties in an
    #     array as the values.  The meaning of each entry in the array is
    #     defined in the class mySQLColumnPropertyIndexes (defined at the top of
    #     this file.
    #
    def _get_query_column_properties(self, resultSet):
        results = {}
        for singleColumn in resultSet.cursor.description:
            results[singleColumn[0]] = singleColumn[:]

        return results


# ------------------------------------------------------------------------------

    # Used by update_record() to assemble the "SET" and "WHERE" clauses of the
    # UPDATE statement.  Given a list of fields and the values for them, this
    # method assembles the contents of the SET or WHERE clause.
    #
    # Parameters:
    #     fieldDictionary         A list of fields and their values.
    #
    #     fieldPlaceholderPrefix  A prefix to add to each field value
    #                             placeholder to help distinguish it in case
    #                             the same field appears twice in an SQL
    #                             statement.
    #
    #     fieldDelimiter          The delimiter to put between each field and
    #                             field value placeholder in the generated list.
    #                             For example, in this list:
    #
    #     inputDictionary         A dictionary of any pre-existing field value
    #                             placeholders and values from a previous call
    #                             to this method.  The new field value
    #                             placeholders and values from this call will be
    #                             added to this list.
    #
    # Return Value:
    #     A tuple consisting of the assembled SQL clause and a dictionary of the
    #     field placeholders and values.
    #
    def _assemble_sql_field_list_clause(self, fieldDictionary,
        fieldPlaceholderPrefix, fieldDelimiter, inputDictionary):

        sqlClause = None
        for fieldKey in fieldDictionary:
            # Have we already written any "field = value" pairs yet?
            if sqlClause != None:
                # Yes.  Put a comma after the last one to separate from the next
                # one:
                sqlClause = sqlClause + fieldDelimiter
            else:
                # No.  Use an empty string to start with:
                sqlClause = ""

            # Append the next field and its value placeholder:
            sqlClause = sqlClause + fieldKey + " = :" \
                + fieldPlaceholderPrefix + fieldKey.lower()

            # Put the value into a dictionary:
            inputDictionary[fieldPlaceholderPrefix + fieldKey.lower()] \
                = fieldDictionary[fieldKey]

        return (sqlClause, inputDictionary)


    def _preprocess_field_group(self, tableProperties, fieldGroup):

        fieldResults = {}

        # For each field whose data is about to be written:
        for singleField in fieldGroup:

            # Get its datatype:
            fieldDataType = tableProperties[singleField][
                mySQLColumnPropertyIndexes.COLUMN_DATATYPE]

            # Is this a VARCHAR field?
            if fieldDataType == mySqlDataTypes.VARCHAR:

                # Find out how long it can be:
                fieldMaxLength = tableProperties[singleField][
                    mySQLColumnPropertyIndexes.COLUMN_INTERNAL_SIZE]

                # Does the new value need to be truncated?  This will be the
                # case if it is not NULL and it is longer than the field length:
                if fieldGroup[singleField] != None \
                    and len(fieldGroup[singleField]) > fieldMaxLength:

                    # It's not NULL and it is too long.  Truncate it to fit:
                    fieldResults[singleField] \
                        = fieldGroup[singleField][0:fieldMaxLength - 1]
                else:
                    # It's either short enough or it's NULL.  Pass it thru
                    # without changes:
                    fieldResults[singleField] = fieldGroup[singleField]

            # If it's a DATETIME field, convert it from Python to MySQL format:
            elif fieldDataType == mySqlDataTypes.DATETIME:
                fieldResults[singleField] = self._python_to_mySQL_date(
                    fieldGroup[singleField])

            # If it's a BIT field, convert it to the representation used by
            # MySQL:
            elif fieldDataType == mySqlDataTypes.BIT:
                fieldResults[singleField] = self._python_mysql_bit(
                    fieldGroup[singleField])

            # For all other types, just pass them thru without any changes:
            else:
                fieldResults[singleField] = fieldGroup[singleField]

        return fieldResults

# ------------------------------------------------------------------------------

    # Before inserting data into an INSERT or UPDATE statement to write it to
    # the database, convert the values for DATETIME and BIT database fields from
    # their Python representations to appropriate MySQL representations.
    # Also, values to be inserted into VARCHAR fields are truncated to the field
    # length to avoid database errors.
    #
    # Parameters:
    #     tableName     The table into which the data will be inserted.
    #
    #     fields        A dictionary of fields whose values will be inserted.
    #                   The format of the dictionary is:
    #                       { field_name: field_value, field_name: field_value,
    #                         field_name: field_value, etc. }
    #                   For example:
    #                         'CHANGESET': 'cicd_demo1-1178745-a3434377d',
    #                         'BRANCH': 'master',
    #                         'SANDBOX_LOC': '/home/evo-builder/,
    #                         'JOB_SUBMIT_TIME': datetime(2016,5,12,14,24,4,12635) }
    #                   In the above example, it is expect the datatypes of
    #                   the fields are similar to the following:
    #                   CHANGESET       VARCHAR(50)
    #                   BRANCH          VARCHAR(100)
    #                   SANDBOX_LOC     VARCHAR(100)
    #                   JOB_SUBMIT_TIME DATETIME
    #
    #     keyFields     An additional dictionary of fields and field values with
    #                   the same format as the "fields" parameter.  For UPDATE
    #                   statements, when the update should be done on specific
    #                   records determined by the value of the primary key or
    #                   other fields, these determining values can be passed
    #                   thru this parameter to keep them separate from their new
    #                   values in the "fields" parameter.  An example of using
    #                   this field is shown below:
    #
    #                   # Correcting the job submit time to adjust for daylight
    #                   # savings time:
    #                   _preprocess_field_data(
    #                     # Table name:
    #                     "EVO_CHECK_PIPELINE_INFO",
    #                     # New values:
    #                     { 'JOB_SUBMIT_TIME': datetime(2016,5,12,14,24,4,12635),
    #                       'NOTES': 'Corrected for DST.' },
    #                     # Old value:
    #                     { 'JOB_SUBMIT_TIME': datetime(2016,5,3,13,24,4,12635)}
    #                   )
    #
    #                   As shown above, this allows you to convert two values
    #                   for the same field.
    #
    # Return Value:
    #     A tuple containing two dictionaries: the converted results of both
    #     the "fields" and "keyFields" parameters:
    #
    #     Example:
    #     (convertedFields, convertedKeys) = _preprocess_field_data("MY_TABLE",
    #         { 'MY_DATE_FIELD', datetime.datetime(2016,5,12,14,24,4,12635) },
    #         { 'MY_DATE_FIELD', datetime.datetime(2016,5,12,13,24,4,12635) } )
    #
    #     If you don't need to use the "keyFields" parameter, you can pass in
    #     an empty dictionary for it and discard its returned value like this:
    #
    #     convertedFields = _preprocess_field_data("MY_TABLE",
    #         { 'MY_DATE_FIELD', datetime.datetime(2016,5,12,14,24,4,12635) },
    #         {} )[0]
    #
    def _preprocess_field_data(self, tableName, fields, keyFields):
        fieldResults = {}
        keyFieldResults = {}

        # Get the properties of the table -- the datatypes and the field
        # lengths -- so we can preprocess any fields that require it:
        tableProperties = self._get_table_column_properties(tableName)

        # Convert the two groups of fields:
        fieldResults = self._preprocess_field_group(tableProperties, fields)
        keyFieldResults = self._preprocess_field_group(tableProperties, keyFields)

        # Return the processed versions of the fields:
        return (fieldResults, keyFieldResults)

    # Given a result set that has just been returned from the database, convert
    # it to an array of dictionaries where each array element is a record and
    # each dictionary element is a column (field).  This will prevent any
    # calling functions or object from needing any dependencies on SQLAlchemy or
    # SQLSoup.  Also, if there are any columns whose datatype causes their data
    # to be returned in a format that is difficult to work with, convert the
    # data to a more convenient format.
    #
    # Parameters:
    #     resultSet     The query results, as an SQLAlchemy.ResultProxy object
    #                   as returned by a call to self.engine.execute().
    #
    # Return Value:
    #     The query results, as an array of dictionaries.  If the query returns
    #     no records, a zero-length array is returned.
    #
    def _postprocess_query_data(self, resultSet):
        """
        Convert the return resulted set to a format that is more convenient to
        work with.
        """

        # Get the properties of each column:
        columnProperties = self._get_query_column_properties(resultSet)

        bitFieldNames = []

        # The only column datatype we have to convert is "BIT" -- a boolean
        # type.  The values that arrive from MySQL are in the format "\x01"
        # for True (a byte with the value 1, not an ASCII '1'), and '\0' for
        # False (a "nul" byte).  Convert them to the Python values True and
        # False.

        # For each column, if it is of type BIT, put it in a list so it can be
        # easily intercepted and converted:
        for singleColumnName in columnProperties:

            if columnProperties[singleColumnName][
                mySQLColumnPropertyIndexes.COLUMN_DATATYPE] \
                == mySqlDataTypes.BIT:

                bitFieldNames.append(singleColumnName)

        results = []

        # For each record:
        for singleRecord in resultSet:

            # Each record will be stored in a dictionary, where the key names
            # are the column names and the values are their values.  Start with
            # an empty dictionary, and fill in the columns from the record one
            # by one:
            nextRecord = {}

            # For each column:
            for singleColumn in singleRecord.items():

                # If this is a BIT field:
                columnName = singleColumn[mySQLResultSetIndexes.COLUMN_NAME]
                if columnName in bitFieldNames:

                    # Convert its value:
                    nextRecord[columnName] = self._mysql_python_bit(
                        getattr(singleRecord, columnName))

                # Otherwise, just pass its value thru unchanged:
                else:
                    nextRecord[columnName] = getattr(singleRecord, columnName)

            # Add this record to the array of results:
            results.append(nextRecord)

        return results

# ------------------------------------------------------------------------------

    def dashboard_opendb  (self) :
        logging.debug("dashboardDB.dashboard_opendb()")
        self.engine = get_dashboard_engine_connection(self.dbType, self.dbUser,
          self.dbPassword, self.dbHost, self.dbPort, self.dbName,
          self.dbSocketFile)

# ------------------------------------------------------------------------------

    # Closes the connection to the database, relinquishing it to any other
    # processes that may need it.  Calling any dashboardDB methods that perform
    # database operations will automatically re-open it.
    #
    # Parameters:
    #     (None)
    #
    # Return Value:
    #     (None)
    #
    def close(self):
        logging.debug("dashboardDB.close()")
        global dashboard_engine_connection

        # Is the database connection open?
        if self.engine != None:

            # If it is not already closed:
            if self.engine.connection().closed == False:
                # Close the database connection:
                self.engine.connection().close()
                self.engine.session.close_all()
                self.engine.engine.dispose()

                # Update the global "dashboard_engine_connection" object so any other
                # objects instantiating a "dashboardDB" object will know the database
                # is closed, too:
                dashboard_engine_connection = self.engine

# ------------------------------------------------------------------------------

    def check_db_connect (self):
        global dashboard_engine_connection

        if self.engine == None:
            self.engine = dashboard_engine_connection

        if self.engine == None or self.engine.connection().closed == True:
            logging.debug("--> Reopening database connection.")
            self.dashboard_opendb()

