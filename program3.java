age cs3743;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

public class P3Program 
{
    private Connection connect = null;
    
    private Statement statement = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;
    public static final int ER_DUP_ENTRY = 1062;
    public static final int ER_DUP_ENTRY_WITH_KEY_NAME = 1586;
    public static final String[] strPropertyIdM =
    {   "MTNDDD"
       ,"NYCCC"
       ,"HOMEJJJ"
       ,"END"
    };
    
    public P3Program (String user, String password) throws Exception
    {
        try
        {
            // This will load the MySQL driver, each DBMS has its own driver
            Class.forName("com.mysql.jdbc.Driver");
            this.connect = DriverManager.getConnection
                    ("jdbc:mysql:**********************"
                    , user
                    , password);
        }
        catch (Exception e) 
        {
            throw e;
        } 
        
    }
       
    public void runProgram() throws Exception 
    {
        try 
        {
	   // your code
	    
	    //part A 

	    statement = connect.createStatement();
	    // create 

	    resultSet = statement.executeQuery("select * from Customer c");    
	    //grab all customers

	    // part B 

	    printCustomers("Beginning Customers", resultSet );
	    // gets the customers

	    // part C 

	    resultSet = statement.executeQuery("select m.* from Property m");
	    // grabs all the properties

	    // part D 

	    MySqlUtility.printUtility("Beginning Properties", resultSet);
	    // get the properties

	    //part E
	    // inserts my info into the table

	    try
	    {
		statement.executeUpdate("insert into gto457db.Customer " 
		    + "(`custNr`, `name`, `baseLoc`, `birthDt`, `gender`)"
		    + "values(\"1999\", \"Dakota\", \"NY\", \"1996-08-17\", \"M\")");	    	    
	    
	    }
	    catch (SQLException e) // throws the sql except
	    {
		switch(e.getErrorCode())
		{
		    case ER_DUP_ENTRY:
		    case ER_DUP_ENTRY_WITH_KEY_NAME:

			System.out.printf("Duplicate key error: %s\n", e.getMessage());
			break;
			//say its a dup
		    default:
			throw e; 
		}
	    }
	
	//part F
	//selects customers after added

	resultSet = statement.executeQuery( "select * from Customer c" );

	printCustomers("Customers after I was added", resultSet); 

	//part G
	// get more stuff

	resultSet = statement.executeQuery("select TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, CARDINALITY"
	+ " from INFORMATION_SCHEMA.STATISTICS"
	+ " where TABLE_SCHEMA = \"gto457db\""
	+ " and TABLE_NAME = \"Rental\""
	+ " order by INDEX_NAME, SEQ_IN_INDEX"); 

	MySqlUtility.printUtility("My Rental Indexes", resultSet);
    
	// part H: unsure about this entering values. 
	//insert into rental. for values

	preparedStatement = connect.prepareStatement( "Insert into  gto457db.Rental" 
	+ "('custNr', 'propId', 'startDt', 'totalCost')"
	+ "values (:Rental.custNr, :Rental.propId, :Rental.startDt, :Rental.totalCost)");

	// part I: this and H go together 
	// iterate thru the length 
	// multiple the total. Give it to em cheap

	for( int i = 0; i < strPropertyIdM.length; i++)
	{
	    double x  = 190.00;
	    double totalCost = x + 10.00;
	    if( strPropertyIdM[i] == "END" )
	    {
		try
		{		    
		    //idk bout this substitution
		    preparedStatement = connect.prepareStatement
		    ("insert into gto457db.Rental values"
		    + "(?, ?, ?, ? )");

		    String param1 = "1999";
		    String param2 = strPropertyIdM[i];
		    java.sql.Date param3 = java.sql.Date.valueOf("2019-12-14");
		    double param4 = totalCost;

		    // change values

		    preparedStatement.setString(1, param1);
		    preparedStatement.setString(2, param2);
		    preparedStatement.setDate(3, param3);
		    preparedStatement.setDouble(4, param4);

		    try
		    {
			preparedStatement.executeUpdate(); 
		    }
		    catch(SQLException e)
		    {
			switch (e.getErrorCode())
			{
			    case ER_DUP_ENTRY:
			    case ER_DUP_ENTRY_WITH_KEY_NAME:
				System.out.printf("Duplicate key ERROR: %s\n", e.getMessage());
				break;
				// throw dup if found
			    default:
				throw e; 
			}
		    }
		}
		catch (Exception e)
		{
		    throw e; 
		}
	    }
	}


	// part J: same as H but my rentals

	preparedStatement = connect.prepareStatement("select * from gto457db.Rental where Rental.custNr = ?");

	preparedStatement.setString(1, "1999");
    
	resultSet = preparedStatement.executeQuery();

	MySqlUtility.printUtility("My rentals", resultSet);
	
	// part K gets the 1999 people who rent but without 1999
	
	preparedStatement = connect.prepareStatement("select r.propId, r.custNr, c.name, r.totalCost "
	+ "from Customer c, Rental r "
	+ "where c.custNr <> 1999 and exists ( "
	+ "select * from Rental r, Rental r99 "
	+ "where r99.custNr = 1999 " 
	+ "and r99.propId = r.propId "
	+ "and c.custNr = r.custNr ) "
	+ "and c.custNr = r.custNr ");
    
	resultSet = preparedStatement.executeQuery();
    
	MySqlUtility.printUtility("Other customers renting my properties", resultSet);

	// part L , updates the database
    	
	statement.executeUpdate("update Rental set totalCost = (totalCost * .90) where custNr = 1999 "); 

	// part M, select my rentals

	preparedStatement = connect.prepareStatement("select * from gto457db.Rental where Rental.custNr = ?");
	preparedStatement.setString(1, "1999");
	resultSet = preparedStatement.executeQuery();
	MySqlUtility.printUtility("My rentals", resultSet);



	// part N, select the ones with a count over 2

	resultSet = statement.executeQuery("SELECT propId, COUNT(*) FROM Property GROUP by propId  HAVING COUNT(*) > 1");

	MySqlUtility.printUtility("Properties having more than 2 rentals", resultSet);

	// part O , performs a update, delete

	statement.executeUpdate("delete from Rental where custNr = \"1999\"");

	// part P, does the same as H- none should print

	preparedStatement = connect.prepareStatement("select r.propId, r.custNr, c.name, r.totalCost "
	+ "from Customer c, Rental r "
	+ "where c.custNr <> 1999 and exists ( "
	+ "select * from Rental r, Rental r99 "
	+ "where r99.custNr = 1999 "
	+ "and r99.propId = r.propId "
	+ "and c.custNr = r.custNr ) "
	+ "and c.custNr = r.custNr ");
	 
	resultSet = preparedStatement.executeQuery();
	 
	MySqlUtility.printUtility("Other customers renting my properties", resultSet);

//end of my code
	
	}
	catch (Exception e) 
	{
	    throw e;
	} 
	finally 
	{
	    close();
	}

    }                                                                                                                        
    
    // This Prnts the names and such for the Customer Table

    private void printCustomers(String title, ResultSet resultSet) throws SQLException 
    {
       // Your output for this must match the format of my sample output exactly. 
       // custNr, name, baseLoc, birthDt, gender
        System.out.printf("%s\n", title);
        
	// your code

	System.out.printf("Number     name    baseLoc      BirthDt    gender \n");

	while (resultSet.next())
	{
	    int custNr = resultSet.getInt("custNr"); 
	    String name = resultSet.getString("name");
	    String baseLoc = resultSet.getString("baseLoc");			// can be null
	    String birthDt = resultSet.getString("birthDt");			// can be null
	    String gender = resultSet.getString("gender"); 

	    if(baseLoc == null)
	    {
		baseLoc = "---";
	    }
	    if(birthDt == null)
	    {
		birthDt = "---";
	    }
	    System.out.printf("%4d %4s %4s %4s %4s \n", custNr, name, baseLoc, birthDt, gender );		
	}   // prints the ouput
    System.out.printf("\n");

	// end of my code. 

    }
    

    // Close the resultSet, statement, preparedStatement, and connect
    private void close() 
    {
        try 
        {
            if (resultSet != null) 
                resultSet.close();

            if (statement != null) 
                statement.close();
            
            if (preparedStatement != null) 
                preparedStatement.close();

            if (connect != null) 
                connect.close();
        } 
        catch (Exception e) 
        {

        }
    }

}
