from sqlalchemy import create_engine

def create_conn():
    """ Create engine """
    url = 'mysql+pymysql://root:password@database:3306/testdb'
    engine = create_engine(url)
    conn = engine.connect()
    print("Connection Created")
    return conn

def create_table(connection):
    result= []
    """ Create Table on Init """

    queries = [
        ("contact","""
            CREATE TABLE contact (
                id INT(11) NOT NULL AUTO_INCREMENT, 
                title ENUM('Mr', 'Mrs', 'Miss', 'Ms', 'Dr'), 
                first_name VARCHAR(64),
                last_name VARCHAR(64),
                company_name VARCHAR(64), 
                date_of_birth DATETIME,
                notes VARCHAR(255),
                PRIMARY KEY(id)
            );
        """),
        ("address","""
           CREATE TABLE address (
                id INT(11) NOT NULL AUTO_INCREMENT, 
                contact_id INT(11) NOT NULL,
                street1 VARCHAR(100),
                street2 VARCHAR(100),
                suburb VARCHAR(64),
                city VARCHAR(64),
                post_code VARCHAR(16),
                PRIMARY KEY(id)
            );
        """),
         ("phone","""
            CREATE TABLE phone (
                id INT(11) NOT NULL AUTO_INCREMENT, contact_id INT(11) NOT NULL,
                name VARCHAR(64),
                content VARCHAR(64),
                type ENUM('Home', 'Work', 'Mobile', 'Other'), PRIMARY KEY(id)
            );
        """)]
    
    for q in queries:
        table = q[0]
        query = q[1]

        try:
            connection.execute(query)
        except Exception as e:
            if "already exist" in str(e):
                print("Table %s already created" % table)
                result.append(True)
            else:
                print("Exception: %s" % e)
                result.append(False)
        else:
            print("Table %s created" % table)
            result.append(True)
    
    if all(result):
        return True
    else:
        return False

if __name__ == "__main__":
    conn = create_conn()
    create_table(conn)
   
