import re
import pandas as pd 
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker


class LoadCsv():
    def __init__(self):
        self.data = None
        self.path = None
        self.engine = None
        self.conn = None
        self.session = None
        self.contact_table = None
        self.address_table = None
        self.phone_table = None

    def create_conn(self):
        """ Create engine """
        url = 'mysql+pymysql://root:password@database:3306/testdb'
        try:
            self.engine = create_engine(url)
            self.conn = self.engine.connect()
           
            session_factory = sessionmaker(bind=self.engine)
            self.session = session_factory()

            print("Connection Created")

            if not self.load_db_tables():
                print("Failed to load tables")
                return False
            return True
        except Exception as e:
            print("Error connecting mysql: %s" % e)
            return False
    
    def load_db_tables(self):
        try:
            Base = automap_base()
            Base.prepare(self.engine, reflect=True)
            self.address_table = Base.classes.address
            self.contact_table = Base.classes.contact
            self.phone_table = Base.classes.phone
            return True
        except Exception as e:
            print("Failed to initalize tables: %s" % e)
            return False

    def load_csv(self):
        try:
            self.data = pd.read_csv(
                            self.path,
                            sep=",",
                            dtype=object)
            return True
        except Exception as e:
            print("Exception %s" % e)
            return False

    def mandatory_sanitization(self, val):
        """ mandatory sanitzation for all columns data """
        # strip whitespace and remove delimiter
        return val.str.strip().str.replace(";", "")
        return val

    def sanitize_business_name(self, val):
        p1, p2 = str(val).split(" ", 1)
        regex = re.compile(r"^([a-zA-Z0-9]\.)")
        if re.match(regex, p1):
            return "%s %s" % (p1.upper(), p2)
        else:
            return val

    def sanitize_date(self,val):
        from dateutil.parser import parse
        try:
            new_date = parse(val)
            return str(new_date)
        except Exception as e:
            print("Error Sanitizing date: %s" % e)
            return val
            

    def sanitize_mobile_numbers(self, val):
        """ Add "64" prefix to front of mobile number """
        if str(val).startswith("64"):
            return val
        else:
            return "64%s" % val
    
    def sanitize_landline_numbers(self, val):
        """ Add "09" prefix to front of landline number """
        if str(val).startswith("(09)") or str(val).startswith("09"):
            return val
        elif str(val).startswith("64"):
            return val
        else:
            return "09-%s" % val
    
    def sanitize_notes(self, val):
        """ Sanitize notes column """
        # sanitize emoji
        val = val.encode('ascii', 'ignore').decode('ascii')

        return val
    
    def main_sanitize_data(self):
        """ Main Sanitize function to sanitize data """
        # Sanitize column names
        self.data.columns = self.data.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')

        # Mandatory Sanitization
        self.data = self.data.apply(self.mandatory_sanitization)

        # Specific Column Sanitization
        self.data['business'] = self.data['business'].loc[self.data['business'].notnull()].apply(self.sanitize_business_name)
        self.data['title'] = self.data['title'].str.capitalize().str.replace(".", "")
        self.data['first_name'] = self.data['first_name'].str.capitalize()
        self.data['last_name'] = self.data['last_name'].str.capitalize()
        self.data['date_of_birth'] = self.data['date_of_birth'].loc[self.data['date_of_birth'].notnull()].apply(self.sanitize_date)
        self.data['home_number'] = self.data['home_number'].loc[self.data['home_number'].notnull()].apply(self.sanitize_landline_numbers)
        self.data['fax_number'] = self.data['fax_number'].loc[self.data['fax_number'].notnull()].apply(self.sanitize_landline_numbers)
        self.data['mobile_number'] = self.data['mobile_number'].loc[self.data['mobile_number'].notnull()].apply(self.sanitize_mobile_numbers)
        self.data['notes'] = self.data['notes'].loc[self.data['notes'].notnull()].apply(self.sanitize_notes)

        # Convert nan to None
        self.data = self.data.where(pd.notnull(self.data), None)
        
        print("Data Sanitization Successful")
        return True
    
    def import_to_db(self):
    
        for row in self.data.itertuples():
            if row.first_name is not None:
                if row.last_name is not None:
                    name = "%s %s" % (row.first_name,row.last_name)
                else:
                    name = "%s" % row.first_name
            elif row.last_name is not None:
                name = "%s" % row.last_name 
            else:
                name = None

            contact_info = {
                        "title": row.title,
                        "first_name": row.first_name,
                        "last_name": row.last_name,
                        "company_name": row.business,
                        "date_of_birth": row.date_of_birth,
                        "notes":row.notes
                    }
            
            contact = self.contact_table(**contact_info)
            self.session.add(contact)
            self.session.commit()
            self.session.flush()
            self.session.refresh(contact)
            id = contact.id
            
            address_info  = {
                        "contact_id": int(id),
                        "street1": row.address_line_1,
                        "street2": row.address_line_2,
                        "suburb": row.suburb,
                        "city": row.city,
                        "post_code":row.post_code
                        }
            address = self.address_table(**address_info)
            self.session.add(address)
            self.session.commit()
            
            if row.home_number is not None:
                home_number_info = {
                    "contact_id": id,
                    "name": name,
                    "content": row.home_number,
                    "type": "Home"
                }
                home_number = self.phone_table(**home_number_info)
                self.session.add(home_number)
                self.session.commit()

            if row.work_number is not None:
                work_number_info = {
                    "contact_id": id,
                    "name": name,
                    "content": row.work_number,
                    "type": "Work"
                }
                work_number = self.phone_table(**work_number_info)
                self.session.add(work_number)
                self.session.commit()

            if row.mobile_number is not None:
                mobile_number_info = {
                    "contact_id": id,
                    "name": name,
                    "content": row.mobile_number,
                    "type": "Mobile"
                }
                mobile_number = self.phone_table(**mobile_number_info)
                self.session.add(mobile_number)
                self.session.commit()

            if row.other_number is not None:
                other_number_info = {
                    "contact_id": id,
                    "name": name,
                    "content": row.other_number,
                    "type": "Other"
                }
                other_number = self.phone_table(**other_number_info)
                self.session.add(other_number)
                self.session.commit()

        return True

    def run(self, path):
        self.path = path
        if not self.create_conn():
            return False

        if not self.load_csv():
            return False
        
        if not self.main_sanitize_data():
            return False
        
        if not self.import_to_db():
            return False
        
        return True

if __name__ == "__main__":
    print("Init loading csv")
    processor = LoadCsv()
    if processor.run("/app/dev/contact_list.csv"):
        print("Data ETL Successful")
    else:
        print("Data ETL Failed")

