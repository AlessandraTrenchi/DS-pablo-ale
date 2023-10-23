import os

class RelationalDataProcessor:
    def __init__(self): #class constructor, initializes the class instance
        self.db_path = None

    def uploadData(self, path):
        """
        Upload data to the relational database.

        Args:
            path (str): The path to the data file to be uploaded.

        Returns:
            bool: True if the data was successfully uploaded, False otherwise.
        """
        if not self.db_path:
            print("Database path is not set. Please set the database path first.")
            return False

        if not os.path.exists(path):
            print(f"File not found: {path}")
            return False

        # Here, you can add the logic to upload the data to the database.
        # You'll need to implement the database-specific code here.
        # Return True if the data was successfully uploaded, or False otherwise.

        # Example code (SQLite):
        try:
            # Your data upload logic here
            print(f"Data uploaded from {path} to {self.db_path}")
            return True
        except Exception as e:
            print(f"Error uploading data: {str(e)}")
            return False

class RelationalProcessor(RelationalDataProcessor):
    def getDbPath(self):
        """
        Get the path to the relational database.

        Returns:
            str: The path to the relational database.
        """
        return self.db_path

    def setDbPath(self, db_path):
        """
        Set the path to the relational database.

        Args:
            db_path (str): The path to the relational database.

        Returns:
            bool: True if the database path was successfully set, False otherwise.
        """
        if os.path.exists(db_path):
            self.db_path = db_path
            return True
        else:
            print(f"Database path '{db_path}' does not exist.")
            return False

# Example usage:
if __name__ == "__main__":
    rp = RelationalProcessor()
    
    # Setting the database path
    success = rp.setDbPath("./pabloale.db")
    if success:
        print("Database path set successfully.")
    
    # Uploading data
    data_path = "/data/data_to_upload.txt"  # Path to the data file
    upload_success = rp.uploadData(data_path)
    if upload_success:
        print("Data upload successful")
    else:
        print("Data upload failed")

    # Retrieving the database path
    db_path = rp.getDbPath()
    print("Database path:", db_path)
