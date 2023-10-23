class RelationalProcessor:
    def __init__(self):
        self.db_path = None

    def setDbPath(self, db_path):
        """
        Set the path to the relational database.

        Args:
            db_path (str): The path to the relational database.
        """
        self.db_path = db_path

    def getDbPath(self):
        """
        Get the path to the relational database.

        Returns:
            str: The path to the relational database.
        """
        return self.db_path

# Example usage:
if __name__ == "__main__":
    rp = RelationalProcessor()
    rp.setDbPath("/data/database.sql") #is this the correct database name? or does it need to have .db as extension
    db_path = rp.getDbPath()
    print("Database path:", db_path)
